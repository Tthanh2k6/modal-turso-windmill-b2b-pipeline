from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from modal_turso_windmill import (
    TokenBucketRateLimiter,
    TursoEdgeStore,
    encode_hrana_arg,
    resolve_tenant_turso_url,
    run_parallel_enrichment,
    verify_modal_webhook_hmac,
)
from windmill_flows.sync_b2b_leads_to_turso import main as windmill_main


def test_turso_many_database_tenant_resolver() -> None:
    url = resolve_tenant_turso_url("Acme AG!", turso_org="trung-b2b", aws_region="aws-eu-central-1")
    assert url == "https://leads-acme-ag-trung-b2b.aws-eu-central-1.turso.io"
    with pytest.raises(ValueError):
        resolve_tenant_turso_url("!!!")


def test_turso_hrana_v2_wire_protocol_and_bearer_auth() -> None:
    captured: dict[str, Any] = {}

    def mock_handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("Authorization")
        captured["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "type": "ok",
                        "response": {
                            "type": "execute",
                            "result": {
                                "cols": [
                                    {"name": "domain"},
                                    {"name": "icp_score"},
                                    {"name": "primary_email"},
                                ],
                                "rows": [
                                    [
                                        {"type": "text", "value": "acme.example.com"},
                                        {"type": "integer", "value": "95"},
                                        {"type": "null"},
                                    ]
                                ],
                            },
                        },
                    },
                    {"type": "ok", "response": {"type": "close"}},
                ]
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    store = TursoEdgeStore(
        url="libsql://leads-acme-b2b.turso.io",
        auth_token="eyJ_turso_jwt_test",
        http_client=client,
    )
    assert store.mode == "hrana_http"
    assert store.pipeline_endpoint == "https://leads-acme-b2b.turso.io/v2/pipeline"

    rows = store.execute_pipeline(
        [("SELECT domain, icp_score, primary_email FROM b2b_leads WHERE icp_score >= ? AND active = ?", [80, True])]
    )
    assert captured["url"] == "https://leads-acme-b2b.turso.io/v2/pipeline"
    assert captured["auth"] == "Bearer eyJ_turso_jwt_test"
    reqs = captured["body"]["requests"]
    assert len(reqs) == 2
    assert reqs[0]["type"] == "execute"
    assert reqs[0]["stmt"]["args"] == [
        {"type": "integer", "value": "80"},
        {"type": "integer", "value": "1"},
    ]
    assert reqs[1] == {"type": "close"}
    assert rows == [[{"domain": "acme.example.com", "icp_score": 95, "primary_email": None}]]

    assert encode_hrana_arg(3.14) == {"type": "float", "value": 3.14}
    assert encode_hrana_arg(None) == {"type": "null"}


def test_sqlite_fts5_upsert_idempotency_and_bm25_search() -> None:
    store = TursoEdgeStore("sqlite://:memory:")
    try:
        store.init_schema()
        initial = [
            {
                "idempotency_key": "idem_1",
                "domain": "acme.example.com",
                "company_name": "Acme AG",
                "primary_email": "jane.doe@acme.example.com",
                "country_code": "CH",
                "icp_score": 85,
                "tech_stack": ["Retool", "PostgreSQL"],
                "summary": "Swiss B2B digital efficiency consultancy building embedded analytics.",
            },
            {
                "idempotency_key": "idem_2",
                "domain": "wellbayt.com",
                "company_name": "Wellbayt",
                "primary_email": "ops@wellbayt.com",
                "country_code": "AE",
                "icp_score": 75,
                "tech_stack": ["HubSpot", "Python"],
                "summary": "Corporate wellbeing marketplace automating partner onboarding.",
            },
        ]
        assert store.upsert_leads(initial) == 2

        # Idempotent update on same domain preserves existing primary_email when new payload has None
        updated = [
            {
                "idempotency_key": "idem_1_v2",
                "domain": "acme.example.com",
                "company_name": "Acme AG Switzerland",
                "primary_email": None,
                "country_code": None,
                "icp_score": 95,
                "tech_stack": ["Retool", "PostgreSQL", "Windmill"],
                "summary": "Swiss B2B digital efficiency consultancy scaling Windmill and Retool.",
            }
        ]
        assert store.upsert_leads(updated) == 1

        hits = store.search_leads_fts5("Windmill OR embedded", limit=5)
        assert len(hits) == 1
        assert hits[0]["domain"] == "acme.example.com"
        assert hits[0]["company_name"] == "Acme AG Switzerland"
        assert hits[0]["primary_email"] == "jane.doe@acme.example.com"
        assert hits[0]["icp_score"] == 95
    finally:
        store.close()


def test_modal_parallel_map_fanout_and_cross_chunk_dedup() -> None:
    raw_items = [
        {
            "website": "https://www.acme.example.com/contact",
            "companyName": "Acme AG",
            "emails": ["noreply@sentry.io", "info@acme.example.com"],
            "techStack": ["PostgreSQL"],
            "description": "First partial crawl",
        },
        {
            "domain": "wellbayt.com",
            "companyName": "Wellbayt",
            "emails": ["ops@wellbayt.com"],
            "techStack": ["Python", "HubSpot"],
        },
        {
            "domain": "acme.example.com",
            "companyName": "Acme AG",
            "emails": ["jane.doe@acme.example.com"],
            "techStack": ["PostgreSQL", "Retool", "Windmill"],
            "description": "Richer crawl in second Modal chunk",
        },
        {
            "domain": "sentry.io",
            "companyName": "Filtered Noise Domain",
        },
    ]
    res = run_parallel_enrichment(raw_items, chunk_size=2)
    assert res["chunks_dispatched"] == 2
    assert res["unique_leads"] == 2
    top = res["leads"][0]
    assert top["domain"] == "acme.example.com"
    assert top["icp_score"] == 100
    assert top["primary_email"] == "jane.doe@acme.example.com"


def test_modal_hmac_signature_and_replay_window() -> None:
    secret = "modal_whsec_test"
    body = b'{"batch_id":"b_01"}'
    ts = "1793400000"
    sig = "sha256=" + hmac.new(secret.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()

    assert verify_modal_webhook_hmac(secret, ts, body, sig, max_skew=300, now_epoch=1793400120) is True
    assert verify_modal_webhook_hmac(secret, ts, body, sig, max_skew=300, now_epoch=1793400900) is False
    assert verify_modal_webhook_hmac(secret, ts, body, "sha256=bad", max_skew=300, now_epoch=1793400120) is False


def test_token_bucket_rate_limiter() -> None:
    bucket = TokenBucketRateLimiter(capacity=2, refill_per_second=1.0, initial_time=10.0)
    assert bucket.allow(cost=1.0, now=10.0) is True
    assert bucket.allow(cost=1.0, now=10.0) is True
    assert bucket.allow(cost=1.0, now=10.2) is False
    assert bucket.allow(cost=1.0, now=11.1) is True


def test_windmill_script_entrypoint_live_and_dry_run() -> None:
    sample_payload = [
        {
            "website": "https://acme-industrial.de",
            "companyName": "Acme Industrial GmbH",
            "emails": ["cto@acme-industrial.de"],
            "techStack": ["Python", "Turso", "Windmill"],
            "countryCode": "de",
            "description": "Automated ERP order routing with Windmill and edge SQLite.",
        }
    ]
    dry = windmill_main(
        leads_payload=sample_payload,
        turso_url="auto-tenant",
        org_slug="Acme Europe",
        dry_run=True,
    )
    assert dry["mode"] == "dry_run"
    assert dry["upserted_count"] == 0
    assert dry["target_url"] == "https://leads-acme-europe-b2b-prod.aws-us-west-2.turso.io"

    live = windmill_main(
        leads_payload=sample_payload,
        turso_url="sqlite://:memory:",
        fts_probe_query="Windmill AND ERP",
        dry_run=False,
    )
    assert live["mode"] == "sqlite"
    assert live["upserted_count"] == 1
    assert len(live["fts_matches"]) == 1
    assert live["fts_matches"][0]["domain"] == "acme-industrial.de"


def test_windmill_openflow_yaml_schema_validity() -> None:
    flow_path = Path(__file__).resolve().parents[1] / "windmill_flows" / "sync_b2b_leads_to_turso.flow" / "flow.yaml"
    flow = yaml.safe_load(flow_path.read_text(encoding="utf-8"))
    assert "summary" in flow
    modules = flow["value"]["modules"]
    assert [m["id"] for m in modules] == ["validate_and_enrich_batch", "upsert_turso_hrana_v2"]
    assert flow["value"]["failure_module"]["id"] == "on_pipeline_error"
    assert set(flow["schema"]["required"]) == {"leads_payload", "turso_url"}
