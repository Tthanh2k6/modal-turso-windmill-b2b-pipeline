# extra_requirements:
# httpx>=0.26.0

from __future__ import annotations

from typing import Any

from modal_turso_windmill.modal_parallel_harvester import run_parallel_enrichment
from modal_turso_windmill.turso_edge_store import TursoEdgeStore, resolve_tenant_turso_url


def main(
    leads_payload: list[dict[str, Any]],
    turso_url: str = "sqlite://:memory:",
    turso_auth_token: str = "",
    org_slug: str = "default-workspace",
    fts_probe_query: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Windmill Python Script entrypoint: enrich B2B leads and upsert into a tenant Turso database.

    Parameters
    ----------
    leads_payload:
        Raw company/contact dicts harvested from Apify (`thanh81646/apify-b2b-lead-and-contact-enricher`) or webhooks.
    turso_url:
        Either `libsql://<db>.turso.io`, `https://<db>.turso.io`, `auto-tenant`, or `sqlite://:memory:`.
    turso_auth_token:
        Turso database JWT token (retrieved from a Windmill Variable/Resource in production).
    org_slug:
        Workspace identifier used when `turso_url == 'auto-tenant'`.
    fts_probe_query:
        Optional FTS5 full-text query executed immediately after upsert to return top matching accounts.
    dry_run:
        When True, runs parallel normalization and ICP scoring without writing to Turso.
    """
    enriched = run_parallel_enrichment(leads_payload, chunk_size=20)
    resolved_url = resolve_tenant_turso_url(org_slug) if turso_url == "auto-tenant" else turso_url

    if dry_run:
        return {
            "mode": "dry_run",
            "target_url": resolved_url,
            "chunks_dispatched": enriched["chunks_dispatched"],
            "upserted_count": 0,
            "unique_leads": enriched["unique_leads"],
            "leads": enriched["leads"],
            "fts_matches": [],
        }

    store = TursoEdgeStore(url=resolved_url, auth_token=turso_auth_token)
    try:
        store.init_schema()
        upserted = store.upsert_leads(enriched["leads"])
        fts_matches = store.search_leads_fts5(fts_probe_query, limit=10) if fts_probe_query else []
        return {
            "mode": store.mode,
            "target_url": store.pipeline_endpoint or resolved_url,
            "chunks_dispatched": enriched["chunks_dispatched"],
            "upserted_count": upserted,
            "unique_leads": enriched["unique_leads"],
            "leads": enriched["leads"],
            "fts_matches": fts_matches,
        }
    finally:
        store.close()
