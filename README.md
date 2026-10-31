# modal-turso-windmill-b2b-pipeline

Reference multi-tenant B2B lead enrichment and full-text edge SQLite pipeline combining **Modal** parallel Python workers, **Turso / libSQL Hrana HTTP Pipeline v2 (`/v2/pipeline`)** with **FTS5** search, and **Windmill** typed Python scripts + OpenFlow (`flow.yaml`).

## Three-Layer Production Architecture

1. **Parallel Compute Layer (`src/modal_turso_windmill/modal_parallel_harvester.py`):**
   - Built on `modal.App("b2b-turso-harvester")` with `.map()` chunk fan-out across raw B2B scraper datasets (`thanh81646/apify-b2b-lead-and-contact-enricher`).
   - Includes constant-time `HMAC-SHA256` webhook verification (`verify_modal_webhook_hmac`), deterministic `TokenBucketRateLimiter`, noise-domain filtering (`sentry.io`, asset filenames), and cross-chunk ICP score deduplication.
   - Ships with an offline-compatible decorator shim when `modal` is not installed locally so CI and unit tests run in `< 0.2s` with zero cloud credentials.

2. **Many-Database Edge Storage Layer (`src/modal_turso_windmill/turso_edge_store.py`):**
   - Native **Turso Hrana HTTP v2 (`/v2/pipeline`)** wire client formatting batch `execute` + `close` pipelines and strict Hrana typed arguments (`text`, 64-bit stringified `integer`, `float`, `null`).
   - Supports Turso's **database-per-tenant** pattern (`resolve_tenant_turso_url(org_slug)`) alongside idempotent `ON CONFLICT(domain) DO UPDATE` upserts and **SQLite FTS5 (`bm25`)** full-text search triggers (`b2b_leads_fts`).
   - Includes a symmetrical `sqlite://:memory:` execution engine for instant local sandboxing and deterministic unit testing.

3. **Workflow Orchestration Layer (`windmill_flows/`):**
   - `windmill_flows/sync_b2b_leads_to_turso.py`: Windmill-compliant `def main(...) -> dict:` entrypoint with `# extra_requirements:` dependency headers and `dry_run` support.
   - `windmill_flows/sync_b2b_leads_to_turso.flow/flow.yaml`: OpenFlow specification with retry policies (`constant` + `exponential`), JavaScript `input_transforms`, JSON Schema `2020-12` inputs, and a structured `failure_module`.

## Local Verification

```bash
pip install -e ".[dev]"
pytest -v
```

## Author

- **Trung Thanh Tran** (`ttrungthanh90@gmail.com`)
- Portfolio: https://trungthanhtran.vercel.app
- Commercial Apify Actors: https://apify.com/thanh81646
- GitHub: https://github.com/Tthanh2k6
