from __future__ import annotations

import re
import sqlite3
from typing import Any

import httpx

SCHEMA_STATEMENTS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS b2b_leads (
        idempotency_key TEXT NOT NULL,
        domain TEXT PRIMARY KEY,
        company_name TEXT NOT NULL,
        primary_email TEXT,
        country_code TEXT,
        icp_score INTEGER NOT NULL DEFAULT 0,
        tech_stack TEXT NOT NULL DEFAULT '',
        summary TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS b2b_leads_fts USING fts5(
        domain,
        company_name,
        tech_stack,
        summary,
        content='b2b_leads',
        content_rowid='rowid'
    )
    """,
    """
    CREATE TRIGGER IF NOT EXISTS b2b_leads_ai AFTER INSERT ON b2b_leads BEGIN
        INSERT INTO b2b_leads_fts(rowid, domain, company_name, tech_stack, summary)
        VALUES (new.rowid, new.domain, new.company_name, new.tech_stack, new.summary);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS b2b_leads_ad AFTER DELETE ON b2b_leads BEGIN
        INSERT INTO b2b_leads_fts(b2b_leads_fts, rowid, domain, company_name, tech_stack, summary)
        VALUES ('delete', old.rowid, old.domain, old.company_name, old.tech_stack, old.summary);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS b2b_leads_au AFTER UPDATE ON b2b_leads BEGIN
        INSERT INTO b2b_leads_fts(b2b_leads_fts, rowid, domain, company_name, tech_stack, summary)
        VALUES ('delete', old.rowid, old.domain, old.company_name, old.tech_stack, old.summary);
        INSERT INTO b2b_leads_fts(rowid, domain, company_name, tech_stack, summary)
        VALUES (new.rowid, new.domain, new.company_name, new.tech_stack, new.summary);
    END
    """,
]

UPSERT_SQL = """
INSERT INTO b2b_leads (
    idempotency_key, domain, company_name, primary_email, country_code, icp_score, tech_stack, summary
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(domain) DO UPDATE SET
    idempotency_key = excluded.idempotency_key,
    company_name = excluded.company_name,
    primary_email = COALESCE(excluded.primary_email, b2b_leads.primary_email),
    country_code = COALESCE(excluded.country_code, b2b_leads.country_code),
    icp_score = excluded.icp_score,
    tech_stack = excluded.tech_stack,
    summary = excluded.summary,
    updated_at = datetime('now')
"""

FTS_SEARCH_SQL = """
SELECT l.domain, l.company_name, l.primary_email, l.icp_score, l.tech_stack, l.summary
FROM b2b_leads_fts f
JOIN b2b_leads l ON l.rowid = f.rowid
WHERE b2b_leads_fts MATCH ?
ORDER BY bm25(b2b_leads_fts), l.icp_score DESC
LIMIT ?
"""


def resolve_tenant_turso_url(org_slug: str, turso_org: str = "b2b-prod", aws_region: str = "aws-us-west-2") -> str:
    """Build a per-tenant Turso database URL for Turso's many-database architecture."""
    slug = re.sub(r"[^a-z0-9-]+", "-", (org_slug or "").strip().lower()).strip("-")
    if not slug:
        raise ValueError("org_slug must contain at least one alphanumeric character")
    return f"https://leads-{slug}-{turso_org}.{aws_region}.turso.io"


def encode_hrana_arg(value: Any) -> dict[str, Any]:
    """Encode a Python scalar into the exact Turso / libSQL Hrana v2 wire format."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "integer", "value": "1" if value else "0"}
    if isinstance(value, int):
        # Hrana v2 encodes 64-bit integers as decimal strings to preserve JS precision
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": value}
    return {"type": "text", "value": str(value)}


def _decode_hrana_cell(cell: dict[str, Any]) -> Any:
    ctype = cell.get("type")
    if ctype == "null":
        return None
    if ctype == "integer":
        return int(cell["value"])
    if ctype == "float":
        return float(cell["value"])
    return cell.get("value")


class TursoEdgeStore:
    """Dual-mode client supporting Turso Hrana HTTP Pipeline v2 (`/v2/pipeline`) and local SQLite."""

    def __init__(
        self,
        url: str = "sqlite://:memory:",
        auth_token: str = "",
        http_client: httpx.Client | None = None,
    ) -> None:
        self.raw_url = url
        self.auth_token = auth_token
        self._http_client = http_client
        self._sqlite_conn: sqlite3.Connection | None = None

        if url.startswith("sqlite://"):
            db_target = url[len("sqlite://") :] or ":memory:"
            self._mode = "sqlite"
            self._pipeline_endpoint = ""
            self._sqlite_conn = sqlite3.connect(db_target)
            self._sqlite_conn.row_factory = sqlite3.Row
        else:
            self._mode = "hrana_http"
            normalized = url.replace("libsql://", "https://", 1).rstrip("/")
            if not normalized.startswith(("https://", "http://")):
                normalized = "https://" + normalized
            self._pipeline_endpoint = f"{normalized}/v2/pipeline"

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def pipeline_endpoint(self) -> str:
        return self._pipeline_endpoint

    @staticmethod
    def build_hrana_pipeline_payload(statements: list[tuple[str, list[Any]]]) -> dict[str, Any]:
        """Construct a batch Hrana v2 `/v2/pipeline` request body terminated with `close`."""
        requests: list[dict[str, Any]] = []
        for sql, params in statements:
            stmt_obj: dict[str, Any] = {"sql": sql.strip()}
            if params:
                stmt_obj["args"] = [encode_hrana_arg(p) for p in params]
            requests.append({"type": "execute", "stmt": stmt_obj})
        requests.append({"type": "close"})
        return {"requests": requests}

    def execute_pipeline(self, statements: list[tuple[str, list[Any]]]) -> list[list[dict[str, Any]]]:
        """Execute a batch of parameterized SQL statements against either SQLite or Turso Hrana v2."""
        if self._mode == "sqlite":
            assert self._sqlite_conn is not None
            outputs: list[list[dict[str, Any]]] = []
            with self._sqlite_conn:
                for sql, params in statements:
                    cur = self._sqlite_conn.execute(sql, params)
                    if cur.description:
                        cols = [d[0] for d in cur.description]
                        outputs.append([dict(zip(cols, row)) for row in cur.fetchall()])
                    else:
                        outputs.append([])
            return outputs

        payload = self.build_hrana_pipeline_payload(statements)
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        own_client = self._http_client is None
        client = self._http_client or httpx.Client(timeout=15.0)
        try:
            resp = client.post(self._pipeline_endpoint, json=payload, headers=headers)
            resp.raise_for_status()
            body = resp.json()
        finally:
            if own_client:
                client.close()

        parsed_results: list[list[dict[str, Any]]] = []
        for item in body.get("results", []):
            if item.get("type") == "error":
                err_msg = (item.get("error") or {}).get("message", "Unknown Turso Hrana error")
                raise RuntimeError(f"Turso Hrana pipeline error: {err_msg}")
            if item.get("type") != "ok":
                continue
            res_block = (item.get("response") or {}).get("result")
            if not res_block:
                continue
            cols = [c["name"] for c in res_block.get("cols", [])]
            rows_out: list[dict[str, Any]] = []
            for raw_row in res_block.get("rows", []):
                decoded_vals = [_decode_hrana_cell(cell) for cell in raw_row]
                rows_out.append(dict(zip(cols, decoded_vals)))
            parsed_results.append(rows_out)
        return parsed_results

    def init_schema(self) -> None:
        self.execute_pipeline([(sql, []) for sql in SCHEMA_STATEMENTS])

    def upsert_leads(self, leads: list[dict[str, Any]]) -> int:
        if not leads:
            return 0
        stmts: list[tuple[str, list[Any]]] = []
        for lead in leads:
            tech_stack = lead.get("tech_stack") or []
            if isinstance(tech_stack, list):
                tech_str = ", ".join(str(x) for x in tech_stack)
            else:
                tech_str = str(tech_stack)
            stmts.append(
                (
                    UPSERT_SQL,
                    [
                        str(lead["idempotency_key"]),
                        str(lead["domain"]),
                        str(lead["company_name"]),
                        lead.get("primary_email"),
                        lead.get("country_code"),
                        int(lead.get("icp_score", 0)),
                        tech_str,
                        str(lead.get("summary") or ""),
                    ],
                )
            )
        self.execute_pipeline(stmts)
        return len(stmts)

    def search_leads_fts5(self, fts_query: str, limit: int = 10) -> list[dict[str, Any]]:
        cleaned = " ".join((fts_query or "").split())
        if not cleaned:
            return []
        batches = self.execute_pipeline([(FTS_SEARCH_SQL, [cleaned, int(limit)])])
        return batches[0] if batches else []

    def close(self) -> None:
        if self._sqlite_conn is not None:
            self._sqlite_conn.close()
            self._sqlite_conn = None
