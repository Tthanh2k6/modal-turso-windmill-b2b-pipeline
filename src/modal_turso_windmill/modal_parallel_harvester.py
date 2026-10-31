from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

try:
    import modal  # type: ignore[import-not-found]

    HAS_MODAL_SDK = True
except ImportError:
    modal = None
    HAS_MODAL_SDK = False


class _ModalFunctionHandle:
    """Offline-compatible wrapper exposing Modal's `.local()` and `.map()` semantics for pytest."""

    def __init__(self, fn: Callable[..., Any]) -> None:
        self._fn = fn
        self.__name__ = getattr(fn, "__name__", "modal_fn")
        self.__doc__ = getattr(fn, "__doc__", None)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._fn(*args, **kwargs)

    def local(self, *args: Any, **kwargs: Any) -> Any:
        return self._fn(*args, **kwargs)

    def map(self, iterable: Iterable[Any]) -> list[Any]:
        return [self._fn(item) for item in iterable]


class _ModalImageShim:
    @staticmethod
    def debian_slim(python_version: str = "3.12") -> "_ModalImageShim":
        return _ModalImageShim()

    def pip_install(self, *packages: str) -> "_ModalImageShim":
        return self


class _ModalAppShim:
    def __init__(self, name: str) -> None:
        self.name = name

    def function(self, **kwargs: Any) -> Callable[[Callable[..., Any]], _ModalFunctionHandle]:
        def decorator(fn: Callable[..., Any]) -> _ModalFunctionHandle:
            return _ModalFunctionHandle(fn)

        return decorator


if HAS_MODAL_SDK:
    image = modal.Image.debian_slim(python_version="3.12").pip_install("httpx>=0.26.0")
    app = modal.App("b2b-turso-harvester")
else:
    image = _ModalImageShim.debian_slim(python_version="3.12").pip_install("httpx>=0.26.0")
    app = _ModalAppShim("b2b-turso-harvester")


NOISE_DOMAINS = {"sentry.io", "example.com", "wixpress.com", "cloudflare.com"}
HIGH_SIGNAL_STACK = {"python", "postgres", "postgresql", "retool", "windmill", "turso", "supabase", "fastapi", "hubspot"}


class TokenBucketRateLimiter:
    """Deterministic token-bucket rate limiter protecting downstream B2B enrichment APIs."""

    def __init__(self, capacity: int = 20, refill_per_second: float = 5.0, initial_time: float = 0.0) -> None:
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.refill_per_second = float(refill_per_second)
        self.last_refill = float(initial_time)

    def allow(self, cost: float = 1.0, now: float | None = None) -> bool:
        current = time.monotonic() if now is None else float(now)
        elapsed = max(0.0, current - self.last_refill)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_second)
        self.last_refill = current
        if self.tokens >= cost:
            self.tokens -= cost
            return True
        return False


def verify_modal_webhook_hmac(secret: str, timestamp: str, raw_body: bytes, signature: str, max_skew: int = 300, now_epoch: int | None = None) -> bool:
    """Verify `X-Webhook-Signature` (`sha256=<hex>`) and replay window for Modal web endpoints."""
    if not secret or not timestamp or not signature:
        return False
    try:
        ts_int = int(timestamp)
    except ValueError:
        return False
    now_val = int(time.time()) if now_epoch is None else int(now_epoch)
    if abs(now_val - ts_int) > max_skew:
        return False
    expected = "sha256=" + hmac.new(secret.encode("utf-8"), timestamp.encode("utf-8") + b"." + raw_body, hashlib.sha256).hexdigest()
    provided = signature if signature.startswith("sha256=") else f"sha256={signature}"
    return hmac.compare_digest(expected, provided)


def chunk_items(items: list[dict[str, Any]], chunk_size: int = 25) -> list[list[dict[str, Any]]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [items[idx : idx + chunk_size] for idx in range(0, len(items), chunk_size)]


def _normalize_domain(raw_url_or_domain: str) -> str:
    val = (raw_url_or_domain or "").strip().lower()
    if not val:
        return ""
    if "://" not in val:
        val = "https://" + val
    host = (urlparse(val).hostname or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    if "." not in host or host in NOISE_DOMAINS:
        return ""
    return host


@app.function(image=image, timeout=180, retries=2)
def harvest_and_enrich_chunk(raw_chunk: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sanitize, deduplicate within chunk, and score a batch of Apify B2B leads on a Modal worker."""
    enriched: list[dict[str, Any]] = []
    seen_in_chunk: set[str] = set()

    for item in raw_chunk:
        domain = _normalize_domain(str(item.get("domain") or item.get("website") or item.get("url") or ""))
        if not domain or domain in seen_in_chunk:
            continue
        seen_in_chunk.add(domain)

        emails_raw: list[str] = []
        if isinstance(item.get("emails"), list):
            emails_raw.extend(str(e) for e in item["emails"])
        if item.get("email"):
            emails_raw.append(str(item["email"]))

        valid_emails = [
            e.strip().lower()
            for e in emails_raw
            if "@" in e
            and e.strip().lower().split("@", 1)[1] not in NOISE_DOMAINS
            and not e.strip().lower().endswith((".png", ".svg", ".jpg", ".webp"))
        ]
        valid_emails.sort(key=lambda e: 0 if e.endswith(domain) else 1)

        raw_stack = item.get("techStack") or item.get("tech_stack") or []
        tech_stack = sorted({str(t).strip() for t in raw_stack if str(t).strip()})

        score = 40
        if valid_emails:
            score += 25
            if valid_emails[0].endswith(domain):
                score += 10
        stack_hits = sum(1 for t in tech_stack if t.lower() in HIGH_SIGNAL_STACK)
        score = min(100, score + stack_hits * 10)

        company_name = " ".join(str(item.get("companyName") or item.get("company_name") or domain).split())
        summary = " ".join(str(item.get("description") or item.get("summary") or "").split())[:400]
        idem = hashlib.sha256(f"modal-turso:{domain}".encode("utf-8")).hexdigest()[:24]

        enriched.append(
            {
                "idempotency_key": idem,
                "domain": domain,
                "company_name": company_name,
                "primary_email": valid_emails[0] if valid_emails else None,
                "country_code": str(item.get("countryCode") or item.get("country_code") or "").strip().upper()[:2] or None,
                "icp_score": score,
                "tech_stack": tech_stack,
                "summary": summary,
            }
        )
    return enriched


def run_parallel_enrichment(raw_leads: list[dict[str, Any]], chunk_size: int = 25) -> dict[str, Any]:
    """Fan out lead chunks via Modal `.map()`, merge cross-chunk duplicates, and sort by ICP score."""
    chunks = chunk_items(raw_leads, chunk_size=chunk_size)
    if not chunks:
        return {"chunks_dispatched": 0, "unique_leads": 0, "leads": []}

    mapped_batches = list(harvest_and_enrich_chunk.map(chunks))
    merged_by_domain: dict[str, dict[str, Any]] = {}
    for batch in mapped_batches:
        for lead in batch:
            existing = merged_by_domain.get(lead["domain"])
            if existing is None or lead["icp_score"] > existing["icp_score"]:
                merged_by_domain[lead["domain"]] = lead

    ordered = sorted(merged_by_domain.values(), key=lambda x: (-x["icp_score"], x["domain"]))
    return {
        "chunks_dispatched": len(chunks),
        "unique_leads": len(ordered),
        "leads": ordered,
    }
