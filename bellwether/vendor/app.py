"""The mock vendor API.

    uvicorn bellwether.vendor.app:app --port 8900

Four endpoints, deliberately inconsistent with each other:

- **Okta** — opaque `after` cursor, next page advertised in a `Link` header.
- **Google Workspace** — `pageToken` in, `nextPageToken` in the body.
- **MailShield** — `cursor` / `next_cursor` / `has_more`.
- **Sentry Agent** — numeric `offset` and `total`.

Plus the two behaviours that decide whether a connector is production-grade:
rate limiting (429 with `Retry-After`) and transient failure (503). Both are
tunable at runtime through `/_control/config` so tests can make them certain
rather than probabilistic.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Annotated, Any

from fastapi import FastAPI, HTTPException, Query, Response

from bellwether.events.schema import Source
from bellwether.vendor.store import VendorStore, build_store

DEFAULT_PAGE_LIMIT = 100
MAX_PAGE_LIMIT = 1000


@dataclass
class VendorConfig:
    """Runtime knobs for how badly the vendor behaves.

    Defaults are lenient enough that an ordinary poll succeeds, so a demo does
    not look broken, but `rate_limit_per_second` is low enough that an
    unthrottled connector hammering the API will trip it.
    """

    rate_limit_per_second: int = 25
    failure_rate: float = 0.0
    force_429_every: int = 0
    force_503_every: int = 0
    latency_ms: int = 0


@dataclass
class _Bucket:
    window_started: float = 0.0
    count: int = 0


@dataclass
class VendorState:
    store: VendorStore
    config: VendorConfig = field(default_factory=VendorConfig)
    buckets: dict[str, _Bucket] = field(default_factory=dict)
    request_counts: dict[str, int] = field(default_factory=dict)
    rng: random.Random = field(default_factory=lambda: random.Random(0))

    def guard(self, endpoint: str) -> None:
        """Apply rate limiting and fault injection. Raises HTTPException if unlucky."""
        config = self.config
        self.request_counts[endpoint] = self.request_counts.get(endpoint, 0) + 1
        seen = self.request_counts[endpoint]

        if config.latency_ms:
            time.sleep(config.latency_ms / 1000.0)

        if config.force_429_every and seen % config.force_429_every == 0:
            raise HTTPException(429, "rate limited", headers={"Retry-After": "1"})
        if config.force_503_every and seen % config.force_503_every == 0:
            raise HTTPException(503, "upstream unavailable")
        if config.failure_rate and self.rng.random() < config.failure_rate:
            raise HTTPException(503, "upstream unavailable")

        now = time.monotonic()
        bucket = self.buckets.setdefault(endpoint, _Bucket(now, 0))
        if now - bucket.window_started >= 1.0:
            bucket.window_started = now
            bucket.count = 0
        bucket.count += 1
        if bucket.count > config.rate_limit_per_second:
            raise HTTPException(429, "rate limited", headers={"Retry-After": "1"})


def _decode_cursor(cursor: str | None) -> int:
    """Cursors are opaque to clients but are just offsets underneath.

    Rejecting a malformed cursor with 400 rather than silently restarting from
    zero matters: a connector that corrupts its cursor should fail loudly, not
    quietly re-ingest history.
    """
    if not cursor:
        return 0
    try:
        return int(cursor.removeprefix("c"))
    except ValueError:
        raise HTTPException(400, f"malformed cursor: {cursor!r}") from None


def _encode_cursor(offset: int) -> str:
    return f"c{offset}"


def create_app(store: VendorStore | None = None, config: VendorConfig | None = None) -> FastAPI:
    app = FastAPI(title="Bellwether mock vendor", docs_url="/_docs")
    state = VendorState(store=store or build_store(), config=config or VendorConfig())
    app.state.vendor = state

    # --- control plane, for tests and demos ------------------------------
    @app.post("/_control/config")
    def set_config(payload: dict[str, Any]) -> dict[str, Any]:
        for key, value in payload.items():
            if not hasattr(state.config, key):
                raise HTTPException(400, f"unknown config key: {key}")
            setattr(state.config, key, value)
        return vars(state.config)

    @app.post("/_control/reset")
    def reset() -> dict[str, str]:
        state.buckets.clear()
        state.request_counts.clear()
        state.config = VendorConfig()
        return {"status": "reset"}

    @app.get("/_control/stats")
    def stats() -> dict[str, Any]:
        return {
            "requests": dict(state.request_counts),
            "totals": {source.value: state.store.total(source) for source in Source},
        }

    # --- Okta: opaque `after` cursor, next page in a Link header ---------
    @app.get("/api/v1/logs")
    def okta_logs(
        response: Response,
        after: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,
    ) -> list[dict[str, Any]]:
        state.guard("okta")
        offset = _decode_cursor(after)
        window, next_offset, has_more = state.store.slice(Source.OKTA, offset, limit)
        if has_more:
            nxt = _encode_cursor(next_offset)
            response.headers["Link"] = (
                f'<https://vendor.local/api/v1/logs?after={nxt}&limit={limit}>; rel="next"'
            )
        return window

    # --- Google Workspace: pageToken in, nextPageToken out ---------------
    @app.get("/admin/reports/v1/activity/users/all/applications/{application}")
    def google_activities(
        application: str,
        pageToken: Annotated[str | None, Query()] = None,  # noqa: N803 — vendor's spelling
        maxResults: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,  # noqa: N803
    ) -> dict[str, Any]:
        state.guard("google")
        offset = _decode_cursor(pageToken)
        window, next_offset, has_more = state.store.slice(
            Source.GOOGLE_WORKSPACE, offset, maxResults
        )
        if application != "all":
            window = [r for r in window if r["id"]["applicationName"] == application]
        body: dict[str, Any] = {"kind": "admin#reports#activities", "items": window}
        if has_more:
            body["nextPageToken"] = _encode_cursor(next_offset)
        return body

    # --- MailShield: cursor / next_cursor / has_more ---------------------
    @app.get("/v2/events")
    def mailshield_events(
        cursor: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,
    ) -> dict[str, Any]:
        state.guard("mailshield")
        offset = _decode_cursor(cursor)
        window, next_offset, has_more = state.store.slice(Source.EMAIL_GATEWAY, offset, limit)
        return {
            "data": window,
            "next_cursor": _encode_cursor(next_offset) if has_more else None,
            "has_more": has_more,
        }

    # --- Sentry Agent: plain numeric offset ------------------------------
    @app.get("/api/telemetry")
    def sentry_telemetry(
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = DEFAULT_PAGE_LIMIT,
    ) -> dict[str, Any]:
        state.guard("sentry")
        window, next_offset, _ = state.store.slice(Source.ENDPOINT_AGENT, offset, limit)
        return {
            "records": window,
            "offset": next_offset,
            "total": state.store.total(Source.ENDPOINT_AGENT),
            "limit": limit,
        }

    return app


app = create_app()
