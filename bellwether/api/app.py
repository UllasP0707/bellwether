"""The read API.

Every dependency is injected, so the whole surface — including tenant isolation
and the audit log — is testable against in-memory stores with no broker, no
Redis and no database.

Two decisions run through all of it.

**Tenancy is a property of the credential.** No endpoint takes a tenant, so
there is no parameter a caller can set to reach across one. A lookup for an
employee in somebody else's tenant returns 404 rather than 403, because
distinguishing "not yours" from "does not exist" tells the caller that the
person exists.

**Live reads come from Redis; history does not come from here at all.** Per
employee lookups and the population ranking are point and range queries against
the projection the scorer writes. Trend over time, cohort comparison and
anything needing a month of history belong in the marts (day 7): serving those
from an online store means scanning it, and an online store that gets scanned
stops being fast for the queries it exists for.
"""

from __future__ import annotations

import statistics
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, Security
from fastapi import Path as PathParam
from fastapi.responses import HTMLResponse
from fastapi.security import APIKeyHeader
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from bellwether.api.audit import InMemoryAudit, ReadAudit
from bellwether.api.models import (
    CatalogEntry,
    DepartmentRisk,
    EmployeeScore,
    FactorOut,
    InterventionOut,
    RankedScore,
    Timeline,
    TimelineEntry,
    catalog_entry,
)
from bellwether.api.security import Principal
from bellwether.dimension import EmployeeRepository
from bellwether.events.schema import SignalType
from bellwether.events.scores import RiskScoreEvent
from bellwether.interventions.policy import InterventionLedger
from bellwether.obs import metrics
from bellwether.scoring import RiskBand, contribution_of
from bellwether.scoring.catalog import CATALOG, spec_for
from bellwether.stream.store import EventWindow, ScoreReader

DASHBOARD = Path(__file__).parent / "dashboard.html"

# The ranking is bounded rather than unbounded, and the cap is low on purpose.
# "Show me everyone" against an online store is a scan, and a caller who wants
# the whole population wants the marts.
MAX_PAGE = 200


@dataclass(frozen=True)
class TenantContext:
    """Everything one tenant's requests are allowed to touch."""

    scores: ScoreReader
    employees: EmployeeRepository
    interventions: InterventionLedger
    window: EventWindow | None = None


def _factors(score: RiskScoreEvent) -> list[FactorOut]:
    out: list[FactorOut] = []
    for factor in score.top_factors:
        try:
            description = spec_for(SignalType(factor.signal)).description
        except (ValueError, KeyError):  # pragma: no cover - a signal this build predates
            description = factor.signal
        out.append(
            FactorOut(
                signal=factor.signal,
                description=description,
                category=factor.category,
                occurrences=factor.occurrences,
                contribution=factor.contribution,
            )
        )
    return out


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(int(p / 100 * len(ordered)), len(ordered) - 1)]


def _route(request: Request) -> str:
    """The matched route template, or `unmatched` for a 404.

    Falling back to the raw path here would reintroduce the cardinality the
    middleware exists to avoid, and a scanner walking random URLs would be
    enough to do it.
    """
    route = request.scope.get("route")
    return str(getattr(route, "path", None) or "unmatched")


_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def caller(request: Request, key: Annotated[str | None, Security(_HEADER)]) -> Principal:
    """Resolve the credential, or refuse.

    Reads the key table off `app.state` rather than closing over it so the
    dependency is a plain module-level function — which is what lets the tenant
    annotation below be written once and reused by every handler, instead of
    each one re-deriving who is asking.
    """
    principals: dict[str, Principal] = request.app.state.principals
    tenants: dict[str, TenantContext] = request.app.state.tenants

    principal = principals.get(key) if key else None
    if principal is None:
        raise HTTPException(status_code=401, detail="unknown or missing API key")
    if principal.tenant_id not in tenants:
        raise HTTPException(status_code=403, detail="tenant not served here")
    return principal


Caller = Annotated[Principal, Depends(caller)]

# Constrained at the edge. An employee id reaches Redis key construction and a
# SQL parameter, and while neither is injectable as written, an identifier that
# can contain anything is a standing invitation for one of them to become so.
EmployeeId = Annotated[str, PathParam(pattern=r"^[A-Za-z0-9_.-]{1,64}$")]


def create_app(
    tenants: dict[str, TenantContext],
    principals: dict[str, Principal],
    audit: ReadAudit | None = None,
    lookback_days: int = 30,
) -> FastAPI:
    """Build the app over the given stores."""
    log = audit or InMemoryAudit()
    app = FastAPI(
        title="Bellwether",
        description="Human risk scoring: who is risky right now, why, and what was done.",
        version="1.0.0",
        docs_url="/docs",
    )
    app.state.principals = principals
    app.state.tenants = tenants

    @app.middleware("http")
    async def observe(request: Request, call_next: Any) -> Response:
        """Count and time every request, labelled by route template.

        The *template* — `/v1/employees/{employee_id}/score` — and never the
        resolved path. Labelling by path would create one time series per
        employee ever looked up, which is unbounded cardinality and, worse, a
        list on an unauthenticated endpoint of exactly whose risk score the
        security team has been reading. The audit log records that on purpose,
        behind a credential; the metrics endpoint must not record it by
        accident.
        """
        with metrics.timed(metrics.api_request_seconds, route=_route(request)):
            response: Response = await call_next(request)
        metrics.api_requests.labels(
            route=_route(request), status=f"{response.status_code // 100}xx"
        ).inc()
        return response

    @app.get("/metrics", include_in_schema=False)
    def prometheus() -> Response:
        return Response(generate_latest(metrics.REGISTRY), media_type=CONTENT_TYPE_LATEST)

    def context(principal: Principal) -> TenantContext:
        return tenants[principal.tenant_id]

    def looked_at(principal: Principal, employee_id: str, endpoint: str) -> None:
        log.record(principal.actor, principal.tenant_id, employee_id, endpoint)

    # --- health and model ---------------------------------------------------

    @app.get("/healthz", tags=["meta"])
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/catalog", tags=["meta"])
    def catalog(_: Caller) -> list[CatalogEntry]:
        """The scoring model itself.

        Published so a security team can answer "why is this person a 78" with
        the actual weights rather than with "the algorithm decided".
        """
        return [catalog_entry(signal) for signal in CATALOG]

    # --- one person ---------------------------------------------------------

    @app.get("/v1/employees/{employee_id}/score", tags=["employee"])
    def employee_score(principal: Caller, employee_id: EmployeeId) -> EmployeeScore:
        """One employee's current score. Named, and audited."""
        ctx = context(principal)
        employee = ctx.employees.get(employee_id)
        if employee is None:
            # 404 for another tenant's employee too: a 403 would confirm they
            # exist, which is the thing tenancy is supposed to hide.
            raise HTTPException(status_code=404, detail="no such employee")

        looked_at(principal, employee_id, "score")
        score = ctx.scores.latest(employee_id)
        if score is None:
            raise HTTPException(status_code=404, detail="employee has no score yet")

        return EmployeeScore(
            employee_id=employee.employee_id,
            display_name=employee.display_name,
            department=employee.department,
            seniority=employee.seniority,
            is_high_value_target=employee.is_high_value_target,
            score=score.score,
            band=score.band,
            previous_band=score.previous_band,
            as_of=score.as_of,
            dominant_category=score.dominant_category,
            by_category=score.by_category,
            top_factors=_factors(score),
            events_considered=score.events_considered,
        )

    @app.get("/v1/employees/{employee_id}/timeline", tags=["employee"])
    def employee_timeline(
        principal: Caller,
        employee_id: EmployeeId,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = 50,
    ) -> Timeline:
        """What is in the window, and what each event is worth today."""
        ctx = context(principal)
        employee = ctx.employees.get(employee_id)
        if employee is None:
            raise HTTPException(status_code=404, detail="no such employee")
        if ctx.window is None:
            raise HTTPException(status_code=501, detail="no event window configured")

        looked_at(principal, employee_id, "timeline")
        as_of = datetime.now(UTC)
        entries: list[TimelineEntry] = []
        for event in ctx.window.events(employee_id):
            age_days = (as_of - event.occurred_at).total_seconds() / 86400.0
            if age_days > lookback_days:
                continue
            spec = spec_for(event.signal)
            entries.append(
                TimelineEntry(
                    signal=event.signal,
                    description=spec.description,
                    category=spec.category,
                    occurred_at=event.occurred_at,
                    age_days=round(age_days, 2),
                    contribution=round(
                        contribution_of(
                            event.signal,
                            event.occurred_at,
                            as_of,
                            employee.is_high_value_target,
                        ),
                        3,
                    ),
                )
            )

        entries.sort(key=lambda e: e.occurred_at, reverse=True)
        return Timeline(employee_id=employee_id, as_of=as_of, entries=entries[:limit])

    @app.get("/v1/employees/{employee_id}/interventions", tags=["employee"])
    def employee_interventions(
        principal: Caller,
        employee_id: EmployeeId,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = 20,
    ) -> list[InterventionOut]:
        """What this person has actually been sent."""
        ctx = context(principal)
        if ctx.employees.get(employee_id) is None:
            raise HTTPException(status_code=404, detail="no such employee")

        looked_at(principal, employee_id, "interventions")
        return [
            InterventionOut(
                intervention_id=i.intervention_id,
                type=i.type.value,
                channel=i.channel.value,
                trigger_signal=i.trigger_signal,
                band=i.band,
                score=i.score,
                subject=i.subject,
                body=i.body,
                copy_source=i.copy_source.value,
                created_at=i.created_at,
            )
            for i in ctx.interventions.history(principal.tenant_id, employee_id, limit=limit)
        ]

    # --- the population -----------------------------------------------------

    @app.get("/v1/population/ranking", tags=["population"])
    def ranking(
        principal: Caller,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = 25,
        offset: Annotated[int, Query(ge=0)] = 0,
        band: RiskBand | None = None,
    ) -> list[RankedScore]:
        """Riskiest first. Pseudonymous, and deliberately not audited."""
        ctx = context(principal)
        # Over-read when filtering, because the sorted set ranks by score and
        # knows nothing about bands. Bands are contiguous score ranges, so this
        # cannot miss anyone above the cut — it only trims the tail.
        window = limit * 5 if band else limit
        rows = ctx.scores.ranking(limit=window, offset=offset)
        if band is not None:
            rows = [r for r in rows if r.band is band][:limit]
        return [RankedScore.of(r, _department(ctx, r.employee_id)) for r in rows]

    @app.get("/v1/population/departments", tags=["population"])
    def departments(principal: Caller) -> list[DepartmentRisk]:
        """Risk by department, computed live over the projection.

        Honest about its limits: this reads every scored employee and folds them
        in Python, which is fine for one company's headcount and the wrong shape
        for a trend, a cohort, or anything spanning more than the current
        instant. Those are the marts' job.
        """
        ctx = context(principal)
        everyone = ctx.employees.all()
        scores = {s.employee_id: s for s in _all_scores(ctx)}

        grouped: dict[str, list[float]] = {}
        headcount: dict[str, int] = {}
        bands: dict[str, dict[RiskBand, int]] = {}
        for employee in everyone:
            headcount[employee.department] = headcount.get(employee.department, 0) + 1
            score = scores.get(employee.employee_id)
            if score is None:
                continue
            grouped.setdefault(employee.department, []).append(score.score)
            counts = bands.setdefault(employee.department, {})
            counts[score.band] = counts.get(score.band, 0) + 1

        return sorted(
            (
                DepartmentRisk(
                    department=department,
                    headcount=headcount.get(department, 0),
                    scored=len(values),
                    mean_score=round(statistics.fmean(values), 2),
                    p90_score=round(_percentile(values, 90), 2),
                    critical=bands.get(department, {}).get(RiskBand.CRITICAL, 0),
                    high=bands.get(department, {}).get(RiskBand.HIGH, 0),
                )
                for department, values in grouped.items()
            ),
            key=lambda d: d.mean_score,
            reverse=True,
        )

    # --- the audit log itself -------------------------------------------------

    @app.get("/v1/audit", tags=["meta"])
    def reads(
        principal: Caller, limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = 50
    ) -> list[dict[str, str]]:
        """Who has been looking at whom.

        Readable rather than write-only, because an audit log nobody can query
        deters nothing.
        """
        return [
            {
                "actor": r.actor,
                "employee_id": r.employee_id,
                "endpoint": r.endpoint,
                "read_at": r.read_at.isoformat(),
            }
            for r in log.recent(principal.tenant_id, limit=limit)
        ]

    @app.get("/", include_in_schema=False)
    def dashboard() -> HTMLResponse:
        if not DASHBOARD.exists():  # pragma: no cover - packaging accident
            raise HTTPException(status_code=404, detail="dashboard not installed")
        return HTMLResponse(DASHBOARD.read_text())

    def _department(ctx: TenantContext, employee_id: str) -> str | None:
        employee = ctx.employees.get(employee_id)
        return employee.department if employee else None

    def _all_scores(ctx: TenantContext) -> Iterable[RiskScoreEvent]:
        """Every scored employee, one page at a time."""
        offset = 0
        while True:
            page = ctx.scores.ranking(limit=MAX_PAGE, offset=offset)
            if not page:
                return
            yield from page
            offset += MAX_PAGE

    return app
