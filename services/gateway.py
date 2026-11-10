"""API gateway: the caller that owns the retry policy.

Client -> Gateway (policy: baseline | retryguard) -> Backend

Run:  POLICY=retryguard BACKEND_URL=http://localhost:8101 \
      uvicorn services.gateway:app --port 8100
"""
from __future__ import annotations

import asyncio
import collections
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel

from retryguard import Action, HealthState, make_policy

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8101")
ATTEMPT_TIMEOUT = float(os.getenv("ATTEMPT_TIMEOUT", "0.5"))   # per-attempt timeout (s)
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.1"))       # health poll period (s)

C_REQ = Counter("rg_gateway_requests_total", "Client requests by outcome", ["outcome", "priority"])
C_ATTEMPTS = Counter("rg_gateway_backend_attempts_total", "Calls sent to the backend")
C_DECISION = Counter("rg_gateway_decisions_total", "Policy decisions", ["action", "reason"])
H_LAT = Histogram("rg_gateway_request_seconds", "End-to-end gateway latency",
                  buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5))
G_CONG = Gauge("rg_gateway_congestion_score", "RetryGuard congestion score")

state: dict = {}
stats: collections.Counter = collections.Counter()


async def _poll_backend() -> None:
    client: httpx.AsyncClient = state["client"]
    health: HealthState = state["health"]
    while True:
        try:
            r = await client.get(f"{BACKEND_URL}/health", timeout=0.3)
            d = r.json()
            health.update_backend(d["queue_length"], d["workers"], d["cpu_percent"])
        except Exception:
            health.mark_backend_unreachable()
        G_CONG.set(state["policy"].congestion(health.snapshot()))
        await asyncio.sleep(POLL_INTERVAL)


@asynccontextmanager
async def lifespan(_: FastAPI):
    state["client"] = httpx.AsyncClient(limits=httpx.Limits(max_connections=2000,
                                                            max_keepalive_connections=500))
    state["health"] = HealthState()
    state["policy"] = make_policy(os.getenv("POLICY", "retryguard"))
    task = asyncio.create_task(_poll_backend())
    yield
    task.cancel()
    await state["client"].aclose()


app = FastAPI(title="RetryGuard gateway", lifespan=lifespan)


async def _call_backend(priority: str) -> tuple[bool, str]:
    client: httpx.AsyncClient = state["client"]
    health: HealthState = state["health"]
    C_ATTEMPTS.inc()
    stats["backend_attempts"] += 1
    t0 = time.monotonic()
    try:
        r = await client.post(f"{BACKEND_URL}/work", headers={"X-Priority": priority},
                              timeout=ATTEMPT_TIMEOUT)
        ok, reason = r.status_code == 200, f"http_{r.status_code}"
    except httpx.HTTPError as exc:
        ok, reason = False, type(exc).__name__
    health.record(time.monotonic() - t0, ok)
    return ok, reason


@app.post("/request")
async def handle(priority: str = Query("low", pattern="^(high|low)$")) -> JSONResponse:
    policy, health = state["policy"], state["health"]
    t0 = time.monotonic()

    def done(outcome: str, status: int, attempts: int, reason: str = "") -> JSONResponse:
        C_REQ.labels(outcome, priority).inc()
        H_LAT.observe(time.monotonic() - t0)
        stats[outcome] += 1
        body = {"outcome": outcome, "attempts": attempts, "reason": reason}
        headers = {"Retry-After": "1"} if outcome == "shed" else None
        return JSONResponse(body, status_code=status, headers=headers)

    stats["requests"] += 1
    decision = policy.admit(priority, health.snapshot())
    if decision.action is Action.REJECT:
        C_DECISION.labels("reject", decision.reason).inc()
        return done("shed", 503, 0, decision.reason)
    policy.on_request()

    attempt = 0
    while True:
        ok, reason = await _call_backend(priority)
        if ok:
            return done("success", 200, attempt + 1)
        decision = policy.on_failure(priority, attempt, health.snapshot())
        C_DECISION.labels(decision.action.value, decision.reason).inc()
        if decision.action is Action.REJECT:
            return done("failed", 502, attempt + 1, f"{reason}/{decision.reason}")
        stats["retries"] += 1
        stats[f"retries_{decision.action.value}"] += 1
        await asyncio.sleep(decision.delay)
        attempt += 1


class PolicyChoice(BaseModel):
    name: str


@app.post("/admin/policy")
async def set_policy(choice: PolicyChoice) -> dict:
    state["policy"] = make_policy(choice.name)
    stats.clear()
    return {"policy": choice.name}


@app.get("/stats")
async def get_stats() -> dict:
    snap = state["health"].snapshot()
    return {
        "policy": state["policy"].name,
        "congestion": state["policy"].congestion(snap),
        "latency_ewma": snap.latency_ewma,
        "error_rate": snap.error_rate,
        **dict(stats),
    }


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
