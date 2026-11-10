"""Backend microservice with a bounded worker pool, a request queue and
fault injection.  It exposes health + Prometheus metrics so RetryGuard can
observe it.

Run:  uvicorn services.backend:app --port 8101
"""
from __future__ import annotations

import asyncio
import os
import random

import psutil
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from pydantic import BaseModel

WORKERS = int(os.getenv("WORKERS", "4"))
SERVICE_TIME = float(os.getenv("SERVICE_TIME", "0.04"))   # seconds per request
MAX_QUEUE = int(os.getenv("MAX_QUEUE", "100"))            # hard admission limit

app = FastAPI(title="RetryGuard demo backend")
_proc = psutil.Process()
_proc.cpu_percent(None)  # prime the counter
_NCPU = psutil.cpu_count() or 1

_state = {
    "sem": None,
    "waiting": 0,
    "in_flight": 0,
    "received": 0,
    "processed": 0,
    "rejected": 0,
    "slow_factor": 1.0,
    "error_rate": 0.0,
}

G_QUEUE = Gauge("rg_backend_queue_length", "Requests waiting for a worker")
G_INFLIGHT = Gauge("rg_backend_in_flight", "Requests being processed")
C_RECEIVED = Counter("rg_backend_requests_received_total", "Requests received")
C_PROCESSED = Counter("rg_backend_requests_processed_total", "Requests processed")
C_REJECTED = Counter("rg_backend_requests_rejected_total", "Requests rejected (queue full / injected error)")


def _sem() -> asyncio.Semaphore:
    if _state["sem"] is None:
        _state["sem"] = asyncio.Semaphore(WORKERS)
    return _state["sem"]


@app.post("/work")
async def work() -> JSONResponse:
    """One unit of work. Note: like many real services, it keeps working on a
    request even if the caller has already timed out (wasted work)."""
    _state["received"] += 1
    C_RECEIVED.inc()
    if _state["waiting"] >= MAX_QUEUE or random.random() < _state["error_rate"]:
        _state["rejected"] += 1
        C_REJECTED.inc()
        return JSONResponse({"ok": False, "error": "overloaded"}, status_code=503)

    _state["waiting"] += 1
    try:
        await _sem().acquire()
    finally:
        _state["waiting"] -= 1
    _state["in_flight"] += 1
    try:
        await asyncio.sleep(SERVICE_TIME * _state["slow_factor"] * random.uniform(0.8, 1.2))
        _state["processed"] += 1
        C_PROCESSED.inc()
        return JSONResponse({"ok": True})
    finally:
        _state["in_flight"] -= 1
        _sem().release()


@app.get("/health")
async def health() -> dict:
    return {
        "queue_length": _state["waiting"],
        "in_flight": _state["in_flight"],
        "workers": WORKERS,
        "queue_capacity": MAX_QUEUE,
        "cpu_percent": _proc.cpu_percent(None) / _NCPU,   # normalised to 0-100
        "mem_percent": _proc.memory_percent(),
        "received_total": _state["received"],
        "processed_total": _state["processed"],
        "rejected_total": _state["rejected"],
        "slow_factor": _state["slow_factor"],
    }


class Fault(BaseModel):
    slow_factor: float = 1.0   # multiplies service time (5.0 => 5x slower)
    error_rate: float = 0.0    # probability of an immediate 503


@app.post("/admin/fault")
async def set_fault(fault: Fault) -> dict:
    _state["slow_factor"], _state["error_rate"] = fault.slow_factor, fault.error_rate
    return fault.model_dump()


@app.post("/admin/reset")
async def reset() -> dict:
    _state.update(slow_factor=1.0, error_rate=0.0)
    return {"ok": True}


@app.get("/metrics")
async def metrics() -> Response:
    G_QUEUE.set(_state["waiting"])
    G_INFLIGHT.set(_state["in_flight"])
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
