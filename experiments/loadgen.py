"""Open-loop (Poisson) load generator with phases.

Standalone use against a running stack (e.g. docker compose):
    python -m experiments.loadgen --target http://localhost:8100 --rate 60 --duration 60
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, List, Optional

import httpx


@dataclass
class Phase:
    name: str
    duration: float   # seconds
    rate: float       # requests / second


async def run_load(
    target: str,
    phases: List[Phase],
    on_phase_start: Optional[Callable[[Phase], Awaitable[None]]] = None,
    high_fraction: float = 0.2,
    client_timeout: float = 5.0,
    seed: int = 1,
) -> List[dict]:
    rng = random.Random(seed)
    records: List[dict] = []
    limits = httpx.Limits(max_connections=2000, max_keepalive_connections=500)
    t_zero = time.monotonic()

    async with httpx.AsyncClient(limits=limits, timeout=client_timeout) as client:

        async def fire(priority: str, phase: str) -> None:
            t_start = time.monotonic() - t_zero
            t0 = time.monotonic()
            try:
                r = await client.post(f"{target}/request", params={"priority": priority})
                body = r.json()
                outcome, attempts = body.get("outcome", "error"), body.get("attempts", 0)
            except Exception:
                outcome, attempts = "client_timeout", 0
            records.append({
                "t": round(t_start, 4), "phase": phase, "priority": priority,
                "latency": round(time.monotonic() - t0, 4),
                "outcome": outcome, "attempts": attempts,
            })

        tasks: List[asyncio.Task] = []
        for phase in phases:
            if on_phase_start:
                await on_phase_start(phase)
            phase_end = time.monotonic() + phase.duration
            next_t = time.monotonic()
            while True:
                next_t += rng.expovariate(phase.rate)
                if next_t >= phase_end:
                    break
                delay = next_t - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                prio = "high" if rng.random() < high_fraction else "low"
                tasks.append(asyncio.create_task(fire(prio, phase.name)))
            remaining = phase_end - time.monotonic()
            if remaining > 0:
                await asyncio.sleep(remaining)
        await asyncio.gather(*tasks)
    return records


def _cli() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="http://localhost:8100")
    ap.add_argument("--rate", type=float, default=60)
    ap.add_argument("--duration", type=float, default=30)
    args = ap.parse_args()
    recs = asyncio.run(run_load(args.target, [Phase("steady", args.duration, args.rate)]))
    ok = sum(r["outcome"] == "success" for r in recs)
    print(f"sent={len(recs)} success={ok} ({100*ok/max(1,len(recs)):.1f}%)")


if __name__ == "__main__":
    _cli()
