"""Run the baseline-vs-RetryGuard experiment end to end.

Starts a backend + gateway per policy, drives three phases
(warm-up -> injected slowdown -> recovery), records client-side results and a
backend time series, then writes CSVs, a summary and comparison plots.

    python -m experiments.run_experiment            # full run (~70 s)
    python -m experiments.run_experiment --quick    # shorter run
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import numpy as np

from experiments.loadgen import Phase, run_load

ROOT = Path(__file__).resolve().parent.parent
BACKEND_PORT, GATEWAY_PORT = 8101, 8100


def start_stack(policy: str) -> list[subprocess.Popen]:
    env = {**os.environ, "POLICY": policy, "BACKEND_URL": f"http://127.0.0.1:{BACKEND_PORT}"}
    procs = []
    for module, port in (("services.backend:app", BACKEND_PORT), ("services.gateway:app", GATEWAY_PORT)):
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "uvicorn", module, "--port", str(port), "--log-level", "warning"],
            cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    for port, path in ((BACKEND_PORT, "/health"), (GATEWAY_PORT, "/stats")):
        for _ in range(100):
            try:
                if httpx.get(f"http://127.0.0.1:{port}{path}", timeout=0.5).status_code == 200:
                    break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"service on port {port} did not start")
    return procs


def stop_stack(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


async def run_policy(policy: str, phases: list[Phase], slow_factor: float, outdir: Path) -> dict:
    print(f"\n=== {policy} ===", flush=True)
    procs = start_stack(policy)
    backend, gateway = f"http://127.0.0.1:{BACKEND_PORT}", f"http://127.0.0.1:{GATEWAY_PORT}"
    series: list[dict] = []
    stop = asyncio.Event()

    async def sampler() -> None:
        t0 = time.monotonic()
        async with httpx.AsyncClient(timeout=1.0) as c:
            while not stop.is_set():
                try:
                    b = (await c.get(f"{backend}/health")).json()
                    g = (await c.get(f"{gateway}/stats")).json()
                    series.append({
                        "t": round(time.monotonic() - t0, 3),
                        "queue_length": b["queue_length"], "in_flight": b["in_flight"],
                        "cpu_percent": b["cpu_percent"], "mem_percent": round(b["mem_percent"], 3),
                        "received_total": b["received_total"], "processed_total": b["processed_total"],
                        "congestion": round(g["congestion"], 3), "retries_total": g.get("retries", 0),
                    })
                except Exception:
                    pass
                await asyncio.sleep(0.5)

    async def on_phase(phase: Phase) -> None:
        factor = slow_factor if phase.name == "fault" else 1.0
        async with httpx.AsyncClient(timeout=2.0) as c:
            await c.post(f"{backend}/admin/fault", json={"slow_factor": factor})
        print(f"  phase {phase.name:<9} {phase.duration:>4.0f}s @ {phase.rate:.0f} rps"
              f"{'  (backend ' + str(slow_factor) + 'x slower)' if factor != 1 else ''}", flush=True)

    try:
        sampler_task = asyncio.create_task(sampler())
        records = await run_load(gateway, phases, on_phase_start=on_phase)
        stop.set()
        await sampler_task
        async with httpx.AsyncClient(timeout=2.0) as c:
            final = (await c.get(f"{backend}/health")).json()
            gstats = (await c.get(f"{gateway}/stats")).json()
    finally:
        stop_stack(procs)

    for name, rows in (("requests", records), ("timeseries", series)):
        with open(outdir / f"{policy}_{name}.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    return {"records": records, "series": series, "final": final, "gateway": gstats}


def _pct(values, q):
    return float(np.percentile(values, q)) if len(values) else float("nan")


def summarise(run: dict, phases: list[Phase]) -> dict:
    recs, series = run["records"], run["series"]
    fault_start = next(sum(p.duration for p in phases[:i]) for i, p in enumerate(phases) if p.name == "fault")
    fault_end = fault_start + next(p.duration for p in phases if p.name == "fault")

    def block(rs):
        n = len(rs)
        ok = [r for r in rs if r["outcome"] == "success"]
        hi = [r for r in rs if r["priority"] == "high"]
        hi_ok = [r for r in hi if r["outcome"] == "success"]
        lat_ok = [r["latency"] for r in ok]
        return {
            "requests": n,
            "success_rate": len(ok) / n if n else float("nan"),
            "high_priority_success_rate": len(hi_ok) / len(hi) if hi else float("nan"),
            "shed": sum(r["outcome"] == "shed" for r in rs),
            "failed": sum(r["outcome"] in ("failed", "client_timeout") for r in rs),
            "p50_success_latency_s": _pct(lat_ok, 50),
            "p95_success_latency_s": _pct(lat_ok, 95),
            "p99_success_latency_s": _pct(lat_ok, 99),
            "mean_attempts": float(np.mean([r["attempts"] for r in rs])) if n else float("nan"),
        }

    overall = block(recs)
    during = block([r for r in recs if fault_start <= r["t"] < fault_end])
    after = block([r for r in recs if r["t"] >= fault_end])
    n = len(recs)
    received = run["final"]["received_total"]
    # Service recovery: first moment after the fault from which every 1-second
    # window keeps a success rate >= 90% (None = not recovered within the run).
    recover = None
    end_t = max((r["t"] for r in recs), default=fault_end)
    t = fault_end
    bins = []
    while t < end_t:
        rs = [r for r in recs if t <= r["t"] < t + 1]
        bins.append((t, (sum(r["outcome"] == "success" for r in rs) / len(rs)) if rs else 1.0))
        t += 1
    for i in range(len(bins)):
        if all(rate >= 0.9 for _, rate in bins[i:]):
            recover = bins[i][0] - fault_end
            break
    return {
        "overall": overall, "during_fault": during, "after_fault": after,
        "backend_requests_received": received,
        "backend_requests_processed": run["final"]["processed_total"],
        "backend_requests_rejected": run["final"]["rejected_total"],
        "load_amplification": received / n if n else float("nan"),
        "retries": run["gateway"].get("retries", 0),
        "max_queue_length": max((s["queue_length"] for s in series), default=0),
        "max_cpu_percent": max((s["cpu_percent"] for s in series), default=0),
        "max_mem_percent": max((s["mem_percent"] for s in series), default=0),
        "service_recovery_time_s": recover,
        "fault_window_s": [fault_start, fault_end],
    }


def plot(results: dict, runs: dict, phases: list[Phase], outdir: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    colors = {"baseline": "#d1495b", "retryguard": "#2a9d8f"}
    f0, f1 = results["baseline"]["fault_window_s"] if "baseline" in results else results["retryguard"]["fault_window_s"]
    total = sum(p.duration for p in phases)
    fig, ax = plt.subplots(2, 2, figsize=(13, 8), sharex=True)

    def shade(a):
        a.axvspan(f0, f1, color="#f4a261", alpha=0.18, lw=0)
        a.set_xlim(0, total)
        a.grid(alpha=0.25)

    for policy, run in runs.items():
        c = colors[policy]
        ts = run["series"]
        t = np.array([s["t"] for s in ts])
        ax[0, 0].plot(t, [s["queue_length"] for s in ts], color=c, label=policy)
        recs = run["records"]
        bins = np.arange(0, total + 1, 1.0)
        mid, p95, succ, succ_hi = [], [], [], []
        for lo in bins[:-1]:
            rs = [r for r in recs if lo <= r["t"] < lo + 1]
            if not rs:
                continue
            ok = [r["latency"] for r in rs if r["outcome"] == "success"]
            hi = [r for r in rs if r["priority"] == "high"]
            mid.append(lo + 0.5)
            p95.append(_pct(ok, 95) if ok else np.nan)
            succ.append(100 * sum(r["outcome"] == "success" for r in rs) / len(rs))
            succ_hi.append(100 * sum(r["outcome"] == "success" for r in hi) / len(hi) if hi else np.nan)
        ax[0, 1].plot(mid, p95, color=c, label=policy)
        ax[1, 0].plot(mid, succ, color=c, label=f"{policy} (all)")
        ax[1, 0].plot(mid, succ_hi, color=c, ls="--", alpha=0.8, label=f"{policy} (high priority)")
        recv = np.array([s["received_total"] for s in ts], dtype=float)
        rate = np.diff(recv) / np.maximum(np.diff(t), 1e-6)
        ax[1, 1].plot(t[1:], rate, color=c, label=policy)

    ax[0, 0].set_title("Backend queue length")
    ax[0, 1].set_title("p95 latency of successful requests (s)")
    ax[1, 0].set_title("Success rate per second (%)")
    ax[1, 1].set_title("Requests reaching the backend per second (offered load)")
    for a in ax.flat:
        shade(a)
        a.legend(fontsize=8)
    for a in ax[1]:
        a.set_xlabel("time (s)  - shaded: injected backend slowdown")
    fig.suptitle("RetryGuard vs. baseline retry policy", fontsize=14)
    fig.tight_layout()
    fig.savefig(outdir / "comparison.png", dpi=130)
    plt.close(fig)


def write_markdown(results: dict, outdir: Path, args) -> None:
    rows = [
        ("Success rate (overall)", lambda r: f"{100*r['overall']['success_rate']:.1f}%"),
        ("Success rate (during fault)", lambda r: f"{100*r['during_fault']['success_rate']:.1f}%"),
        ("Success rate (after fault)", lambda r: f"{100*r['after_fault']['success_rate']:.1f}%"),
        ("High-priority success (during fault)", lambda r: f"{100*r['during_fault']['high_priority_success_rate']:.1f}%"),
        ("p50 latency, successes (s)", lambda r: f"{r['overall']['p50_success_latency_s']:.3f}"),
        ("p95 latency, successes (s)", lambda r: f"{r['overall']['p95_success_latency_s']:.3f}"),
        ("p99 latency, successes (s)", lambda r: f"{r['overall']['p99_success_latency_s']:.3f}"),
        ("Retries performed", lambda r: f"{r['retries']}"),
        ("Requests shed at admission", lambda r: f"{r['overall']['shed']}"),
        ("Requests failed", lambda r: f"{r['overall']['failed']}"),
        ("Load amplification (backend reqs / client reqs)", lambda r: f"{r['load_amplification']:.2f}x"),
        ("Max backend queue length", lambda r: f"{r['max_queue_length']}"),
        ("Time to recover after fault ends (s)", lambda r: "not recovered" if r['service_recovery_time_s'] is None else f"{r['service_recovery_time_s']:.0f}"),
        ("Max backend CPU / memory %", lambda r: f"{r['max_cpu_percent']:.0f} / {r['max_mem_percent']:.2f}"),
    ]
    names = list(results)
    lines = ["| Metric | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
    for label, fn in rows:
        lines.append(f"| {label} | " + " | ".join(fn(results[n]) for n in names) + " |")
    (outdir / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n" + "\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy", choices=["baseline", "retryguard", "both"], default="both")
    ap.add_argument("--quick", action="store_true", help="shorter phases")
    ap.add_argument("--rate", type=float, default=60, help="normal request rate (rps)")
    ap.add_argument("--fault-rate", type=float, default=80, help="request rate during the fault (rps)")
    ap.add_argument("--slow-factor", type=float, default=5.0, help="backend slowdown during the fault")
    ap.add_argument("--out", default=str(ROOT / "results"))
    args = ap.parse_args()

    d = (5, 8, 10) if args.quick else (8, 12, 15)
    phases = [Phase("warmup", d[0], args.rate), Phase("fault", d[1], args.fault_rate),
              Phase("recovery", d[2], args.rate)]
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    policies = ["baseline", "retryguard"] if args.policy == "both" else [args.policy]

    runs, results = {}, {}
    for p in policies:
        runs[p] = asyncio.run(run_policy(p, phases, args.slow_factor, outdir))
        results[p] = summarise(runs[p], phases)
    (outdir / "summary.json").write_text(json.dumps(results, indent=2, default=float))
    write_markdown(results, outdir, args)
    plot(results, runs, phases, outdir)
    print(f"\nResults written to {outdir}")


if __name__ == "__main__":
    main()
