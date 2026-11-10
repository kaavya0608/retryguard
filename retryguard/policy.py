"""Retry / load-shedding decision policies.

Two policies share one interface so the gateway can swap them:

* ``BaselinePolicy``   - conventional fixed retry count + fixed delay.
* ``RetryGuardPolicy`` - decisions based on live system state (queue length,
  latency, error rate, CPU), request priority and retry history.

The module is deliberately free of I/O so it can be unit-tested with an
injected clock and random generator.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict

HIGH = "high"
LOW = "low"


class Action(str, Enum):
    ALLOW = "allow"    # admit a new request
    RETRY = "retry"    # retry after a short (normal) backoff
    DELAY = "delay"    # retry after a congestion-stretched backoff
    REJECT = "reject"  # shed the request / give up, do not retry


@dataclass(frozen=True)
class Decision:
    action: Action
    delay: float = 0.0
    reason: str = ""


@dataclass
class HealthSnapshot:
    """What the policy can see about the downstream service right now."""

    queue_length: float = 0.0     # requests waiting for a worker
    workers: int = 1              # concurrent workers in the backend
    cpu_percent: float = 0.0      # backend process CPU %
    latency_ewma: float = 0.0     # seconds, observed by the gateway
    error_rate: float = 0.0       # 0..1 EWMA of recent failures
    stale: bool = False           # backend health endpoint unreachable


class HealthState:
    """Combines polled backend metrics with gateway-observed latency/errors.

    Observed EWMAs decay with time, so that after a burst of shedding (when
    no fresh samples arrive) the system can recover instead of staying stuck.
    """

    def __init__(
        self,
        latency_alpha: float = 0.2,
        error_alpha: float = 0.1,
        half_life: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.latency_alpha = latency_alpha
        self.error_alpha = error_alpha
        self.half_life = half_life
        self._clock = clock
        self._last_decay = clock()
        self._latency = 0.0
        self._error = 0.0
        self._queue = 0.0
        self._workers = 1
        self._cpu = 0.0
        self._stale = False

    def _decay(self) -> None:
        now = self._clock()
        dt = max(0.0, now - self._last_decay)
        self._last_decay = now
        factor = 0.5 ** (dt / self.half_life)
        self._latency *= factor
        self._error *= factor

    def update_backend(self, queue_length: float, workers: int, cpu_percent: float) -> None:
        self._queue, self._workers, self._cpu = queue_length, max(1, workers), cpu_percent
        self._stale = False

    def mark_backend_unreachable(self) -> None:
        self._stale = True

    def record(self, latency: float, ok: bool) -> None:
        self._decay()
        self._latency += self.latency_alpha * (latency - self._latency)
        self._error += self.error_alpha * ((0.0 if ok else 1.0) - self._error)

    def snapshot(self) -> HealthSnapshot:
        self._decay()
        return HealthSnapshot(
            queue_length=self._queue,
            workers=self._workers,
            cpu_percent=self._cpu,
            latency_ewma=self._latency,
            error_rate=self._error,
            stale=self._stale,
        )


class RetryBudget:
    """Token bucket: every first attempt earns ``ratio`` tokens, every retry
    costs one. Caps retries to roughly ``ratio`` of regular traffic."""

    def __init__(self, ratio: float = 0.1, burst: float = 10.0) -> None:
        self.ratio, self.burst = ratio, burst
        self.tokens = burst

    def deposit(self) -> None:
        self.tokens = min(self.burst, self.tokens + self.ratio)

    def try_withdraw(self) -> bool:
        if self.tokens >= 1.0 - 1e-9:   # epsilon: 10 x 0.1 must equal 1 token
            self.tokens = max(0.0, self.tokens - 1.0)
            return True
        return False


class BaselinePolicy:
    """Conventional approach: always admit, retry N times with a fixed delay."""

    name = "baseline"

    def __init__(self, max_retries: int = 3, delay: float = 0.1) -> None:
        self.max_retries, self.delay = max_retries, delay

    def congestion(self, snap: HealthSnapshot) -> float:
        return 0.0

    def on_request(self) -> None:
        pass

    def admit(self, priority: str, snap: HealthSnapshot) -> Decision:
        return Decision(Action.ALLOW)

    def on_failure(self, priority: str, attempt: int, snap: HealthSnapshot) -> Decision:
        if attempt >= self.max_retries:
            return Decision(Action.REJECT, reason="retries_exhausted")
        return Decision(Action.RETRY, delay=self.delay, reason="fixed_retry")


@dataclass
class RetryGuardConfig:
    max_retries: int = 2
    base_delay: float = 0.05
    max_delay: float = 1.0
    # --- congestion model: each signal is normalised so 1.0 == "saturated"
    queue_per_worker: float = 5.0   # queue length per worker that counts as 1.0
    latency_slo: float = 0.4        # seconds; EWMA latency / SLO
    error_target: float = 0.5       # error rate that counts as 1.0
    cpu_floor: float = 70.0         # CPU % where CPU pressure starts
    # --- thresholds per priority
    shed_start: Dict[str, float] = field(default_factory=lambda: {LOW: 0.5, HIGH: 0.9})
    shed_full: Dict[str, float] = field(default_factory=lambda: {LOW: 1.0, HIGH: 1.5})
    retry_block: Dict[str, float] = field(default_factory=lambda: {LOW: 0.6, HIGH: 1.0})
    delay_start: float = 0.3        # congestion at which backoff is stretched
    # --- retry budget
    budget_ratio: float = 0.1
    budget_burst: float = 10.0


class RetryGuardPolicy:
    """Queue- and health-aware retry / delay / shed decisions.

    * ``admit``      - probabilistic load shedding before the first attempt;
                       low-priority traffic is shed earlier than high-priority.
    * ``on_failure`` - after a failed attempt choose RETRY, DELAY or REJECT
                       using congestion, priority and the retry budget.
    """

    name = "retryguard"

    def __init__(
        self,
        config: RetryGuardConfig | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.cfg = config or RetryGuardConfig()
        self.rng = rng or random.Random()
        self.budget = RetryBudget(self.cfg.budget_ratio, self.cfg.budget_burst)

    # -- state estimation -------------------------------------------------
    def congestion(self, snap: HealthSnapshot) -> float:
        """Return a congestion score; ~0 idle, 1.0 saturated, >1 overloaded."""
        c = self.cfg
        queue = snap.queue_length / (max(1, snap.workers) * c.queue_per_worker)
        latency = snap.latency_ewma / c.latency_slo
        errors = snap.error_rate / c.error_target
        cpu = max(0.0, (snap.cpu_percent - c.cpu_floor) / (100.0 - c.cpu_floor))
        score = max(queue, latency, errors, cpu)
        if snap.stale:
            score = max(score, 1.0)
        return min(score, 3.0)

    # -- decisions --------------------------------------------------------
    def on_request(self) -> None:
        self.budget.deposit()

    def admit(self, priority: str, snap: HealthSnapshot) -> Decision:
        score = self.congestion(snap)
        start, full = self.cfg.shed_start[priority], self.cfg.shed_full[priority]
        p_shed = min(1.0, max(0.0, (score - start) / (full - start)))
        if p_shed > 0 and self.rng.random() < p_shed:
            return Decision(Action.REJECT, reason=f"shed_{priority}")
        return Decision(Action.ALLOW)

    def on_failure(self, priority: str, attempt: int, snap: HealthSnapshot) -> Decision:
        c = self.cfg
        if attempt >= c.max_retries:
            return Decision(Action.REJECT, reason="retries_exhausted")
        score = self.congestion(snap)
        if score >= c.retry_block[priority]:
            return Decision(Action.REJECT, reason="congested_no_retry")
        if not self.budget.try_withdraw():
            return Decision(Action.REJECT, reason="retry_budget_exhausted")
        backoff = min(c.max_delay, c.base_delay * (2 ** attempt))
        if score >= c.delay_start:
            stretched = min(c.max_delay, backoff * (1.0 + 4.0 * score))
            return Decision(Action.DELAY, delay=self.rng.uniform(0.5, 1.0) * stretched,
                            reason="congested_backoff")
        return Decision(Action.RETRY, delay=self.rng.uniform(0.0, backoff), reason="jittered_retry")


def make_policy(name: str) -> BaselinePolicy | RetryGuardPolicy:
    if name == "baseline":
        return BaselinePolicy()
    if name == "retryguard":
        return RetryGuardPolicy()
    raise ValueError(f"unknown policy: {name!r}")
