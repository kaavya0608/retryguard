import random

from retryguard import (
    Action, BaselinePolicy, HealthSnapshot, HealthState, RetryBudget,
    RetryGuardPolicy, HIGH, LOW,
)


def snap(**kw):
    return HealthSnapshot(**{"workers": 4, **kw})


def policy(seed=0):
    return RetryGuardPolicy(rng=random.Random(seed))


# ---------------------------------------------------------------- baseline
def test_baseline_retries_fixed_times_then_gives_up():
    p = BaselinePolicy(max_retries=3, delay=0.1)
    s = snap(queue_length=1000)  # baseline ignores load entirely
    assert p.admit(LOW, s).action is Action.ALLOW
    for attempt in range(3):
        d = p.on_failure(LOW, attempt, s)
        assert d.action is Action.RETRY and d.delay == 0.1
    assert p.on_failure(LOW, 3, s).action is Action.REJECT


# --------------------------------------------------------------- congestion
def test_congestion_is_low_when_idle_and_high_when_queue_builds():
    p = policy()
    assert p.congestion(snap()) < 0.1
    assert p.congestion(snap(queue_length=40)) >= 1.0   # 40 / (4 workers * 5)


def test_congestion_uses_worst_signal():
    p = policy()
    assert p.congestion(snap(latency_ewma=0.8)) == 2.0   # 0.8 / 0.4 SLO
    assert p.congestion(snap(error_rate=0.5)) == 1.0
    assert p.congestion(snap(cpu_percent=100)) == 1.0
    assert p.congestion(snap(cpu_percent=50)) == 0.0     # below cpu_floor


def test_stale_health_counts_as_saturated():
    assert policy().congestion(snap(stale=True)) >= 1.0


# ---------------------------------------------------------------- shedding
def test_no_shedding_when_healthy():
    p = policy()
    assert all(p.admit(LOW, snap()).action is Action.ALLOW for _ in range(200))


def test_low_priority_is_shed_before_high_priority():
    p = policy()
    mid = snap(queue_length=16)       # congestion 0.8: low sheds, high does not
    low_shed = sum(p.admit(LOW, mid).action is Action.REJECT for _ in range(500))
    high_shed = sum(p.admit(HIGH, mid).action is Action.REJECT for _ in range(500))
    assert low_shed > 100
    assert high_shed == 0


def test_everything_low_priority_shed_when_saturated():
    p = policy()
    sat = snap(queue_length=100)
    assert all(p.admit(LOW, sat).action is Action.REJECT for _ in range(100))


# ----------------------------------------------------------------- retries
def test_retry_is_quick_and_jittered_when_healthy():
    p = policy()
    p.on_request()
    d = p.on_failure(LOW, 0, snap())
    assert d.action is Action.RETRY
    assert 0.0 <= d.delay <= p.cfg.base_delay


def test_retry_is_delayed_when_moderately_congested():
    p = policy()
    d = p.on_failure(HIGH, 0, snap(queue_length=8))   # congestion 0.4
    assert d.action is Action.DELAY
    assert d.delay > p.cfg.base_delay


def test_no_retry_when_congested():
    p = policy()
    heavy = snap(queue_length=20)                     # congestion 1.0
    assert p.on_failure(LOW, 0, heavy).action is Action.REJECT
    assert p.on_failure(HIGH, 0, heavy).action is Action.REJECT


def test_retries_limited_by_max_retries():
    p = policy()
    assert p.on_failure(LOW, p.cfg.max_retries, snap()).reason == "retries_exhausted"


def test_retry_budget_caps_retry_volume():
    p = policy()
    allowed = sum(p.on_failure(LOW, 0, snap()).action is not Action.REJECT for _ in range(100))
    assert allowed == int(p.cfg.budget_burst)         # only the burst allowance, no new requests
    for _ in range(50):
        p.on_request()                                # 50 requests earn 5 tokens
    allowed = sum(p.on_failure(LOW, 0, snap()).action is not Action.REJECT for _ in range(100))
    assert allowed == 5


def test_retry_budget_token_bucket():
    b = RetryBudget(ratio=0.5, burst=2)
    assert b.try_withdraw() and b.try_withdraw() and not b.try_withdraw()
    b.deposit(); b.deposit()
    assert b.try_withdraw()


# ------------------------------------------------------------ health state
def test_health_state_ewma_and_time_decay():
    now = [0.0]
    h = HealthState(half_life=1.0, clock=lambda: now[0])
    for _ in range(30):
        h.record(1.0, ok=False)
    s = h.snapshot()
    assert s.latency_ewma > 0.9 and s.error_rate > 0.9
    now[0] += 3.0                                     # 3 half-lives of silence
    s = h.snapshot()
    assert s.latency_ewma < 0.2 and s.error_rate < 0.2


def test_health_state_backend_update_and_stale():
    h = HealthState()
    h.update_backend(queue_length=7, workers=4, cpu_percent=55)
    s = h.snapshot()
    assert (s.queue_length, s.workers, s.cpu_percent, s.stale) == (7, 4, 55, False)
    h.mark_backend_unreachable()
    assert h.snapshot().stale
