| Metric | baseline | retryguard |
|---|---|---|
| Success rate (overall) | 20.5% | 60.6% |
| Success rate (during fault) | 1.2% | 8.1% |
| Success rate (after fault) | 0.9% | 98.5% |
| High-priority success (during fault) | 1.5% | 18.2% |
| p50 latency, successes (s) | 0.048 | 0.050 |
| p95 latency, successes (s) | 0.084 | 0.202 |
| p99 latency, successes (s) | 2.111 | 0.432 |
| Retries performed | 5538 | 5 |
| Requests shed at admission | 0 | 761 |
| Requests failed | 1838 | 151 |
| Load amplification (backend reqs / client reqs) | 3.39x | 0.67x |
| Max backend queue length | 100 | 20 |
| Time to recover after fault ends (s) | not recovered | 1 |
| Max backend CPU / memory % | 10 / 0.35 | 4 / 0.34 |
