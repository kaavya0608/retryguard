"""Service-level tests using FastAPI's in-process test client."""
from fastapi.testclient import TestClient

from services import backend


def test_backend_work_and_health():
    with TestClient(backend.app) as c:
        backend._state.update(waiting=0, in_flight=0, slow_factor=1.0, error_rate=0.0)
        assert c.post("/work").status_code == 200
        h = c.get("/health").json()
        assert h["workers"] == backend.WORKERS
        assert h["processed_total"] >= 1


def test_backend_fault_injection_and_reset():
    with TestClient(backend.app) as c:
        c.post("/admin/fault", json={"slow_factor": 1.0, "error_rate": 1.0})
        assert c.post("/work").status_code == 503
        c.post("/admin/reset")
        assert c.post("/work").status_code == 200


def test_backend_rejects_when_queue_full():
    with TestClient(backend.app) as c:
        backend._state["waiting"] = backend.MAX_QUEUE
        assert c.post("/work").status_code == 503
        backend._state["waiting"] = 0


def test_backend_metrics_endpoint():
    with TestClient(backend.app) as c:
        body = c.get("/metrics").text
        assert "rg_backend_queue_length" in body
