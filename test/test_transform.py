# test_health.py
from fastapi.testclient import TestClient
from apps.api.main import app


client = TestClient(app)


def test_healthz():
    """Basic /healthz endpoint check."""
    response = client.get("/healthz")

    # 状态码检查
    assert response.status_code == 200, f"Unexpected status: {response.status_code}"

    # 响应结构检查
    data = response.json()
    assert isinstance(data, dict), "Response must be a JSON object"
    assert data.get("status") == "ok", f"Unexpected response JSON: {data}"
