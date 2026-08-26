import json
from app import app


def test_post_feasibility_returns_score():
    client = app.test_client()
    resp = client.post("/feasibility", json={"sequence": "ACGTACGTACGTACGTACGT"})
    assert resp.status_code == 200
    body = resp.get_json()
    assert "score" in body and "feasible" in body


def test_post_feasibility_missing_sequence_returns_400():
    client = app.test_client()
    resp = client.post("/feasibility", json={})
    assert resp.status_code == 400
