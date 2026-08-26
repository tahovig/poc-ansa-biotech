import os
from unittest.mock import patch

os.environ.setdefault("SFDC_CONSUMER_KEY", "test-key")
os.environ.setdefault("SFDC_USERNAME", "test@example.com")
os.environ.setdefault("SFDC_TOKEN_URL", "https://mock.my.salesforce.com/services/oauth2/token")
os.environ.setdefault("SFDC_JWT_PRIVATE_KEY_PATH", "/nonexistent")

from app import app  # noqa: E402


@patch("app.fetch_access_token")
@patch("app.build_assertion")
@patch("app._load_private_key")
def test_get_token_returns_access_token_and_instance_url(mock_load_key, mock_build, mock_fetch):
    mock_load_key.return_value = "fake-pem"
    mock_build.return_value = "mock.assertion.value"
    mock_fetch.return_value = {"access_token": "mock-token", "instance_url": "https://mock.my.salesforce.com"}

    client = app.test_client()
    resp = client.get("/token")

    assert resp.status_code == 200
    assert resp.get_json() == {"access_token": "mock-token", "instance_url": "https://mock.my.salesforce.com"}


@patch("app.fetch_access_token")
@patch("app.build_assertion")
@patch("app._load_private_key")
def test_get_token_returns_502_when_salesforce_call_fails(mock_load_key, mock_build, mock_fetch):
    mock_load_key.return_value = "fake-pem"
    mock_build.return_value = "mock.assertion.value"
    mock_fetch.side_effect = Exception("connection refused")

    client = app.test_client()
    resp = client.get("/token")

    assert resp.status_code == 502
    assert "error" in resp.get_json()
