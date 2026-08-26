import os
from unittest.mock import patch

import pytest

os.environ.setdefault("SFDC_CONSUMER_KEY", "test-key")
os.environ.setdefault("SFDC_USERNAME", "test@example.com")
os.environ.setdefault("SFDC_TOKEN_URL", "https://mock.my.salesforce.com/services/oauth2/token")
os.environ.setdefault("SFDC_JWT_PRIVATE_KEY_PATH", "/nonexistent")

import app  # noqa: E402


@pytest.fixture(autouse=True)
def reset_token_cache():
    # /token caches its result in-process (see app.py's comment); without
    # resetting between tests, whichever test runs first populates the
    # cache and every later test silently gets that cached token back
    # instead of exercising its own mocks.
    app._cached_token = None
    app._cached_at = 0.0


@patch("app.fetch_access_token")
@patch("app.build_assertion")
@patch("app._load_private_key")
def test_get_token_returns_access_token_and_instance_url(mock_load_key, mock_build, mock_fetch):
    mock_load_key.return_value = "fake-pem"
    mock_build.return_value = "mock.assertion.value"
    mock_fetch.return_value = {"access_token": "mock-token", "instance_url": "https://mock.my.salesforce.com"}

    client = app.app.test_client()
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

    client = app.app.test_client()
    resp = client.get("/token")

    assert resp.status_code == 502
    assert "error" in resp.get_json()


@patch("app.fetch_access_token")
@patch("app.build_assertion")
@patch("app._load_private_key")
def test_get_token_reuses_cached_token_within_ttl(mock_load_key, mock_build, mock_fetch):
    mock_load_key.return_value = "fake-pem"
    mock_build.return_value = "mock.assertion.value"
    mock_fetch.return_value = {"access_token": "mock-token", "instance_url": "https://mock.my.salesforce.com"}

    client = app.app.test_client()
    client.get("/token")
    client.get("/token")

    assert mock_fetch.call_count == 1
