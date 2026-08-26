import time
from unittest.mock import Mock, patch

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from auth import build_assertion, fetch_access_token


def _generate_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    public_key = private_key.public_key()
    return private_pem, public_key


def test_build_assertion_produces_a_correctly_claimed_signed_jwt():
    private_pem, public_key = _generate_keypair()
    assertion = build_assertion(
        consumer_key="consumer-key-123",
        username="user@example.com",
        audience="https://login.salesforce.com",
        private_key=private_pem,
    )
    claims = jwt.decode(assertion, public_key, algorithms=["RS256"], audience="https://login.salesforce.com")
    assert claims["iss"] == "consumer-key-123"
    assert claims["sub"] == "user@example.com"
    assert claims["aud"] == "https://login.salesforce.com"
    assert claims["exp"] > int(time.time())


def test_build_assertion_expiry_is_within_five_minutes():
    private_pem, public_key = _generate_keypair()
    before = int(time.time())
    assertion = build_assertion(
        consumer_key="k", username="u", audience="https://login.salesforce.com", private_key=private_pem
    )
    claims = jwt.decode(assertion, public_key, algorithms=["RS256"], audience="https://login.salesforce.com")
    assert 0 < claims["exp"] - before <= 300


@patch("auth.requests.post")
def test_fetch_access_token_posts_jwt_bearer_grant_and_returns_shape(mock_post):
    mock_response = Mock()
    mock_response.json.return_value = {
        "access_token": "mock-token",
        "instance_url": "https://mock.my.salesforce.com",
        "token_type": "Bearer",
    }
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    result = fetch_access_token("https://mock.my.salesforce.com/services/oauth2/token", "mock.assertion.value")

    mock_post.assert_called_once_with(
        "https://mock.my.salesforce.com/services/oauth2/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": "mock.assertion.value",
        },
    )
    assert result == {"access_token": "mock-token", "instance_url": "https://mock.my.salesforce.com"}


@patch("auth.requests.post")
def test_fetch_access_token_raises_on_http_error(mock_post):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = Exception("401 unauthorized")
    mock_post.return_value = mock_response

    try:
        fetch_access_token("https://mock.my.salesforce.com/services/oauth2/token", "bad-assertion")
        assert False, "expected an exception"
    except Exception as exc:
        assert "401" in str(exc)
