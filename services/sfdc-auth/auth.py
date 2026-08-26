import time

import jwt
import requests


def build_assertion(consumer_key: str, username: str, audience: str, private_key: str) -> str:
    claims = {
        "iss": consumer_key,
        "sub": username,
        "aud": audience,
        "exp": int(time.time()) + 300,
    }
    return jwt.encode(claims, private_key, algorithm="RS256")


def fetch_access_token(token_url: str, assertion: str) -> dict:
    resp = requests.post(
        token_url,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        },
        timeout=10,
    )
    resp.raise_for_status()
    body = resp.json()
    return {"access_token": body["access_token"], "instance_url": body["instance_url"]}
