import os
import time

from flask import Flask, jsonify

from auth import build_assertion, fetch_access_token

app = Flask(__name__)

# Cache the last token in-process rather than signing a fresh JWT and
# round-tripping to Salesforce on every /token call. Found reviewing the
# final branch: every Mule System API call fetches a token, and the
# dashboard alone polls GET /orders every 3s, so an uncached service was
# making roughly 1,200 Salesforce token exchanges/hour just from a browser
# tab left open -- real risk of hitting a Developer Edition org's request
# limits mid-demo. The JWT Bearer token response carries no expires_in, so
# this uses a fixed, conservative TTL well under Salesforce's shortest
# common session-timeout policy rather than trusting a value that isn't
# there.
_TOKEN_TTL_SECONDS = 25 * 60
_cached_token: dict | None = None
_cached_at: float = 0.0


def _load_private_key() -> str:
    with open(os.environ["SFDC_JWT_PRIVATE_KEY_PATH"]) as f:
        return f.read()


def _get_token() -> dict:
    global _cached_token, _cached_at
    if _cached_token is not None and (time.time() - _cached_at) < _TOKEN_TTL_SECONDS:
        return _cached_token
    assertion = build_assertion(
        consumer_key=os.environ["SFDC_CONSUMER_KEY"],
        username=os.environ["SFDC_USERNAME"],
        audience=os.environ.get("SFDC_JWT_AUDIENCE", "https://login.salesforce.com"),
        private_key=_load_private_key(),
    )
    result = fetch_access_token(os.environ["SFDC_TOKEN_URL"], assertion)
    _cached_token = result
    _cached_at = time.time()
    return result


@app.route("/token", methods=["GET"])
def token():
    try:
        result = _get_token()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502
    return jsonify(result), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
