import os

from flask import Flask, jsonify

from auth import build_assertion, fetch_access_token

app = Flask(__name__)


def _load_private_key() -> str:
    with open(os.environ["SFDC_JWT_PRIVATE_KEY_PATH"]) as f:
        return f.read()


@app.route("/token", methods=["GET"])
def token():
    try:
        assertion = build_assertion(
            consumer_key=os.environ["SFDC_CONSUMER_KEY"],
            username=os.environ["SFDC_USERNAME"],
            audience=os.environ.get("SFDC_JWT_AUDIENCE", "https://login.salesforce.com"),
            private_key=_load_private_key(),
        )
        result = fetch_access_token(os.environ["SFDC_TOKEN_URL"], assertion)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502
    return jsonify(result), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
