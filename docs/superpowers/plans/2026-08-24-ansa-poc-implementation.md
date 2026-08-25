# Ansa Biotechnologies POC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a locally-runnable, Docker-composed DNA synthesis order & fulfillment integration pipeline demonstrating MuleSoft API-led connectivity, real Salesforce integration, and event-driven telemetry ingestion.

**Architecture:** Three Mule API tiers (Experience / Process / System) front a real Salesforce Developer org as system of record. Two Python services — a sequence-feasibility scorer and an instrument-telemetry simulator — talk to Mule over HTTP and to each other over an ActiveMQ broker (Mule via JMS/OpenWire, Python via STOMP, same broker/destinations). A static HTML/JS dashboard polls the Experience API.

**Tech Stack:** Mule 4.x (Community Edition, standalone), Salesforce REST API via `http:request` + OAuth 2.0 JWT Bearer (see note below — not the packaged Salesforce Connector, which is Enterprise-only), Mule JMS Connector, Mule Java module (JWT signing, pure JDK), ActiveMQ, Python 3.11 (Flask, pytest, stomp.py), Salesforce CLI (`sf`), Docker / Docker Compose, vanilla HTML/JS/CSS dashboard (no build step).

**Note (fixed during Task 5 execution):** the plan originally specified
MuleSoft's packaged Salesforce Connector. That connector's own bundled
descriptor requires `MULE_EE` unconditionally (verified by inspecting its
`META-INF/mule-artifact/mule-artifact.json` — not fixable via
`mule-artifact.json` in this project), which needs a licensed Anypoint
account — directly contradicting this plan's Community-Edition/no-Anypoint
constraint. Replaced with plain `http:request` calls to Salesforce's REST
API, authenticated with the same JWT Bearer flow from Task 2. User approved
this pivot. See the ledger's Ruling entry for full detail.

**Spec:** `docs/superpowers/specs/2026-08-24-ansa-poc-design.md`

## Global Constraints

- Target Salesforce org is real: `orgfarm-46688fa9f2-dev-ed.develop.my.salesforce.com` (Developer Edition). Never mock Salesforce.
- Mule runtime is 4.x standalone/Community Edition only. No CloudHub, no Anypoint Platform account dependency to run or review the demo.
- Full stack (Mule, ActiveMQ, both Python services, dashboard) must run via a single `docker-compose up`.
- Only two backend languages: Mule/DataWeave and Python. Dashboard is plain HTML/JS/CSS, no build tooling.
- Credentials are never committed. `.env` is gitignored; `.env.example` (no real values) is committed as the template.
- No idempotency keys / de-dup on order submission — explicitly out of scope per spec.
- No load/performance testing — explicitly out of scope per spec.
- Salesforce write happens **before** the queue publish on order creation (write-before-publish ordering) — never the reverse.
- Telemetry consumer only advances `Status__c` forward along the lifecycle `Submitted → Feasible/Rejected → In_Synthesis → QC → Shipped` (with `At_Risk` as a lateral flag) — never backward.
- Commits in this repo carry no `Co-Authored-By` trailer — author is the user's own identity only.
- **MUnit execution is a known, documented environment limitation in this sandbox (decided during Task 5 — see the ledger's Ruling entry for the full elimination chain).** MUnit's embedded test container deploys the app cleanly with zero errors but silently discovers and runs zero test suites, for reasons that resisted diagnosis even after: independently verifying every connector/module/XML attribute against decompiled bytecode; trying multiple Mule runtime and plugin version combinations; purging and force-refreshing the local Maven cache; and eliminating a cross-filesystem (WSL/Windows-mount) hypothesis by installing a fully native Maven. For every Mule task (5 through 9), the acceptance bar is therefore **`mvn clean package` succeeding** (schema/structural validation plus a successfully built deployable artifact) rather than `mvn test`/a green MUnit run. Each task's MUnit suite should still be written correctly per its TDD steps — it documents real testing intent and would run in a normal Anypoint Studio or properly-provisioned environment — but its pass/fail is not the gate here. Real functional verification happens via Task 12's end-to-end `docker-compose` run against the live Salesforce org. Use the native Maven at `~/tools/apache-maven-3.9.6/bin/mvn` for all Mule build/test commands in this environment, not the system `mvn` (3.6.3, incompatible with `mule-maven-plugin` 4.10.1) or any Windows-mounted install.

---

## File Structure

```
poc-ansa-biotech/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── README.md
├── salesforce/
│   ├── sfdx-project.json
│   └── force-app/main/default/objects/Synthesis_Order__c/
│       ├── Synthesis_Order__c.object-meta.xml
│       └── fields/
│           ├── Account__c.field-meta.xml
│           ├── Sequence__c.field-meta.xml
│           ├── Length_bp__c.field-meta.xml
│           ├── Status__c.field-meta.xml
│           ├── Feasibility_Score__c.field-meta.xml
│           ├── Rejection_Reason__c.field-meta.xml
│           ├── Promised_Ship_Date__c.field-meta.xml
│           ├── Progress_Pct__c.field-meta.xml
│           └── Batch_Id__c.field-meta.xml
├── services/
│   ├── feasibility/
│   │   ├── scoring.py
│   │   ├── app.py
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       ├── test_scoring.py
│   │       └── test_app.py
│   └── instrument-simulator/
│       ├── simulator.py
│       ├── requirements.txt
│       ├── Dockerfile
│       └── tests/
│           └── test_simulator.py
├── mule-app/
│   ├── pom.xml
│   ├── mule-artifact.json
│   ├── docker-entrypoint.sh
│   ├── Dockerfile
│   ├── src/main/mule/
│   │   ├── global-config.xml
│   │   ├── salesforce-system-api.xml
│   │   ├── feasibility-system-api.xml
│   │   ├── process-api.xml
│   │   └── experience-api.xml
│   ├── src/main/java/com/poc/ansa/
│   │   └── JwtSigner.java      (Task 5 — JWT-signing helper, since the packaged
│   │                              Salesforce Connector requires EE; see Task 5)
│   ├── src/main/resources/
│   │   ├── log4j2.xml
│   │   └── keys/              (gitignored: sfdc-jwt.key, sfdc-jwt.crt, sfdc-jwt.p12 — Task 2)
│   └── src/test/munit/
│       ├── salesforce-system-api-test-suite.xml
│       ├── feasibility-system-api-test-suite.xml
│       ├── process-api-order-test-suite.xml
│       ├── process-api-telemetry-test-suite.xml
│       └── experience-api-test-suite.xml
└── dashboard/
    ├── index.html
    ├── app.js
    ├── styles.css
    └── tests/
        └── app.test.js
```

**Responsibilities:**
- `salesforce/` — metadata-as-code for the `Synthesis_Order__c` object, deployed via `sf project deploy`.
- `services/feasibility/` — pure scoring logic (`scoring.py`) separated from the thin Flask HTTP layer (`app.py`), so domain logic is unit-testable without spinning up a server.
- `services/instrument-simulator/` — consumes `synthesis-jobs`, produces `batch-telemetry`, both over STOMP.
- `mule-app/` — one XML file per API tier plus `global-config.xml` for shared connector configs, matching the spec's tier boundaries exactly. One MUnit suite per XML file. All four tiers deploy as a single Mule application (one Maven project, one jar) with each tier bound to its own HTTP listener port — a POC simplification of what would be separate deployables in production. Because of this, calls from the Process API to the System API tiers are still real HTTP calls (never in-JVM shortcuts), just addressed at `localhost:<tier-port>` rather than a different host.
- `dashboard/` — no framework; `app.js` isolates pure rendering/formatting functions from the polling loop so they're unit-testable with Node's built-in test runner.

---

### Task 1: Salesforce object model

**Files:**
- Create: `salesforce/sfdx-project.json`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/Synthesis_Order__c.object-meta.xml`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/fields/Account__c.field-meta.xml`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/fields/Sequence__c.field-meta.xml`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/fields/Length_bp__c.field-meta.xml`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/fields/Status__c.field-meta.xml`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/fields/Feasibility_Score__c.field-meta.xml`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/fields/Rejection_Reason__c.field-meta.xml`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/fields/Promised_Ship_Date__c.field-meta.xml`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/fields/Progress_Pct__c.field-meta.xml`
- Create: `salesforce/force-app/main/default/objects/Synthesis_Order__c/fields/Batch_Id__c.field-meta.xml`

**Interfaces:**
- Produces: Salesforce object `Synthesis_Order__c` with standard `Name` field (autonumber not needed — text, per object metadata below) plus the nine custom fields above. Every later task's Salesforce System API reads/writes this object and these exact API names.

**Note on data model:** the spec's `Order_Name__c` maps onto the object's standard `Name` field — every custom object already has one, so a separate custom field for the same purpose would be redundant. All other fields are created exactly as specified.

- [ ] **Step 1: Write `sfdx-project.json`**

```json
{
  "packageDirectories": [
    { "path": "force-app", "default": true }
  ],
  "name": "poc-ansa-biotech",
  "namespace": "",
  "sfdcLoginUrl": "https://login.salesforce.com",
  "sourceApiVersion": "60.0"
}
```

- [ ] **Step 2: Write the object metadata**

`salesforce/force-app/main/default/objects/Synthesis_Order__c/Synthesis_Order__c.object-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomObject xmlns="http://soap.sforce.com/2006/04/metadata">
    <label>Synthesis Order</label>
    <pluralLabel>Synthesis Orders</pluralLabel>
    <nameField>
        <label>Order Name</label>
        <type>Text</type>
    </nameField>
    <deploymentStatus>Deployed</deploymentStatus>
    <sharingModel>ReadWrite</sharingModel>
    <enableActivities>false</enableActivities>
</CustomObject>
```

- [ ] **Step 3: Write the field metadata files**

`.../fields/Account__c.field-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Account__c</fullName>
    <label>Account</label>
    <type>Lookup</type>
    <referenceTo>Account</referenceTo>
    <relationshipLabel>Synthesis Orders</relationshipLabel>
    <relationshipName>Synthesis_Orders</relationshipName>
    <deleteConstraint>SetNull</deleteConstraint>
</CustomField>
```

`.../fields/Sequence__c.field-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Sequence__c</fullName>
    <label>Sequence</label>
    <type>LongTextArea</type>
    <length>32768</length>
    <visibleLines>5</visibleLines>
</CustomField>
```

`.../fields/Length_bp__c.field-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Length_bp__c</fullName>
    <label>Length (bp)</label>
    <type>Number</type>
    <precision>10</precision>
    <scale>0</scale>
</CustomField>
```

`.../fields/Status__c.field-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Status__c</fullName>
    <label>Status</label>
    <type>Picklist</type>
    <valueSet>
        <restricted>true</restricted>
        <valueSetDefinition>
            <sorted>false</sorted>
            <value><fullName>Submitted</fullName><default>true</default></value>
            <value><fullName>Feasible</fullName><default>false</default></value>
            <value><fullName>Rejected</fullName><default>false</default></value>
            <value><fullName>In_Synthesis</fullName><default>false</default></value>
            <value><fullName>QC</fullName><default>false</default></value>
            <value><fullName>Shipped</fullName><default>false</default></value>
            <value><fullName>At_Risk</fullName><default>false</default></value>
        </valueSetDefinition>
    </valueSet>
</CustomField>
```

`.../fields/Feasibility_Score__c.field-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Feasibility_Score__c</fullName>
    <label>Feasibility Score</label>
    <type>Number</type>
    <precision>3</precision>
    <scale>2</scale>
</CustomField>
```

`.../fields/Rejection_Reason__c.field-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Rejection_Reason__c</fullName>
    <label>Rejection Reason</label>
    <type>LongTextArea</type>
    <length>4096</length>
    <visibleLines>3</visibleLines>
</CustomField>
```

`.../fields/Promised_Ship_Date__c.field-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Promised_Ship_Date__c</fullName>
    <label>Promised Ship Date</label>
    <type>Date</type>
</CustomField>
```

`.../fields/Progress_Pct__c.field-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Progress_Pct__c</fullName>
    <label>Progress %</label>
    <type>Number</type>
    <precision>5</precision>
    <scale>2</scale>
</CustomField>
```

`.../fields/Batch_Id__c.field-meta.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Batch_Id__c</fullName>
    <label>Batch Id</label>
    <type>Text</type>
    <length>255</length>
    <externalId>true</externalId>
    <unique>true</unique>
    <caseSensitive>false</caseSensitive>
</CustomField>
```

- [ ] **Step 4: Authenticate the Salesforce CLI against the dev org**

Run (interactive, one-time, from repo root):

```bash
sf --version
sf org login web --alias ansa-poc-dev --instance-url https://orgfarm-46688fa9f2-dev-ed.develop.my.salesforce.com
```

This opens a browser for login; confirm it completes and `sf org display --target-org ansa-poc-dev` shows the org.

- [ ] **Step 5: Deploy the metadata**

```bash
cd salesforce
sf project deploy start --target-org ansa-poc-dev
```

Expected: deploy succeeds, output lists `Synthesis_Order__c` and its 9 fields as `Created`.

- [ ] **Step 6: Verify the object and fields exist**

```bash
sf sobject describe --sobject Synthesis_Order__c --target-org ansa-poc-dev --json | grep -E '"name" *: *"(Name|Account__c|Sequence__c|Length_bp__c|Status__c|Feasibility_Score__c|Rejection_Reason__c|Promised_Ship_Date__c|Progress_Pct__c|Batch_Id__c)"'
```

Expected: all 10 field names print.

- [ ] **Step 7: Commit**

```bash
git add salesforce/
git commit -m "Add Synthesis_Order__c Salesforce object metadata"
```

---

### Task 2: Salesforce External Client App + JWT Bearer auth

**Revised during execution.** The plan originally specified the OAuth 2.0
Username-Password flow. That flow (and the classic SOAP `login()` API) are
both **blocked by default** on this org — Salesforce disables both for any
org created Summer '23 or later, and the toggle to re-enable
Username-Password is not editable even by an org admin on this org. This
was discovered empirically while executing this task (see the ledger's
Ruling entry). The plan now uses the **OAuth 2.0 JWT Bearer flow** instead:
Mule signs a JWT with a private key; Salesforce verifies it against a
certificate uploaded to the app. No password crosses the wire, and this
flow is not affected by the Username-Password block.

**Files:**
- Create: `.gitignore`
- Create: `.env.example`
- Create (gitignored, not committed): `mule-app/src/main/resources/keys/sfdc-jwt.key`, `sfdc-jwt.crt`, `sfdc-jwt.p12`

**Interfaces:**
- Produces: `.env` (untracked) supplying `SFDC_CONSUMER_KEY`, `SFDC_USERNAME`, `SFDC_TOKEN_URL`, `SFDC_JWT_AUDIENCE`, `SFDC_JWT_KEYSTORE_PATH`, `SFDC_JWT_KEYSTORE_PASSWORD`, `SFDC_JWT_KEY_ALIAS` — consumed by `global-config.xml` in Task 5. Also produces the PKCS12 keystore file that same config references.

- [ ] **Step 1: Create the External Client App in Salesforce Setup (manual, one-time)**

In the dev org (Setup → App Manager → **New External Client App** — not "New Lightning App"):
1. Name: `Ansa POC Mule Integration`, any contact email.
2. Enable OAuth Settings, Callback URL: `http://localhost:8081/callback` (required to save, unused by JWT Bearer).
3. OAuth Scopes: add "Manage user data via APIs (api)" and "Perform requests at any time (refresh_token, offline_access)".
4. Save, wait for propagation (10+ minutes — this app type propagates slower than classic Connected Apps).
5. Under **Flow Enablement**, check **"Enable JWT Bearer Flow"** — this reveals a Certificate Upload control (it's hidden until this box is checked, unlike the classic Connected App UI).
6. Open the app → **Policies** tab → note the Consumer Key shown there (Consumer Secret is also shown but unused by JWT Bearer — no need to record it).

- [ ] **Step 2: Generate a self-signed keypair and package it as a keystore**

```bash
mkdir -p mule-app/src/main/resources/keys
openssl req -x509 -sha256 -nodes -days 3650 -newkey rsa:2048 \
  -keyout mule-app/src/main/resources/keys/sfdc-jwt.key \
  -out mule-app/src/main/resources/keys/sfdc-jwt.crt \
  -subj "/CN=ansa-poc-mule-integration/O=Ansa POC/C=US"
openssl pkcs12 -export \
  -in mule-app/src/main/resources/keys/sfdc-jwt.crt \
  -inkey mule-app/src/main/resources/keys/sfdc-jwt.key \
  -out mule-app/src/main/resources/keys/sfdc-jwt.p12 \
  -name sfdc-jwt -passout pass:changeit
```

None of these three files are committed — see Step 4's `.gitignore`.

- [ ] **Step 3: Upload the certificate and pre-authorize the app**

1. On the External Client App's Flow Enablement section (from Step 1.5), upload `sfdc-jwt.crt` under Certificate Upload. Confirm the displayed subject matches `C=US, O=Ansa POC, CN=ansa-poc-mule-integration`.
2. On the **Policies** tab, change "Permitted Users" (App Authorization) to **"Admin approved users are pre-authorized"** — JWT Bearer has no browser consent screen, so pre-authorization replaces it.
3. Create a Permission Set (Setup → Permission Sets → New; any label, License = `--None--`).
4. Back on the External Client App's Policies tab, a "Select Permission Sets" control appears once Permitted Users is set to pre-authorized — add the new Permission Set to "Selected Permission Sets" and save.
5. On the Permission Set itself → Manage Assignments → Add Assignment → assign it to your own user.

- [ ] **Step 4: Write `.gitignore`**

```
.env
mule-app/src/main/resources/keys/
*.log
target/
__pycache__/
*.pyc
.pytest_cache/
node_modules/
salesforce/.sf/
```

- [ ] **Step 5: Write `.env.example` (committed, no real values)**

```bash
# Salesforce (OAuth 2.0 JWT Bearer flow via External Client App).
# Username-password and SOAP login() are both blocked by default on this org
# (Salesforce policy for orgs created Summer '23+), so auth uses a signed JWT
# instead of a password. See Task 2 in the implementation plan for how the
# keystore at mule-app/src/main/resources/keys/sfdc-jwt.p12 was generated and
# how the app's Connected App / Permission Set were configured to allow it.
SFDC_CONSUMER_KEY=
SFDC_USERNAME=
SFDC_TOKEN_URL=https://orgfarm-46688fa9f2-dev-ed.develop.my.salesforce.com/services/oauth2/token
SFDC_INSTANCE_HOST=orgfarm-46688fa9f2-dev-ed.develop.my.salesforce.com
SFDC_JWT_AUDIENCE=https://login.salesforce.com
SFDC_JWT_KEYSTORE_PATH=keys/sfdc-jwt.p12
SFDC_JWT_KEYSTORE_PASSWORD=changeit
SFDC_JWT_KEY_ALIAS=sfdc-jwt

# Mule — SF/Feasibility-System/Process API hosts are "localhost" because
# all four tiers run as one Mule app (see mule-app/ note in the plan's File
# Structure section); each tier is still a real HTTP call to its own port.
HTTP_PORT=8081
FEASIBILITY_HOST=feasibility
FEASIBILITY_PORT=5001
SF_SYSTEM_API_HOST=localhost
SF_SYSTEM_API_PORT=8082
FEASIBILITY_SYSTEM_API_HOST=localhost
FEASIBILITY_SYSTEM_API_PORT=8083
PROCESS_API_HOST=localhost
PROCESS_API_PORT=8084
EXPERIENCE_API_HOST=localhost
EXPERIENCE_API_PORT=8081

# ActiveMQ
ACTIVEMQ_BROKER_URL=tcp://activemq:61616
ACTIVEMQ_STOMP_HOST=activemq
ACTIVEMQ_STOMP_PORT=61613
```

- [ ] **Step 6: Create your local `.env` from the template**

```bash
cp .env.example .env
```

Fill in `SFDC_CONSUMER_KEY` and `SFDC_USERNAME` (from Step 1.6); `SFDC_JWT_KEYSTORE_PATH` should be the path to Step 2's `.p12` file relative to wherever the verification script or Mule app runs from. This file stays local, never committed.

- [ ] **Step 7: Verify the JWT Bearer credentials work**

```bash
pip install --quiet pyjwt cryptography
python3 - <<'EOF'
import time, urllib.request, urllib.parse, json, jwt

env = {}
with open(".env") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k] = v

with open(env["SFDC_JWT_KEYSTORE_PATH"].replace(".p12", ".key")) as f:
    private_key = f.read()

claims = {
    "iss": env["SFDC_CONSUMER_KEY"],
    "sub": env["SFDC_USERNAME"],
    "aud": env["SFDC_JWT_AUDIENCE"],
    "exp": int(time.time()) + 300,
}
assertion = jwt.encode(claims, private_key, algorithm="RS256")
data = urllib.parse.urlencode({
    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
    "assertion": assertion,
}).encode()
req = urllib.request.Request(env["SFDC_TOKEN_URL"], data=data, method="POST")
try:
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read())
        print("HTTP", resp.status, "access_token present:", "access_token" in body)
except urllib.error.HTTPError as e:
    print("HTTP", e.code, json.loads(e.read()))
EOF
```

Expected: `HTTP 200 access_token present: True`. If you get `invalid_grant: user hasn't approved this consumer`, the Permission Set from Step 3 isn't assigned yet or hasn't propagated. If you get an assertion/signature error, double check the uploaded certificate matches the keypair generated in Step 2.

- [ ] **Step 8: Commit**

```bash
git add .gitignore .env.example
git commit -m "Add JWT Bearer auth setup for Salesforce (Username-Password flow is blocked on this org)"
```

The keystore files (`.key`/`.crt`/`.p12`) are never committed — anyone re-running this plan regenerates their own via Step 2.

---

### Task 3: Feasibility scoring service (Python)

**Files:**
- Create: `services/feasibility/scoring.py`
- Create: `services/feasibility/app.py`
- Create: `services/feasibility/requirements.txt`
- Create: `services/feasibility/Dockerfile`
- Test: `services/feasibility/tests/test_scoring.py`
- Test: `services/feasibility/tests/test_app.py`

**Interfaces:**
- Produces: `score_sequence(sequence: str) -> dict` in `scoring.py`, returning `{"score": float, "feasible": bool, "reasons": list[str], "flags": {"gc_content": float, "max_homopolymer_run": int, "repeat_regions": list[dict]}}`. Consumed directly by `app.py`'s `POST /feasibility`, and indirectly by the Mule Feasibility System API (Task 6) which calls this HTTP endpoint.

- [ ] **Step 1: Write the failing scoring tests**

`services/feasibility/tests/test_scoring.py`:

```python
from scoring import score_sequence


def test_balanced_short_sequence_is_feasible():
    result = score_sequence("ACGTACGTACGTACGTACGT")
    assert result["feasible"] is True
    assert result["score"] > 0.7
    assert result["reasons"] == []


def test_extremely_gc_rich_sequence_is_flagged():
    result = score_sequence("GCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGC")
    assert result["flags"]["gc_content"] > 0.9
    assert "high GC content" in " ".join(result["reasons"]).lower()
    assert result["feasible"] is False


def test_long_homopolymer_run_is_flagged():
    result = score_sequence("ACGT" + "A" * 15 + "ACGT")
    assert result["flags"]["max_homopolymer_run"] == 15
    assert "homopolymer" in " ".join(result["reasons"]).lower()
    assert result["feasible"] is False


def test_repeat_region_is_detected():
    result = score_sequence("ACGTACGT" + "CAGCAG" * 6 + "ACGTACGT")
    assert len(result["flags"]["repeat_regions"]) >= 1
    assert "repeat" in " ".join(result["reasons"]).lower()
    assert result["feasible"] is False


def test_empty_sequence_raises():
    import pytest
    with pytest.raises(ValueError):
        score_sequence("")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd services/feasibility
pip install -r requirements.txt 2>/dev/null || pip install pytest flask
pytest tests/test_scoring.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'scoring'` (file doesn't exist yet).

- [ ] **Step 3: Write `scoring.py`**

```python
import re

GC_RICH_THRESHOLD = 0.65
MAX_HOMOPOLYMER_THRESHOLD = 10
MIN_REPEAT_UNIT = 2
MAX_REPEAT_UNIT = 6
MIN_REPEAT_COUNT = 4


def _gc_content(sequence: str) -> float:
    gc = sum(1 for base in sequence if base in "GC")
    return gc / len(sequence)


def _max_homopolymer_run(sequence: str) -> int:
    longest = 1
    current = 1
    for i in range(1, len(sequence)):
        if sequence[i] == sequence[i - 1]:
            current += 1
            longest = max(longest, current)
        else:
            current = 1
    return longest if sequence else 0


def _repeat_regions(sequence: str) -> list[dict]:
    regions = []
    for unit_len in range(MIN_REPEAT_UNIT, MAX_REPEAT_UNIT + 1):
        pattern = re.compile(r"(.{%d})\1{%d,}" % (unit_len, MIN_REPEAT_COUNT - 1))
        for match in pattern.finditer(sequence):
            regions.append({
                "start": match.start(),
                "end": match.end(),
                "unit": match.group(1),
            })
    return regions


def score_sequence(sequence: str) -> dict:
    if not sequence:
        raise ValueError("sequence must not be empty")

    gc_content = _gc_content(sequence)
    max_run = _max_homopolymer_run(sequence)
    repeats = _repeat_regions(sequence)

    reasons = []
    penalty = 0.0

    if gc_content > GC_RICH_THRESHOLD:
        reasons.append(f"High GC content ({gc_content:.0%}) increases synthesis difficulty")
        penalty += (gc_content - GC_RICH_THRESHOLD) * 2

    if max_run > MAX_HOMOPOLYMER_THRESHOLD:
        reasons.append(f"Homopolymer run of {max_run} bases exceeds safe threshold")
        penalty += 0.3

    if repeats:
        reasons.append(f"{len(repeats)} repeat region(s) detected")
        penalty += 0.1 * len(repeats)

    score = max(0.0, min(1.0, 1.0 - penalty))
    feasible = len(reasons) == 0

    return {
        "score": round(score, 2),
        "feasible": feasible,
        "reasons": reasons,
        "flags": {
            "gc_content": round(gc_content, 2),
            "max_homopolymer_run": max_run,
            "repeat_regions": repeats,
        },
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_scoring.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Write the failing HTTP layer test**

`services/feasibility/tests/test_app.py`:

```python
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
```

- [ ] **Step 6: Run to verify it fails**

```bash
pytest tests/test_app.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'app'`.

- [ ] **Step 7: Write `app.py`**

```python
from flask import Flask, request, jsonify
from scoring import score_sequence

app = Flask(__name__)


@app.route("/feasibility", methods=["POST"])
def feasibility():
    payload = request.get_json(silent=True) or {}
    sequence = payload.get("sequence", "")
    if not sequence:
        return jsonify({"error": "sequence is required"}), 400
    try:
        result = score_sequence(sequence)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(result), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
```

- [ ] **Step 8: Run to verify it passes**

```bash
pytest tests/test_app.py -v
```

Expected: 2 passed.

- [ ] **Step 9: Write `requirements.txt` and `Dockerfile`**

`services/feasibility/requirements.txt`:

```
flask==3.0.3
pytest==8.2.0
```

`services/feasibility/Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scoring.py app.py ./
EXPOSE 5001
CMD ["python", "app.py"]
```

- [ ] **Step 10: Commit**

```bash
git add services/feasibility/
git commit -m "Add DNA sequence feasibility scoring service"
```

---

### Task 4: ActiveMQ in Docker Compose

**Files:**
- Create: `docker-compose.yml` (initial version — extended in later tasks)

**Interfaces:**
- Produces: an ActiveMQ broker reachable at `activemq:61616` (OpenWire/JMS, for Mule) and `activemq:61613` (STOMP, for Python) within the compose network, `activemq:8161` (web console) exposed to the host for debugging.

- [ ] **Step 1: Write the initial `docker-compose.yml`**

```yaml
services:
  activemq:
    image: apache/activemq-artemis:2.37.0
    environment:
      ARTEMIS_USER: admin
      ARTEMIS_PASSWORD: admin
    ports:
      - "8161:8161"
      - "61616:61616"
      - "61613:61613"
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8161 || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 10
```

- [ ] **Step 2: Bring it up and verify the broker is healthy**

```bash
docker compose up -d activemq
sleep 5
docker compose ps
```

Expected: `activemq` service shows `healthy`.

- [ ] **Step 3: Smoke-test STOMP connectivity**

```bash
pip install stomp.py --quiet
python3 - <<'EOF'
import stomp, time

class Listener(stomp.ConnectionListener):
    def on_message(self, frame):
        print("received:", frame.body)

conn = stomp.Connection([("localhost", 61613)])
conn.set_listener("", Listener())
conn.connect("admin", "admin", wait=True)
conn.subscribe(destination="/queue/smoke-test", id=1, ack="auto")
conn.send(body="hello", destination="/queue/smoke-test")
time.sleep(1)
conn.disconnect()
EOF
```

Expected: prints `received: hello`.

- [ ] **Step 4: Tear down and commit**

```bash
docker compose down
git add docker-compose.yml
git commit -m "Add ActiveMQ Artemis to docker-compose"
```

---

### Task 5: Mule project scaffold + Salesforce System API

**Files:**
- Create: `mule-app/pom.xml`
- Create: `mule-app/mule-artifact.json`
- Create: `mule-app/src/main/resources/log4j2.xml`
- Create: `mule-app/src/main/mule/global-config.xml`
- Create: `mule-app/src/main/mule/salesforce-system-api.xml`
- Create: `mule-app/src/main/java/com/poc/ansa/JwtSigner.java` (JWT-signing helper, added when the packaged Salesforce Connector turned out to require EE — see below)
- Modify: `.env.example`, `.env` (add `SFDC_INSTANCE_HOST`, needed by the REST-based Salesforce config)
- Test: `mule-app/src/test/munit/salesforce-system-api-test-suite.xml`

**Interfaces:**
- Produces: Salesforce System API on port `${SF_SYSTEM_API_PORT}` (default 8082):
  - `POST /synthesis-orders` — body `{name, accountId, sequence, lengthBp, status, feasibilityScore, rejectionReason, promisedShipDate, batchId}` → creates a `Synthesis_Order__c`, returns `{id}`.
  - `PATCH /synthesis-orders/{batchId}` — body is a partial set of the same fields → updates the record matched by `Batch_Id__c`.
  - `GET /synthesis-orders` — returns all orders as JSON array `[{id, name, status, feasibilityScore, progressPct, batchId, promisedShipDate}, ...]`.
  - `GET /synthesis-orders/{batchId}` — returns one order by `Batch_Id__c`, 404 if not found.
  - Consumed by the Process API (Task 7) and Experience API (Task 9) via HTTP.

- [ ] **Step 1: Write `mule-artifact.json`**

```json
{
  "minMuleVersion": "4.6.0",
  "name": "poc-ansa-biotech",
  "requiredProduct": "MULE",
  "classLoaderModelLoaderDescriptor": {
    "id": "mule",
    "attributes": { "exportedPackages": [], "exportedResources": [] }
  }
}
```

**Note (fixed during Task 5 execution):** originally specified `MULE_EE`, which
forces Maven to pull a licensed Enterprise runtime to execute MUnit
tests — requiring Anypoint EE credentials this project doesn't have and
directly contradicting this plan's own Global Constraint (Community
Edition, no Anypoint account required). `MULE` (Community Edition) is
correct here; see the ledger's Ruling entry for Task 5.

- [ ] **Step 2: Write `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.poc.ansa</groupId>
    <artifactId>poc-ansa-biotech</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>mule-application</packaging>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <app.runtime>4.6.0</app.runtime>
        <munit.version>3.3.0</munit.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.mule.connectors</groupId>
            <artifactId>mule-http-connector</artifactId>
            <version>1.9.0</version>
            <classifier>mule-plugin</classifier>
        </dependency>
        <dependency>
            <groupId>org.mule.connectors</groupId>
            <artifactId>mule-jms-connector</artifactId>
            <version>1.9.2</version>
            <classifier>mule-plugin</classifier>
        </dependency>
        <dependency>
            <groupId>org.apache.activemq</groupId>
            <artifactId>artemis-jms-client-all</artifactId>
            <version>2.37.0</version>
        </dependency>
        <dependency>
            <groupId>org.mule.modules</groupId>
            <artifactId>mule-java-module</artifactId>
            <version>1.2.19</version>
            <classifier>mule-plugin</classifier>
        </dependency>
        <dependency>
            <groupId>com.mulesoft.munit</groupId>
            <artifactId>munit-runner</artifactId>
            <version>${munit.version}</version>
            <classifier>mule-plugin</classifier>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.mulesoft.munit</groupId>
            <artifactId>munit-tools</artifactId>
            <version>${munit.version}</version>
            <classifier>mule-plugin</classifier>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.mule.tools.maven</groupId>
                <artifactId>mule-maven-plugin</artifactId>
                <version>4.2.0</version>
                <extensions>true</extensions>
            </plugin>
            <plugin>
                <groupId>com.mulesoft.munit.tools</groupId>
                <artifactId>munit-maven-plugin</artifactId>
                <version>${munit.version}</version>
                <executions>
                    <execution>
                        <id>test</id>
                        <phase>test</phase>
                        <goals>
                            <goal>test</goal>
                            <goal>coverage-report</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

    <repositories>
        <repository>
            <id>anypoint-exchange-v3</id>
            <name>Anypoint Exchange</name>
            <url>https://maven.anypoint.mulesoft.com/api/v3/maven</url>
        </repository>
        <repository>
            <id>mulesoft-releases</id>
            <name>MuleSoft Releases Repository</name>
            <url>https://repository.mulesoft.org/releases/</url>
        </repository>
    </repositories>
    <pluginRepositories>
        <pluginRepository>
            <id>mulesoft-releases</id>
            <name>MuleSoft Releases Repository</name>
            <url>https://repository.mulesoft.org/releases/</url>
        </pluginRepository>
    </pluginRepositories>
</project>
```

**Note (fixed during Task 5 execution):** `mulesoft-releases` was only
listed under `<pluginRepositories>`, which governs Maven *plugin*
resolution only. The embedded Mule Community Edition runtime BOM
(`com.mulesoft.mule.distributions:mule-runtime-impl-no-services-bom`),
needed by MUnit to actually spin up a container to run tests against, is a
regular dependency-graph artifact, resolved via `<repositories>` — and it
isn't published to Maven Central, so without this entry `mvn test` fails
trying (and failing) to find it there. Added `mulesoft-releases` to both
sections above.

**Note:** connector version numbers above are best-known-good as of this plan's writing; if `mvn` dependency resolution fails, run `mvn versions:display-dependency-updates` and bump to the latest available in Anypoint Exchange — this is exactly what Step 5's build/verify step is for.

**Note (fixed during Task 5 execution):** the original pom.xml was missing
the `munit-maven-plugin` execution binding entirely — without it, `mvn test`
silently runs zero MUnit tests instead of failing loudly, which would have
masked real problems. Added above. The Salesforce connector dependency is
gone (see the Tech Stack note); `mule-java-module` is added instead, needed
for the JWT-signing `java:invoke` call in the rewritten
`salesforce-system-api.xml`.

- [ ] **Step 3: Write minimal `log4j2.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration>
    <Appenders>
        <Console name="Console" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
    </Appenders>
    <Loggers>
        <Root level="INFO">
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
</Configuration>
```

- [ ] **Step 4: Write `global-config.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:jms="http://www.mulesoft.org/schema/mule/jms"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/jms http://www.mulesoft.org/schema/mule/jms/current/mule-jms.xsd">

    <http:listener-config name="SF_System_API_Listener_Config">
        <http:listener-connection host="0.0.0.0" port="${sf.system.api.port}" />
    </http:listener-config>

    <http:listener-config name="Feasibility_System_API_Listener_Config">
        <http:listener-connection host="0.0.0.0" port="${feasibility.system.api.port}" />
    </http:listener-config>

    <http:listener-config name="Process_API_Listener_Config">
        <http:listener-connection host="0.0.0.0" port="${process.api.port}" />
    </http:listener-config>

    <http:listener-config name="Experience_API_Listener_Config">
        <http:listener-connection host="0.0.0.0" port="${experience.api.port}" />
    </http:listener-config>

    <http:request-config name="Feasibility_Service_Request_Config">
        <http:request-connection host="${feasibility.host}" port="${feasibility.port}" />
    </http:request-config>

    <http:request-config name="SF_System_API_Request_Config">
        <http:request-connection host="${sf.system.api.host}" port="${sf.system.api.port}" />
    </http:request-config>

    <http:request-config name="Feasibility_System_API_Request_Config">
        <http:request-connection host="${feasibility.system.api.host}" port="${feasibility.system.api.port}" />
    </http:request-config>

    <!--
      Salesforce REST API, not the packaged Salesforce Connector (that
      connector requires MULE_EE unconditionally per its own bundled
      descriptor, incompatible with this project's Community Edition/
      no-Anypoint-account constraint — discovered and fixed while executing
      Task 5). Auth is OAuth 2.0 JWT Bearer, same flow verified working in
      Task 2, but the token exchange now happens via a plain http:request
      inside salesforce-system-api.xml's get-sfdc-access-token-flow rather
      than a connector-managed connection. One shared request-config
      serves both the token endpoint and the data REST endpoints since
      they're the same host (the org's My Domain instance URL).
    -->
    <http:request-config name="SFDC_Request_Config">
        <http:request-connection host="${sfdc.instance.host}" port="443" protocol="HTTPS" />
    </http:request-config>

    <jms:config name="JMS_Config">
        <jms:active-mq-connection>
            <jms:factory-configuration brokerUrl="${activemq.broker.url}" />
        </jms:active-mq-connection>
    </jms:config>

</mule>
```

- [ ] **Step 5: Write `JwtSigner.java` and the rewritten `salesforce-system-api.xml`**

**Revised during execution — see the Tech Stack note above.** This step
originally used `salesforce:create`/`query`/`update` operations against
`Salesforce_Config`. It now uses plain `http:request` calls to Salesforce's
REST API, with a small Java helper doing the JWT-signing math (pure JDK —
`java.security.Signature` with `SHA256withRSA` — no third-party JWT
library needed) since Mule Community Edition has no built-in RS256 signing.

`mule-app/src/main/java/com/poc/ansa/JwtSigner.java`:

```java
package com.poc.ansa;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Base64;

public class JwtSigner {

    public static String sign(String consumerKey, String username, String audience,
                               String keystorePath, String keystorePassword, String keyAlias) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream(keystorePath)) {
            keyStore.load(fis, keystorePassword.toCharArray());
        }
        PrivateKey privateKey = (PrivateKey) keyStore.getKey(keyAlias, keystorePassword.toCharArray());

        long exp = (System.currentTimeMillis() / 1000L) + 300L;
        String header = "{\"alg\":\"RS256\"}";
        String claims = String.format(
            "{\"iss\":\"%s\",\"sub\":\"%s\",\"aud\":\"%s\",\"exp\":%d}",
            consumerKey, username, audience, exp);

        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String headerEncoded = encoder.encodeToString(header.getBytes(StandardCharsets.UTF_8));
        String claimsEncoded = encoder.encodeToString(claims.getBytes(StandardCharsets.UTF_8));
        String signingInput = headerEncoded + "." + claimsEncoded;

        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(signingInput.getBytes(StandardCharsets.UTF_8));
        byte[] signed = signature.sign();
        String signatureEncoded = encoder.encodeToString(signed);

        return signingInput + "." + signatureEncoded;
    }
}
```

This mirrors the already-verified-working logic from Task 2's Python
verification script (same claims shape, same RS256 signing over the same
keystore) — if this Java version and the Python version ever disagree,
trust the Python one (it's the one independently verified against the
real org in Task 2) and fix the Java to match.

`mule-app/src/main/mule/salesforce-system-api.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:java="http://www.mulesoft.org/schema/mule/java"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/java http://www.mulesoft.org/schema/mule/java/current/mule-java.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd">

    <sub-flow name="get-sfdc-access-token-flow">
        <java:invoke class="com.poc.ansa.JwtSigner" method="sign(String, String, String, String, String, String)">
            <java:args><![CDATA[#[{
                arg0: p('sfdc.consumer.key'),
                arg1: p('sfdc.username'),
                arg2: p('sfdc.jwt.audience'),
                arg3: p('sfdc.jwt.keystore.path'),
                arg4: p('sfdc.jwt.keystore.password'),
                arg5: p('sfdc.jwt.key.alias')
            }]]]></java:args>
        </java:invoke>
        <set-variable variableName="jwtAssertion" value="#[payload]"/>
        <http:request config-ref="SFDC_Request_Config" method="POST" path="/services/oauth2/token">
            <http:body><![CDATA[#[output application/x-www-form-urlencoded
---
{
    grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
    assertion: vars.jwtAssertion
}]]]></http:body>
        </http:request>
    </sub-flow>

    <flow name="sf-create-order-flow">
        <http:listener config-ref="SF_System_API_Listener_Config" path="/synthesis-orders" allowedMethods="POST"/>
        <ee:transform>
            <ee:message>
                <ee:set-payload><![CDATA[%dw 2.0
output application/json
---
{
    Name: payload.name,
    Account__c: payload.accountId,
    Sequence__c: payload.sequence,
    Length_bp__c: payload.lengthBp,
    Status__c: payload.status,
    Feasibility_Score__c: payload.feasibilityScore default null,
    Rejection_Reason__c: payload.rejectionReason default null,
    Promised_Ship_Date__c: payload.promisedShipDate default null,
    Batch_Id__c: payload.batchId
}]]></ee:set-payload>
            </ee:message>
        </ee:transform>
        <set-variable variableName="recordPayload" value="#[payload]"/>

        <flow-ref name="get-sfdc-access-token-flow"/>
        <set-variable variableName="accessToken" value="#[payload.access_token]"/>

        <http:request config-ref="SFDC_Request_Config" method="POST" path="/services/data/v60.0/sobjects/Synthesis_Order__c">
            <http:headers><![CDATA[#[{ 'Authorization': 'Bearer ' ++ vars.accessToken, 'Content-Type': 'application/json' }]]]></http:headers>
            <http:body><![CDATA[#[output application/json --- vars.recordPayload]]]></http:body>
        </http:request>

        <ee:transform>
            <ee:message>
                <ee:set-payload><![CDATA[%dw 2.0
output application/json
---
{ id: payload.id }]]></ee:set-payload>
            </ee:message>
        </ee:transform>
    </flow>

    <flow name="sf-update-order-flow">
        <http:listener config-ref="SF_System_API_Listener_Config" path="/synthesis-orders/{batchId}" allowedMethods="PATCH"/>
        <set-variable variableName="requestBody" value="#[payload]"/>

        <flow-ref name="get-sfdc-access-token-flow"/>
        <set-variable variableName="accessToken" value="#[payload.access_token]"/>

        <ee:transform>
            <ee:variables>
                <ee:set-variable variableName="soql"><![CDATA[%dw 2.0
output text/plain
---
"SELECT Id FROM Synthesis_Order__c WHERE Batch_Id__c = '" ++ attributes.uriParams.batchId ++ "'"]]></ee:set-variable>
            </ee:variables>
        </ee:transform>

        <http:request config-ref="SFDC_Request_Config" method="GET" path="/services/data/v60.0/query">
            <http:headers><![CDATA[#[{ 'Authorization': 'Bearer ' ++ vars.accessToken }]]]></http:headers>
            <http:query-params><![CDATA[#[{ q: vars.soql }]]]></http:query-params>
        </http:request>

        <choice>
            <when expression="#[sizeOf(payload.records) == 0]">
                <set-variable variableName="httpStatus" value="404"/>
                <set-payload value='#[output application/json --- { error: "order not found for batchId " ++ attributes.uriParams.batchId }]'/>
            </when>
            <otherwise>
                <set-variable variableName="recordId" value="#[payload.records[0].Id]"/>
                <ee:transform>
                    <ee:message>
                        <ee:set-payload><![CDATA[%dw 2.0
output application/json
var body = vars.requestBody
---
{
    (Status__c: body.status) if (body.status?),
    (Progress_Pct__c: body.progressPct) if (body.progressPct?),
    (Feasibility_Score__c: body.feasibilityScore) if (body.feasibilityScore?),
    (Rejection_Reason__c: body.rejectionReason) if (body.rejectionReason?)
}]]></ee:set-payload>
                    </ee:message>
                </ee:transform>
                <http:request config-ref="SFDC_Request_Config" method="PATCH" path="#['/services/data/v60.0/sobjects/Synthesis_Order__c/' ++ vars.recordId]">
                    <http:headers><![CDATA[#[{ 'Authorization': 'Bearer ' ++ vars.accessToken, 'Content-Type': 'application/json' }]]]></http:headers>
                    <http:body><![CDATA[#[payload]]]></http:body>
                </http:request>
                <!-- Salesforce REST PATCH returns 204 No Content on success — don't
                     try to read a body from it, just overwrite payload directly. -->
                <set-payload value='#[output application/json --- { updated: true }]'/>
            </otherwise>
        </choice>
    </flow>

    <flow name="sf-get-orders-flow">
        <http:listener config-ref="SF_System_API_Listener_Config" path="/synthesis-orders" allowedMethods="GET"/>

        <flow-ref name="get-sfdc-access-token-flow"/>
        <set-variable variableName="accessToken" value="#[payload.access_token]"/>

        <ee:transform>
            <ee:variables>
                <ee:set-variable variableName="soql"><![CDATA[%dw 2.0
output text/plain
---
"SELECT Id, Name, Status__c, Feasibility_Score__c, Progress_Pct__c, Batch_Id__c, Promised_Ship_Date__c FROM Synthesis_Order__c ORDER BY CreatedDate DESC"]]></ee:set-variable>
            </ee:variables>
        </ee:transform>

        <http:request config-ref="SFDC_Request_Config" method="GET" path="/services/data/v60.0/query">
            <http:headers><![CDATA[#[{ 'Authorization': 'Bearer ' ++ vars.accessToken }]]]></http:headers>
            <http:query-params><![CDATA[#[{ q: vars.soql }]]]></http:query-params>
        </http:request>

        <ee:transform>
            <ee:message>
                <ee:set-payload><![CDATA[%dw 2.0
output application/json
---
payload.records map (order) -> {
    id: order.Id,
    name: order.Name,
    status: order.Status__c,
    feasibilityScore: order.Feasibility_Score__c,
    progressPct: order.Progress_Pct__c,
    batchId: order.Batch_Id__c,
    promisedShipDate: order.Promised_Ship_Date__c
}]]></ee:set-payload>
            </ee:message>
        </ee:transform>
    </flow>

    <flow name="sf-get-order-by-batch-flow">
        <http:listener config-ref="SF_System_API_Listener_Config" path="/synthesis-orders/{batchId}" allowedMethods="GET"/>

        <flow-ref name="get-sfdc-access-token-flow"/>
        <set-variable variableName="accessToken" value="#[payload.access_token]"/>

        <ee:transform>
            <ee:variables>
                <ee:set-variable variableName="soql"><![CDATA[%dw 2.0
output text/plain
---
"SELECT Id, Name, Status__c, Feasibility_Score__c, Progress_Pct__c, Batch_Id__c, Promised_Ship_Date__c FROM Synthesis_Order__c WHERE Batch_Id__c = '" ++ attributes.uriParams.batchId ++ "'"]]></ee:set-variable>
            </ee:variables>
        </ee:transform>

        <http:request config-ref="SFDC_Request_Config" method="GET" path="/services/data/v60.0/query">
            <http:headers><![CDATA[#[{ 'Authorization': 'Bearer ' ++ vars.accessToken }]]]></http:headers>
            <http:query-params><![CDATA[#[{ q: vars.soql }]]]></http:query-params>
        </http:request>

        <choice>
            <when expression="#[sizeOf(payload.records) == 0]">
                <set-variable variableName="httpStatus" value="404"/>
                <set-payload value='#[output application/json --- { error: "not found" }]'/>
            </when>
            <otherwise>
                <ee:transform>
                    <ee:message>
                        <ee:set-payload><![CDATA[%dw 2.0
output application/json
var order = payload.records[0]
---
{
    id: order.Id,
    name: order.Name,
    status: order.Status__c,
    feasibilityScore: order.Feasibility_Score__c,
    progressPct: order.Progress_Pct__c,
    batchId: order.Batch_Id__c,
    promisedShipDate: order.Promised_Ship_Date__c
}]]></ee:set-payload>
                    </ee:message>
                </ee:transform>
            </otherwise>
        </choice>
    </flow>

</mule>
```

The exact `java:invoke` XML shape (method signature string format,
`java:args` structure) should be verified against the installed
`mule-java-module` version's actual schema once built — same
verify-against-installed-version caveat as the pom.xml note. Getting the
MUnit tests green is the acceptance bar for this file, not matching this
XML byte-for-byte.

- [ ] **Step 6: Write the failing MUnit suite**

`mule-app/src/test/munit/salesforce-system-api-test-suite.xml`:

**Revised during execution — same reason as Step 5.** Every test now mocks
`java:invoke` (the JWT signing call — return a canned assertion string, no
real keystore needed in tests) and two distinct `http:request` calls
distinguished by `path`: the token endpoint (`/services/oauth2/token`,
always mocked the same way) and whichever Salesforce REST endpoint the
flow under test calls.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns:munit="http://www.mulesoft.org/schema/mule/munit"
      xmlns:munit-tools="http://www.mulesoft.org/schema/mule/munit-tools"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:java="http://www.mulesoft.org/schema/mule/java"
      xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/munit http://www.mulesoft.org/schema/mule/munit/current/mule-munit.xsd
        http://www.mulesoft.org/schema/mule/munit-tools http://www.mulesoft.org/schema/mule/munit-tools/current/mule-munit-tools.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/java http://www.mulesoft.org/schema/mule/java/current/mule-java.xsd">

    <munit:config name="salesforce-system-api-test-suite.xml"/>

    <munit:test name="create-order-maps-fields-and-calls-salesforce-rest-create">
        <munit:behavior>
            <munit-tools:mock-when processor="java:invoke">
                <munit-tools:then-return>
                    <munit-tools:payload value="#['mock.jwt.assertion']"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/services/oauth2/token" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[{ access_token: "mock-token", instance_url: "https://mock.my.salesforce.com" }]' mediaType="application/java"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/services/data/v60.0/sobjects/Synthesis_Order__c" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[{ id: "001XX", success: true }]' mediaType="application/java"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
        </munit:behavior>
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { name: "ORD-1", accountId: "001AA", sequence: "ACGT", lengthBp: 4, status: "Feasible", feasibilityScore: 0.9, promisedShipDate: "2026-09-01", batchId: "batch-1" }]'/>
            </set-event>
            <flow-ref name="sf-create-order-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:assert-that expression="#[payload.id]" is="#[MunitTools::equalTo('001XX')]"/>
        </munit:validation>
    </munit:test>

    <munit:test name="get-orders-transforms-salesforce-records-to-json-shape">
        <munit:behavior>
            <munit-tools:mock-when processor="java:invoke">
                <munit-tools:then-return>
                    <munit-tools:payload value="#['mock.jwt.assertion']"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/services/oauth2/token" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[{ access_token: "mock-token", instance_url: "https://mock.my.salesforce.com" }]' mediaType="application/java"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/services/data/v60.0/query" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[{ records: [{ Id: "001XX", Name: "ORD-1", Status__c: "Feasible", Feasibility_Score__c: 0.9, Progress_Pct__c: 0, Batch_Id__c: "batch-1", Promised_Ship_Date__c: "2026-09-01" }] }]' mediaType="application/java"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
        </munit:behavior>
        <munit:execution>
            <flow-ref name="sf-get-orders-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:assert-that expression="#[payload[0].batchId]" is="#[MunitTools::equalTo('batch-1')]"/>
        </munit:validation>
    </munit:test>

    <munit:test name="get-order-by-batch-returns-404-when-not-found">
        <munit:behavior>
            <munit-tools:mock-when processor="java:invoke">
                <munit-tools:then-return>
                    <munit-tools:payload value="#['mock.jwt.assertion']"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/services/oauth2/token" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[{ access_token: "mock-token", instance_url: "https://mock.my.salesforce.com" }]' mediaType="application/java"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/services/data/v60.0/query" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[{ records: [] }]' mediaType="application/java"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
        </munit:behavior>
        <munit:execution>
            <set-event>
                <munit-tools:attributes value='#[{ uriParams: { batchId: "unknown-batch" } }]'/>
            </set-event>
            <flow-ref name="sf-get-order-by-batch-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:assert-that expression="#[vars.httpStatus]" is="#[MunitTools::equalTo('404')]"/>
        </munit:validation>
    </munit:test>

</mule>
```

The `get-order-by-batch` test needs `attributes.uriParams.batchId` set via
`set-event` since the flow reads it directly (there's no real HTTP listener
in a MUnit `flow-ref` execution) — the value doesn't matter since the mock
always returns zero records regardless of what SOQL was built from it.

- [ ] **Step 7: Build and verify (see the MUnit limitation in Global Constraints)**

MUnit execution does not work in this sandbox — see the Global Constraints
note. The acceptance bar for this step is a clean package build, not a
green MUnit run:

```bash
cd mule-app
~/tools/apache-maven-3.9.6/bin/mvn clean package
```

Expected: `BUILD SUCCESS`, producing `target/poc-ansa-biotech-1.0.0-SNAPSHOT-mule-application.jar` (or similarly named deployable artifact). This validates XML schema correctness, DataWeave syntax, and that every flow/config/connector reference resolves — the same class of errors MUnit's RED phase would have caught. The MUnit suite (Step 6) stays in the repo as correct, intentional test design; it just can't be executed as the pass/fail gate here. Real functional verification of these flows happens in Task 12's end-to-end run against the live Salesforce org.

- [ ] **Step 8: Commit**

```bash
git add mule-app/
git commit -m "Scaffold Mule project and add Salesforce System API"
```

---

### Task 6: Feasibility System API (Mule wrapper)

**Files:**
- Create: `mule-app/src/main/mule/feasibility-system-api.xml`
- Test: `mule-app/src/test/munit/feasibility-system-api-test-suite.xml`

**Interfaces:**
- Consumes: `services/feasibility` `POST /feasibility` (Task 3) via `Feasibility_Service_Request_Config`.
- Produces: `POST /feasibility` on `${feasibility.system.api.port}` (default 8083), passthrough shape `{score, feasible, reasons, flags}`. Consumed by the Process API (Task 7).

- [ ] **Step 1: Write the failing MUnit suite**

`mule-app/src/test/munit/feasibility-system-api-test-suite.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns:munit="http://www.mulesoft.org/schema/mule/munit"
      xmlns:munit-tools="http://www.mulesoft.org/schema/mule/munit-tools"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/munit http://www.mulesoft.org/schema/mule/munit/current/mule-munit.xsd
        http://www.mulesoft.org/schema/mule/munit-tools http://www.mulesoft.org/schema/mule/munit-tools/current/mule-munit-tools.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd">

    <munit:config name="feasibility-system-api-test-suite.xml"/>

    <munit:test name="feasibility-flow-proxies-request-and-returns-scoring-response">
        <munit:behavior>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/feasibility" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[[{ score: 0.9, feasible: true, reasons: [], flags: { gc_content: 0.5, max_homopolymer_run: 3, repeat_regions: [] } }]]' mediaType="application/json"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
        </munit:behavior>
        <munit:execution>
            <flow-ref name="feasibility-system-api-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:assert-that expression="#[payload.feasible]" is="#[MunitTools::equalTo(true)]"/>
        </munit:validation>
    </munit:test>

</mule>
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd mule-app
mvn -q clean test -Dtest=feasibility-system-api-test-suite
```

Expected: FAIL — no flow named `feasibility-system-api-flow`.

- [ ] **Step 3: Write `feasibility-system-api.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd">

    <flow name="feasibility-system-api-flow">
        <http:listener config-ref="Feasibility_System_API_Listener_Config" path="/feasibility" allowedMethods="POST"/>
        <http:request config-ref="Feasibility_Service_Request_Config" method="POST" path="/feasibility">
            <http:body><![CDATA[#[payload]]]></http:body>
        </http:request>
    </flow>

</mule>
```

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q clean test -Dtest=feasibility-system-api-test-suite
```

Expected: `Tests run: 1, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add mule-app/src/main/mule/feasibility-system-api.xml mule-app/src/test/munit/feasibility-system-api-test-suite.xml
git commit -m "Add Feasibility System API proxy flow"
```

---

### Task 7: Order Process API (orchestration)

**Files:**
- Create: `mule-app/src/main/mule/process-api.xml` (order-submission flow only — telemetry consumer flow added in Task 8)
- Test: `mule-app/src/test/munit/process-api-order-test-suite.xml`

**Interfaces:**
- Consumes: Feasibility System API `POST /feasibility` (Task 6), Salesforce System API `POST /synthesis-orders` (Task 5), JMS queue `synthesis-jobs`.
- Produces: `POST /orders` on `${process.api.port}` (default 8084), body `{sequence, accountId, requestedShipDate}` → `{orderId, batchId, feasible, score, reasons, status}`. Publishes `{batchId, orderId, requestedShipDate}` JSON to JMS queue `synthesis-jobs` for feasible orders only. Consumed by the Experience API (Task 9) and the instrument simulator (Task 10, via queue).

- [ ] **Step 1: Write the failing MUnit suite**

`mule-app/src/test/munit/process-api-order-test-suite.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns:munit="http://www.mulesoft.org/schema/mule/munit"
      xmlns:munit-tools="http://www.mulesoft.org/schema/mule/munit-tools"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:jms="http://www.mulesoft.org/schema/mule/jms"
      xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/munit http://www.mulesoft.org/schema/mule/munit/current/mule-munit.xsd
        http://www.mulesoft.org/schema/mule/munit-tools http://www.mulesoft.org/schema/mule/munit-tools/current/mule-munit-tools.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/jms http://www.mulesoft.org/schema/mule/jms/current/mule-jms.xsd">

    <munit:config name="process-api-order-test-suite.xml"/>

    <munit:test name="feasible-order-writes-salesforce-before-publishing-to-queue">
        <munit:behavior>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/feasibility" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[[{ score: 0.9, feasible: true, reasons: [], flags: {} }]]' mediaType="application/json"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/synthesis-orders" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[[{ id: "001XX" }]]' mediaType="application/json"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="jms:publish"/>
        </munit:behavior>
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { sequence: "ACGTACGTACGTACGTACGT", accountId: "001AA", requestedShipDate: "2026-09-01" }]'/>
            </set-event>
            <flow-ref name="process-order-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:verify-call processor="http:request" withAttributeValue="#[{ attributeName: 'path', whereValue: '/synthesis-orders' }]" times="1"/>
            <munit-tools:verify-call processor="jms:publish" times="1"/>
            <munit-tools:assert-that expression="#[payload.feasible]" is="#[MunitTools::equalTo(true)]"/>
        </munit:validation>
    </munit:test>

    <munit:test name="infeasible-order-does-not-publish-to-queue">
        <munit:behavior>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/feasibility" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[[{ score: 0.2, feasible: false, reasons: ["High GC content"], flags: {} }]]' mediaType="application/json"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/synthesis-orders" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[[{ id: "001XX" }]]' mediaType="application/json"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="jms:publish"/>
        </munit:behavior>
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { sequence: "GCGCGCGCGCGCGCGCGCGC", accountId: "001AA", requestedShipDate: "2026-09-01" }]'/>
            </set-event>
            <flow-ref name="process-order-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:verify-call processor="jms:publish" times="0"/>
            <munit-tools:assert-that expression="#[payload.status]" is="#[MunitTools::equalTo('Rejected')]"/>
        </munit:validation>
    </munit:test>

    <munit:test name="feasibility-service-timeout-returns-502-and-does-not-write-salesforce">
        <munit:behavior>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="/feasibility" attributeName="path"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:error typeId="HTTP:TIMEOUT"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
        </munit:behavior>
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { sequence: "ACGTACGTACGTACGTACGT", accountId: "001AA", requestedShipDate: "2026-09-01" }]'/>
            </set-event>
            <flow-ref name="process-order-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:verify-call processor="http:request" withAttributeValue="#[{ attributeName: 'path', whereValue: '/synthesis-orders' }]" times="0"/>
            <munit-tools:assert-that expression="#[vars.httpStatus]" is="#[MunitTools::equalTo('502')]"/>
        </munit:validation>
    </munit:test>

</mule>
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd mule-app
mvn -q clean test -Dtest=process-api-order-test-suite
```

Expected: FAIL — no flow named `process-order-flow`.

- [ ] **Step 3: Write `process-api.xml` (order-submission flow)**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:jms="http://www.mulesoft.org/schema/mule/jms"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/jms http://www.mulesoft.org/schema/mule/jms/current/mule-jms.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd">

    <flow name="process-order-flow">
        <error-handler>
            <on-error-propagate type="HTTP:TIMEOUT, HTTP:CONNECTIVITY">
                <set-variable variableName="httpStatus" value="502"/>
                <set-payload value='#[output application/json --- { error: "feasibility service unavailable" }]'/>
            </on-error-propagate>
        </error-handler>

        <set-variable variableName="orderPayload" value="#[payload]"/>
        <set-variable variableName="batchId" value="#[uuid()]"/>

        <http:request config-ref="Feasibility_System_API_Request_Config" method="POST" path="/feasibility">
            <http:body><![CDATA[#[output application/json --- { sequence: vars.orderPayload.sequence }]]]></http:body>
        </http:request>
        <set-variable variableName="feasibilityResult" value="#[payload]"/>

        <choice>
            <when expression="#[vars.feasibilityResult.feasible]">
                <ee:transform>
                    <ee:message>
                        <ee:set-payload><![CDATA[%dw 2.0
output application/json
var order = vars.orderPayload
var result = vars.feasibilityResult
---
{
    name: "ORD-" ++ vars.batchId[0 to 7],
    accountId: order.accountId,
    sequence: order.sequence,
    lengthBp: sizeOf(order.sequence),
    status: "Feasible",
    feasibilityScore: result.score,
    promisedShipDate: order.requestedShipDate,
    batchId: vars.batchId
}]]></ee:set-payload>
                    </ee:message>
                </ee:transform>
                <http:request config-ref="SF_System_API_Request_Config" method="POST" path="/synthesis-orders">
                    <http:body><![CDATA[#[payload]]]></http:body>
                </http:request>
                <set-variable variableName="orderId" value="#[payload.id]"/>

                <jms:publish config-ref="JMS_Config" destination="synthesis-jobs">
                    <jms:message>
                        <jms:body><![CDATA[#[output application/json --- { batchId: vars.batchId, orderId: vars.orderId, requestedShipDate: vars.orderPayload.requestedShipDate }]]]></jms:body>
                    </jms:message>
                </jms:publish>

                <ee:transform>
                    <ee:message>
                        <ee:set-payload><![CDATA[%dw 2.0
output application/json
---
{
    orderId: vars.orderId,
    batchId: vars.batchId,
    feasible: true,
    score: vars.feasibilityResult.score,
    reasons: vars.feasibilityResult.reasons,
    status: "Feasible"
}]]></ee:set-payload>
                    </ee:message>
                </ee:transform>
            </when>
            <otherwise>
                <ee:transform>
                    <ee:message>
                        <ee:set-payload><![CDATA[%dw 2.0
output application/json
var order = vars.orderPayload
var result = vars.feasibilityResult
---
{
    name: "ORD-" ++ vars.batchId[0 to 7],
    accountId: order.accountId,
    sequence: order.sequence,
    lengthBp: sizeOf(order.sequence),
    status: "Rejected",
    feasibilityScore: result.score,
    rejectionReason: result.reasons joinBy ", ",
    promisedShipDate: order.requestedShipDate,
    batchId: vars.batchId
}]]></ee:set-payload>
                    </ee:message>
                </ee:transform>
                <http:request config-ref="SF_System_API_Request_Config" method="POST" path="/synthesis-orders">
                    <http:body><![CDATA[#[payload]]]></http:body>
                </http:request>
                <ee:transform>
                    <ee:message>
                        <ee:set-payload><![CDATA[%dw 2.0
output application/json
---
{
    batchId: vars.batchId,
    feasible: false,
    score: vars.feasibilityResult.score,
    reasons: vars.feasibilityResult.reasons,
    status: "Rejected"
}]]></ee:set-payload>
                    </ee:message>
                </ee:transform>
            </otherwise>
        </choice>
    </flow>

</mule>
```

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q clean test -Dtest=process-api-order-test-suite
```

Expected: `Tests run: 3, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add mule-app/src/main/mule/process-api.xml mule-app/src/test/munit/process-api-order-test-suite.xml
git commit -m "Add Process API order-submission orchestration flow"
```

---

### Task 8: Telemetry consumer (Process API)

**Files:**
- Modify: `mule-app/src/main/mule/process-api.xml` (append telemetry-consumer flow)
- Test: `mule-app/src/test/munit/process-api-telemetry-test-suite.xml`

**Interfaces:**
- Consumes: JMS queue `batch-telemetry` (messages `{batchId, progressPct, event, timestamp}` where `event` is `started|running|qc_pass|qc_fail|shipped`), Salesforce System API `GET /synthesis-orders/{batchId}` and `PATCH /synthesis-orders/{batchId}` (Task 5).
- Produces: updates to `Synthesis_Order__c.Status__c` / `Progress_Pct__c`; dead-letters unresolvable messages to JMS queue `batch-telemetry-dlq`.

**Status lifecycle (forward-only) and event mapping:**

| Lifecycle order | Status |
|---|---|
| 1 | Feasible |
| 2 | In_Synthesis |
| 3 | QC |
| 4 | Shipped |

`At_Risk` is a lateral flag on top of `In_Synthesis`/`QC`, not a lifecycle step. Event → status: `started`→`In_Synthesis`, `running`→`In_Synthesis` (or `At_Risk` if projected late), `qc_pass`/`qc_fail`→`QC`, `shipped`→`Shipped`.

- [ ] **Step 1: Write the failing MUnit suite**

`mule-app/src/test/munit/process-api-telemetry-test-suite.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns:munit="http://www.mulesoft.org/schema/mule/munit"
      xmlns:munit-tools="http://www.mulesoft.org/schema/mule/munit-tools"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:jms="http://www.mulesoft.org/schema/mule/jms"
      xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/munit http://www.mulesoft.org/schema/mule/munit/current/mule-munit.xsd
        http://www.mulesoft.org/schema/mule/munit-tools http://www.mulesoft.org/schema/mule/munit-tools/current/mule-munit-tools.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/jms http://www.mulesoft.org/schema/mule/jms/current/mule-jms.xsd">

    <munit:config name="process-api-telemetry-test-suite.xml"/>

    <munit:test name="running-event-advances-status-and-updates-progress">
        <munit:behavior>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="GET" attributeName="method"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[[{ status: "Feasible", progressPct: 0, promisedShipDate: "2026-09-10" }]]' mediaType="application/json"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="PATCH" attributeName="method"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[[{ updated: true }]]' mediaType="application/json"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
        </munit:behavior>
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { batchId: "batch-1", progressPct: 40, event: "running", timestamp: "2026-08-24T10:00:00Z" }]'/>
            </set-event>
            <flow-ref name="telemetry-consumer-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:verify-call processor="http:request" withAttributeValue="#[{ attributeName: 'method', whereValue: 'PATCH' }]" times="1"/>
        </munit:validation>
    </munit:test>

    <munit:test name="stale-event-does-not-move-status-backward">
        <munit:behavior>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="GET" attributeName="method"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:payload value='#[[{ status: "Shipped", progressPct: 100, promisedShipDate: "2026-09-10" }]]' mediaType="application/json"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
        </munit:behavior>
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { batchId: "batch-2", progressPct: 40, event: "running", timestamp: "2026-08-20T10:00:00Z" }]'/>
            </set-event>
            <flow-ref name="telemetry-consumer-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:verify-call processor="http:request" withAttributeValue="#[{ attributeName: 'method', whereValue: 'PATCH' }]" times="0"/>
        </munit:validation>
    </munit:test>

    <munit:test name="unknown-batch-id-dead-letters-after-lookup-miss">
        <munit:behavior>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:with-attributes>
                    <munit-tools:with-attribute whereValue="GET" attributeName="method"/>
                </munit-tools:with-attributes>
                <munit-tools:then-return>
                    <munit-tools:attributes value='#[{ statusCode: 404 }]'/>
                    <munit-tools:payload value='#[output application/json --- { error: "not found" }]'/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
            <munit-tools:mock-when processor="jms:publish"/>
        </munit:behavior>
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { batchId: "unknown-batch", progressPct: 10, event: "started", timestamp: "2026-08-24T10:00:00Z" }]'/>
            </set-event>
            <flow-ref name="telemetry-consumer-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:verify-call processor="jms:publish" withAttributeValue="#[{ attributeName: 'destination', whereValue: 'batch-telemetry-dlq' }]" times="1"/>
        </munit:validation>
    </munit:test>

</mule>
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd mule-app
mvn -q clean test -Dtest=process-api-telemetry-test-suite
```

Expected: FAIL — no flow named `telemetry-consumer-flow`.

- [ ] **Step 3: Append the telemetry-consumer flow to `process-api.xml`**

Add before the closing `</mule>` tag:

```xml
    <flow name="telemetry-consumer-flow">
        <jms:listener config-ref="JMS_Config" destination="batch-telemetry"/>
        <set-variable variableName="telemetry" value="#[payload]"/>

        <http:request config-ref="SF_System_API_Request_Config" method="GET" path="/synthesis-orders/#[vars.telemetry.batchId]">
            <http:response-validator>
                <http:success-status-code-validator values="0..599"/>
            </http:response-validator>
        </http:request>

        <choice>
            <when expression="#[attributes.statusCode == 404]">
                <jms:publish config-ref="JMS_Config" destination="batch-telemetry-dlq">
                    <jms:message>
                        <jms:body><![CDATA[#[vars.telemetry]]]></jms:body>
                    </jms:message>
                </jms:publish>
            </when>
            <otherwise>
                <set-variable variableName="currentOrder" value="#[payload]"/>
                <set-variable variableName="lifecycleRank" value='#[{
                    "Submitted": 0, "Feasible": 1, "Rejected": 1,
                    "In_Synthesis": 2, "QC": 3, "Shipped": 4, "At_Risk": 2
                }]'/>
                <set-variable variableName="eventStatusMap" value='#[{
                    "started": "In_Synthesis", "running": "In_Synthesis",
                    "qc_pass": "QC", "qc_fail": "QC", "shipped": "Shipped"
                }]'/>
                <set-variable variableName="candidateStatus" value="#[vars.eventStatusMap[vars.telemetry.event]]"/>
                <set-variable variableName="isForwardMove"
                    value="#[vars.lifecycleRank[vars.candidateStatus] >= vars.lifecycleRank[vars.currentOrder.status]]"/>

                <choice>
                    <when expression="#[vars.isForwardMove]">
                        <set-variable variableName="daysElapsedRatio"
                            value="#[if (vars.telemetry.progressPct == 0) 0 else (vars.telemetry.progressPct / 100)]"/>
                        <set-variable variableName="projectedLate"
                            value="#[(vars.telemetry.event == 'running') and (vars.telemetry.progressPct < 50) and (vars.currentOrder.promisedShipDate as DateTime) &lt; (now() as DateTime >> |P7D|)]"/>
                        <set-variable variableName="finalStatus"
                            value="#[if (vars.projectedLate) 'At_Risk' else vars.candidateStatus]"/>
                        <ee:transform>
                            <ee:message>
                                <ee:set-payload><![CDATA[%dw 2.0
output application/json
---
{ status: vars.finalStatus, progressPct: vars.telemetry.progressPct }]]></ee:set-payload>
                            </ee:message>
                        </ee:transform>
                        <http:request config-ref="SF_System_API_Request_Config" method="PATCH" path="/synthesis-orders/#[vars.telemetry.batchId]">
                            <http:body><![CDATA[#[payload]]]></http:body>
                        </http:request>
                    </when>
                    <otherwise>
                        <logger level="WARN" message="#['Ignoring stale/out-of-order telemetry for batch ' ++ vars.telemetry.batchId ++ ': event=' ++ vars.telemetry.event]"/>
                    </otherwise>
                </choice>
            </otherwise>
        </choice>
    </flow>
```

Add `xmlns:ee` to the file's root element if not already present from Task 7 (it is).

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q clean test -Dtest=process-api-telemetry-test-suite
```

Expected: `Tests run: 3, Failures: 0, Errors: 0`. If the DataWeave date-math expression errors, simplify `projectedLate` to compare `vars.telemetry.progressPct` against a fixed threshold only (e.g. `< 50`) — the date projection is a nice-to-have refinement, the forward-only state machine is the load-bearing behavior under test.

- [ ] **Step 5: Commit**

```bash
git add mule-app/src/main/mule/process-api.xml mule-app/src/test/munit/process-api-telemetry-test-suite.xml
git commit -m "Add telemetry consumer flow with forward-only state machine and DLQ"
```

---

### Task 9: Experience API

**Files:**
- Create: `mule-app/src/main/mule/experience-api.xml`
- Test: `mule-app/src/test/munit/experience-api-test-suite.xml`

**Interfaces:**
- Consumes: Process API `POST /orders` (Task 7), Salesforce System API `GET /synthesis-orders` (Task 5).
- Produces: `POST /orders` and `GET /orders` on `${experience.api.port}` (default 8081). Consumed by the dashboard (Task 11).

- [ ] **Step 1: Write the failing MUnit suite**

`mule-app/src/test/munit/experience-api-test-suite.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns:munit="http://www.mulesoft.org/schema/mule/munit"
      xmlns:munit-tools="http://www.mulesoft.org/schema/mule/munit-tools"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/munit http://www.mulesoft.org/schema/mule/munit/current/mule-munit.xsd
        http://www.mulesoft.org/schema/mule/munit-tools http://www.mulesoft.org/schema/mule/munit-tools/current/mule-munit-tools.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd">

    <munit:config name="experience-api-test-suite.xml"/>

    <munit:test name="invalid-sequence-returns-400-without-calling-process-api">
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { sequence: "", accountId: "001AA", requestedShipDate: "2026-09-01" }]'/>
            </set-event>
            <flow-ref name="experience-submit-order-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:verify-call processor="http:request" times="0"/>
            <munit-tools:assert-that expression="#[vars.httpStatus]" is="#[MunitTools::equalTo('400')]"/>
        </munit:validation>
    </munit:test>

    <munit:test name="sequence-with-invalid-bases-returns-400">
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { sequence: "ACGTXYZ", accountId: "001AA", requestedShipDate: "2026-09-01" }]'/>
            </set-event>
            <flow-ref name="experience-submit-order-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:assert-that expression="#[vars.httpStatus]" is="#[MunitTools::equalTo('400')]"/>
        </munit:validation>
    </munit:test>

    <munit:test name="valid-sequence-proxies-to-process-api">
        <munit:behavior>
            <munit-tools:mock-when processor="http:request">
                <munit-tools:then-return>
                    <munit-tools:payload value='#[[{ orderId: "001XX", batchId: "batch-1", feasible: true, score: 0.9, reasons: [], status: "Feasible" }]]' mediaType="application/json"/>
                </munit-tools:then-return>
            </munit-tools:mock-when>
        </munit:behavior>
        <munit:execution>
            <set-event>
                <munit-tools:payload value='#[output application/json --- { sequence: "ACGTACGTACGTACGTACGT", accountId: "001AA", requestedShipDate: "2026-09-01" }]'/>
            </set-event>
            <flow-ref name="experience-submit-order-flow"/>
        </munit:execution>
        <munit:validation>
            <munit-tools:verify-call processor="http:request" times="1"/>
            <munit-tools:assert-that expression="#[payload.feasible]" is="#[MunitTools::equalTo(true)]"/>
        </munit:validation>
    </munit:test>

</mule>
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd mule-app
mvn -q clean test -Dtest=experience-api-test-suite
```

Expected: FAIL — no flow named `experience-submit-order-flow`.

- [ ] **Step 3: Write `experience-api.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:ee="http://www.mulesoft.org/schema/mule/ee/core"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/ee/core http://www.mulesoft.org/schema/mule/ee/core/current/mule-ee.xsd">

    <flow name="experience-submit-order-flow">
        <http:listener config-ref="Experience_API_Listener_Config" path="/orders" allowedMethods="POST"/>
        <choice>
            <when expression="#[not (isEmpty(payload.sequence default '')) and (payload.sequence matches /^[ACGTN]+$/)]">
                <http:request config-ref="Process_API_Request_Config" method="POST" path="/orders">
                    <http:body><![CDATA[#[payload]]]></http:body>
                </http:request>
            </when>
            <otherwise>
                <set-variable variableName="httpStatus" value="400"/>
                <set-payload value='#[output application/json --- { error: "sequence must be non-empty and contain only IUPAC bases A/C/G/T/N" }]'/>
            </otherwise>
        </choice>
    </flow>

    <flow name="experience-get-orders-flow">
        <http:listener config-ref="Experience_API_Listener_Config" path="/orders" allowedMethods="GET"/>
        <http:request config-ref="SF_System_API_Request_Config" method="GET" path="/synthesis-orders"/>
    </flow>

</mule>
```

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q clean test -Dtest=experience-api-test-suite
```

Expected: `Tests run: 3, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add mule-app/src/main/mule/experience-api.xml mule-app/src/test/munit/experience-api-test-suite.xml
git commit -m "Add Experience API with sequence input validation"
```

---

### Task 10: Instrument telemetry simulator (Python)

**Files:**
- Create: `services/instrument-simulator/simulator.py`
- Create: `services/instrument-simulator/requirements.txt`
- Create: `services/instrument-simulator/Dockerfile`
- Test: `services/instrument-simulator/tests/test_simulator.py`

**Interfaces:**
- Consumes: STOMP queue `synthesis-jobs` (JSON `{batchId, orderId, requestedShipDate}`, published by the Process API in Task 7).
- Produces: STOMP queue `batch-telemetry` (JSON `{batchId, progressPct, event, timestamp}`), consumed by the telemetry-consumer flow (Task 8).

- [ ] **Step 1: Write the failing test for the timeline generator**

`services/instrument-simulator/tests/test_simulator.py`:

```python
from simulator import build_telemetry_timeline


def test_timeline_starts_at_zero_and_ends_shipped():
    timeline = build_telemetry_timeline(batch_id="batch-1", tick_seconds=0)
    assert timeline[0]["event"] == "started"
    assert timeline[0]["progressPct"] == 0
    assert timeline[-1]["event"] == "shipped"
    assert timeline[-1]["progressPct"] == 100


def test_timeline_progress_is_monotonically_nondecreasing():
    timeline = build_telemetry_timeline(batch_id="batch-1", tick_seconds=0)
    progresses = [e["progressPct"] for e in timeline]
    assert progresses == sorted(progresses)


def test_every_event_carries_the_batch_id():
    timeline = build_telemetry_timeline(batch_id="batch-42", tick_seconds=0)
    assert all(e["batchId"] == "batch-42" for e in timeline)


def test_qc_fail_variant_ends_without_shipped():
    timeline = build_telemetry_timeline(batch_id="batch-1", tick_seconds=0, qc_outcome="qc_fail")
    events = [e["event"] for e in timeline]
    assert "qc_fail" in events
    assert "shipped" not in events
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd services/instrument-simulator
pip install pytest stomp.py
pytest tests/test_simulator.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'simulator'`.

- [ ] **Step 3: Write `simulator.py`**

```python
import json
import time
import uuid
from datetime import datetime, timezone

import stomp


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_telemetry_timeline(batch_id: str, tick_seconds: float = 2.0, qc_outcome: str = "qc_pass") -> list[dict]:
    steps = [
        ("started", 0),
        ("running", 25),
        ("running", 50),
        ("running", 75),
        (qc_outcome, 90),
    ]
    if qc_outcome == "qc_pass":
        steps.append(("shipped", 100))

    timeline = []
    for event, progress in steps:
        timeline.append({
            "batchId": batch_id,
            "progressPct": progress,
            "event": event,
            "timestamp": _now_iso(),
        })
        if tick_seconds:
            time.sleep(tick_seconds)
    return timeline


class TelemetryPublisher:
    def __init__(self, host: str, port: int, username: str = "admin", password: str = "admin"):
        self._conn = stomp.Connection([(host, port)])
        self._conn.connect(username, password, wait=True)

    def publish(self, event: dict) -> None:
        self._conn.send(body=json.dumps(event), destination="/queue/batch-telemetry")

    def close(self) -> None:
        self._conn.disconnect()


class SynthesisJobListener(stomp.ConnectionListener):
    def __init__(self, on_job):
        self._on_job = on_job

    def on_message(self, frame):
        job = json.loads(frame.body)
        self._on_job(job)


def run(host: str, port: int, tick_seconds: float = 2.0) -> None:
    publisher = TelemetryPublisher(host, port)

    def handle_job(job: dict) -> None:
        for event in build_telemetry_timeline(batch_id=job["batchId"], tick_seconds=tick_seconds):
            publisher.publish(event)

    listener_conn = stomp.Connection([(host, port)])
    listener_conn.set_listener("", SynthesisJobListener(handle_job))
    listener_conn.connect("admin", "admin", wait=True)
    listener_conn.subscribe(destination="/queue/synthesis-jobs", id=str(uuid.uuid4()), ack="auto")

    while True:
        time.sleep(1)


if __name__ == "__main__":
    import os
    run(
        host=os.environ.get("ACTIVEMQ_STOMP_HOST", "activemq"),
        port=int(os.environ.get("ACTIVEMQ_STOMP_PORT", "61613")),
        tick_seconds=float(os.environ.get("SIMULATOR_TICK_SECONDS", "2")),
    )
```

- [ ] **Step 4: Run to verify it passes**

```bash
pytest tests/test_simulator.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Write `requirements.txt` and `Dockerfile`**

`services/instrument-simulator/requirements.txt`:

```
stomp.py==8.1.2
pytest==8.2.0
```

`services/instrument-simulator/Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY simulator.py .
CMD ["python", "simulator.py"]
```

- [ ] **Step 6: Commit**

```bash
git add services/instrument-simulator/
git commit -m "Add instrument telemetry simulator"
```

---

### Task 11: Status dashboard

**Files:**
- Create: `dashboard/index.html`
- Create: `dashboard/app.js`
- Create: `dashboard/styles.css`
- Test: `dashboard/tests/app.test.js`

**Interfaces:**
- Consumes: Experience API `GET /orders` and `POST /orders` (Task 9), same-origin via a reverse-proxied path or direct `fetch` to the compose service (configured in Task 12).
- Produces: `formatOrderRow(order)` and `statusBadgeClass(status)` pure functions in `app.js`, unit tested with Node's built-in test runner; the polling/render loop consuming them is manually verified in the browser (no headless-browser dependency added for a POC).

- [ ] **Step 1: Write the failing unit tests for the pure rendering functions**

`dashboard/tests/app.test.js`:

```javascript
const test = require('node:test');
const assert = require('node:assert');
const { formatOrderRow, statusBadgeClass } = require('../app.js');

test('formatOrderRow renders name, status, and rounded progress', () => {
  const row = formatOrderRow({ name: 'ORD-1', status: 'In_Synthesis', progressPct: 42.7, feasibilityScore: 0.91, batchId: 'batch-1' });
  assert.match(row, /ORD-1/);
  assert.match(row, /In_Synthesis/);
  assert.match(row, /43%/);
  assert.match(row, /0\.91/);
});

test('statusBadgeClass maps At_Risk to a warning class', () => {
  assert.strictEqual(statusBadgeClass('At_Risk'), 'badge badge-warning');
});

test('statusBadgeClass maps Shipped to a success class', () => {
  assert.strictEqual(statusBadgeClass('Shipped'), 'badge badge-success');
});

test('statusBadgeClass maps Rejected to a danger class', () => {
  assert.strictEqual(statusBadgeClass('Rejected'), 'badge badge-danger');
});

test('statusBadgeClass falls back to neutral for in-progress statuses', () => {
  assert.strictEqual(statusBadgeClass('In_Synthesis'), 'badge badge-neutral');
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd dashboard
node --test tests/app.test.js
```

Expected: FAIL — `app.js` doesn't exist / doesn't export these functions.

- [ ] **Step 3: Write `app.js`**

```javascript
function statusBadgeClass(status) {
  const map = {
    At_Risk: 'badge badge-warning',
    Rejected: 'badge badge-danger',
    Shipped: 'badge badge-success',
  };
  return map[status] || 'badge badge-neutral';
}

function formatOrderRow(order) {
  const progress = Math.round(order.progressPct || 0);
  const score = (order.feasibilityScore ?? 0).toFixed(2);
  return `<tr>
    <td>${order.name}</td>
    <td><span class="${statusBadgeClass(order.status)}">${order.status}</span></td>
    <td>${progress}%</td>
    <td>${score}</td>
    <td>${order.batchId}</td>
  </tr>`;
}

async function fetchOrders() {
  const res = await fetch('/api/orders');
  if (!res.ok) throw new Error('failed to fetch orders');
  return res.json();
}

async function refresh() {
  const orders = await fetchOrders();
  const tbody = document.querySelector('#orders-body');
  tbody.innerHTML = orders.map(formatOrderRow).join('');
}

async function submitOrder(event) {
  event.preventDefault();
  const form = event.target;
  const body = {
    sequence: form.sequence.value.trim().toUpperCase(),
    accountId: form.accountId.value.trim(),
    requestedShipDate: form.requestedShipDate.value,
  };
  const res = await fetch('/api/orders', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const result = await res.json();
  document.querySelector('#submit-result').textContent = JSON.stringify(result);
  await refresh();
}

if (typeof module !== 'undefined') {
  module.exports = { formatOrderRow, statusBadgeClass };
} else {
  document.querySelector('#order-form').addEventListener('submit', submitOrder);
  refresh();
  setInterval(refresh, 3000);
}
```

- [ ] **Step 4: Run to verify it passes**

```bash
node --test tests/app.test.js
```

Expected: 5 passed.

- [ ] **Step 5: Write `index.html` and `styles.css`**

`dashboard/index.html`:

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Ansa POC — Synthesis Orders</title>
  <link rel="stylesheet" href="styles.css">
</head>
<body>
  <h1>Synthesis Orders</h1>

  <form id="order-form">
    <label>Sequence <input name="sequence" required></label>
    <label>Account ID <input name="accountId" required></label>
    <label>Requested Ship Date <input name="requestedShipDate" type="date" required></label>
    <button type="submit">Submit Order</button>
  </form>
  <pre id="submit-result"></pre>

  <table>
    <thead>
      <tr><th>Order</th><th>Status</th><th>Progress</th><th>Feasibility</th><th>Batch</th></tr>
    </thead>
    <tbody id="orders-body"></tbody>
  </table>

  <script src="app.js"></script>
</body>
</html>
```

`dashboard/styles.css`:

```css
body { font-family: system-ui, sans-serif; margin: 2rem; }
table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
th, td { border: 1px solid #ddd; padding: 0.5rem; text-align: left; }
.badge { padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.85rem; }
.badge-success { background: #d4edda; color: #155724; }
.badge-warning { background: #fff3cd; color: #856404; }
.badge-danger { background: #f8d7da; color: #721c24; }
.badge-neutral { background: #e2e3e5; color: #383d41; }
```

- [ ] **Step 6: Commit**

```bash
git add dashboard/
git commit -m "Add polling status dashboard"
```

---

### Task 12: Full stack wiring (Docker Compose + Mule packaging)

**Files:**
- Create: `mule-app/docker-entrypoint.sh`
- Create: `mule-app/Dockerfile`
- Modify: `docker-compose.yml` (extend with mule-app, feasibility, instrument-simulator, dashboard)
- Create: `README.md`

**Interfaces:**
- Produces: `docker-compose up` bringing up the entire stack, dashboard reachable at `http://localhost:8081` (Experience API also serves the static dashboard files, reverse-proxied at `/api/*`), demonstrating an end-to-end order-to-shipment run.

- [ ] **Step 1: Write `mule-app/docker-entrypoint.sh`**

```bash
#!/bin/sh
set -e

exec /opt/mule/bin/mule \
  -M-Dsf.system.api.port="${SF_SYSTEM_API_PORT}" \
  -M-Dfeasibility.system.api.port="${FEASIBILITY_SYSTEM_API_PORT}" \
  -M-Dprocess.api.port="${PROCESS_API_PORT}" \
  -M-Dexperience.api.port="${EXPERIENCE_API_PORT}" \
  -M-Dfeasibility.host="${FEASIBILITY_HOST}" \
  -M-Dfeasibility.port="${FEASIBILITY_PORT}" \
  -M-Dsf.system.api.host="${SF_SYSTEM_API_HOST}" \
  -M-Dfeasibility.system.api.host="${FEASIBILITY_SYSTEM_API_HOST}" \
  -M-Dprocess.api.host="${PROCESS_API_HOST}" \
  -M-Dsfdc.consumer.key="${SFDC_CONSUMER_KEY}" \
  -M-Dsfdc.username="${SFDC_USERNAME}" \
  -M-Dsfdc.instance.host="${SFDC_INSTANCE_HOST}" \
  -M-Dsfdc.jwt.audience="${SFDC_JWT_AUDIENCE}" \
  -M-Dsfdc.jwt.keystore.path="${SFDC_JWT_KEYSTORE_PATH}" \
  -M-Dsfdc.jwt.keystore.password="${SFDC_JWT_KEYSTORE_PASSWORD}" \
  -M-Dsfdc.jwt.key.alias="${SFDC_JWT_KEY_ALIAS}" \
  -M-Dactivemq.broker.url="${ACTIVEMQ_BROKER_URL}"
```

- [ ] **Step 2: Write `mule-app/Dockerfile`**

```dockerfile
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml mule-artifact.json ./
COPY src ./src
RUN mvn -q clean package -DskipMunitTests

FROM eclipse-temurin:17-jre
ENV MULE_HOME=/opt/mule
RUN apt-get update && apt-get install -y --no-install-recommends unzip curl && rm -rf /var/lib/apt/lists/*
RUN curl -sL https://repository.mulesoft.org/nexus/content/repositories/releases/org/mule/distributions/mule-standalone/4.6.0/mule-standalone-4.6.0.tar.gz \
    | tar xz -C /opt && mv /opt/mule-standalone-4.6.0 /opt/mule
COPY --from=build /build/target/*.jar /opt/mule/apps/poc-ansa-biotech.jar
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
EXPOSE 8081 8082 8083 8084
ENTRYPOINT ["/docker-entrypoint.sh"]
```

**Note:** if the Mule standalone distribution download URL has moved by the time this runs, download the matching `4.6.x` standalone tarball from MuleSoft's distribution site manually and adjust the `curl` URL — this is an infra-plumbing detail to verify at build time, not a design choice.

- [ ] **Step 3: Extend `docker-compose.yml`**

```yaml
services:
  activemq:
    image: apache/activemq-artemis:2.37.0
    environment:
      ARTEMIS_USER: admin
      ARTEMIS_PASSWORD: admin
    ports:
      - "8161:8161"
      - "61616:61616"
      - "61613:61613"
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8161 || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 10

  feasibility:
    build: ./services/feasibility
    ports:
      - "5001:5001"

  mule-app:
    build: ./mule-app
    env_file: .env
    depends_on:
      activemq:
        condition: service_healthy
      feasibility:
        condition: service_started
    ports:
      - "8081:8081"
      - "8082:8082"
      - "8083:8083"
      - "8084:8084"

  instrument-simulator:
    build: ./services/instrument-simulator
    environment:
      ACTIVEMQ_STOMP_HOST: activemq
      ACTIVEMQ_STOMP_PORT: "61613"
      SIMULATOR_TICK_SECONDS: "2"
    depends_on:
      activemq:
        condition: service_healthy

  dashboard:
    image: python:3.11-slim
    working_dir: /app
    volumes:
      - ./dashboard:/app
    command: python -m http.server 8090
    ports:
      - "8090:8090"
    depends_on:
      - mule-app
```

**Note on dashboard's Experience API access:** the dashboard's `fetch('/api/orders')` calls in `app.js` assume same-origin `/api/*` routing. For this POC, simplest correct fix is calling the Experience API's actual origin directly — update `app.js`'s `fetchOrders`/`submitOrder` to call `http://localhost:8081/orders` (not `/api/orders`) since the dashboard is served from a different port (8090) than the Experience API (8081) and no reverse proxy is in this stack. Apply this as an edit to `dashboard/app.js` in this task:

```javascript
const EXPERIENCE_API_BASE = 'http://localhost:8081';
```

and replace `fetch('/api/orders')` with `fetch(`${EXPERIENCE_API_BASE}/orders`)`, and the `submitOrder` fetch URL similarly. Also add CORS headers to the Experience API's two flows in `experience-api.xml` (`<http:response><http:headers><![CDATA[#[{'Access-Control-Allow-Origin': '*'}]]]></http:headers></http:response>` inside each flow) so the browser's cross-origin request succeeds.

- [ ] **Step 4: Write `README.md`**

```markdown
# Ansa Biotechnologies POC — DNA Synthesis Order & Fulfillment Pipeline

MuleSoft-based integration demo: API-led connectivity (Experience/Process/System
APIs), real Salesforce integration, and event-driven instrument telemetry.

## Run it

1. Copy `.env.example` to `.env` and fill in the `SFDC_*` values (see
   `docs/superpowers/plans/2026-08-24-ansa-poc-implementation.md` Task 2 for
   how to obtain them from the Salesforce Developer org).
2. Deploy the Salesforce object model once: `cd salesforce && sf project deploy start --target-org ansa-poc-dev`.
3. `docker compose up --build`.
4. Open `http://localhost:8090` — submit an order, watch its status progress
   from Feasible → In_Synthesis → QC → Shipped (or At_Risk / Rejected) as the
   simulated instrument reports telemetry.

## Architecture

See `docs/superpowers/specs/2026-08-24-ansa-poc-design.md`.
```

- [ ] **Step 5: Bring up the full stack and run the end-to-end demo**

```bash
docker compose up --build -d
sleep 20
curl -s -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"sequence":"ACGTACGTACGTACGTACGTACGTACGT","accountId":"<a real Account Id from your dev org>","requestedShipDate":"2026-09-15"}'
sleep 15
curl -s http://localhost:8081/orders | python3 -m json.tool
```

Expected: the `POST` returns `feasible: true` with a `batchId`; the subsequent `GET` shows that order's `status` having advanced past `Feasible` (e.g. `In_Synthesis`, `QC`, or `Shipped`) as the simulator's telemetry has been consumed.

- [ ] **Step 6: Tear down and commit**

```bash
docker compose down
git add mule-app/docker-entrypoint.sh mule-app/Dockerfile docker-compose.yml README.md dashboard/app.js mule-app/src/main/mule/experience-api.xml
git commit -m "Wire full stack via docker-compose and add end-to-end demo instructions"
```
