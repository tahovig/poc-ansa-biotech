# Ansa Biotechnologies POC — DNA Synthesis Order & Fulfillment Pipeline

MuleSoft-based integration demo: API-led connectivity (Experience/Process/System
APIs), real Salesforce integration, and event-driven instrument telemetry.

## Run it

1. Generate a JWT signing keypair (used for Salesforce's OAuth 2.0 JWT Bearer
   flow; see `docs/superpowers/plans/2026-08-24-ansa-poc-implementation.md`
   Task 2 for how to configure the matching Salesforce Connected App):

   ```bash
   mkdir -p keys
   openssl req -x509 -sha256 -nodes -days 3650 -newkey rsa:2048 \
     -keyout keys/sfdc-jwt.key \
     -out keys/sfdc-jwt.crt \
     -subj "/CN=ansa-poc-mule-integration/O=Ansa POC/C=US"
   ```

   Neither file is committed (`keys/` is gitignored).

2. Copy `.env.example` to `.env` and fill in the `SFDC_*` values.
3. Deploy the Salesforce object model and permission set:

   ```bash
   cd salesforce
   sf project deploy start --target-org ansa-poc-dev
   sf org assign permset --name Synthesis_Order_Access --target-org ansa-poc-dev
   cd ..
   ```

   The permission set assignment is required, not optional: custom fields
   deployed via the Metadata API get zero field-level security on any
   profile by default — including System Administrator — so every SOQL
   query against `Synthesis_Order__c` fails with "No such column" until
   this runs. See "Notable runtime findings" below.

4. `docker compose up --build`.
5. Open `http://localhost:8090` — submit an order (any real Salesforce
   Account Id from your org works for "Account ID"; the dashboard uppercases
   and validates the sequence for you) and watch its status progress from
   Feasible → In_Synthesis → QC → Shipped (or At_Risk / Rejected) as the
   simulated instrument reports telemetry.

## Architecture

See `docs/superpowers/specs/2026-08-24-ansa-poc-design.md`.

## Notable runtime findings

Getting a real Mule 4.6.0 standalone deploy working end to end against a
live Salesforce org and a real ActiveMQ broker surfaced a long list of
runtime behavior that isn't obvious from the docs. A few of the more
interesting ones, each documented in place in the source file it affects:

- **`mule-java-module` has no version compatible with this runtime.**
  Every version new enough for Java 17 declares a `mule-sdk-api` enum
  constant (`JavaVersion.JAVA_25`) the runtime's bundled version lacks;
  patching the runtime to add it breaks its own EE/DataWeave registration
  instead. JWT signing moved out of Mule entirely into a small dedicated
  `services/sfdc-auth/` Python service. See `mule-app/pom.xml`.
- **`-M-D` silently drops flags past the 8th** on the `bin/mule` command
  line — confirmed via `/proc/<pid>/cmdline`, not `ps aux`, which
  truncates. Properties are baked into `wrapper.conf` at container
  startup instead. See `mule-app/docker-entrypoint.sh`.
- **`attributes` is not stable across a flow** — it's replaced by the
  response attributes of every `<http:request>` the flow makes. Reading
  `attributes.uriParams` or `attributes.method` after a token-fetch or
  SOQL call reads *that call's* response attributes, not the original
  inbound request's. See `mule-app/src/main/mule/salesforce-system-api.xml`.
- **Two `<http:listener>` elements sharing one path template** (e.g. GET
  and PATCH both on `/synthesis-orders/{batchId}`) never populate
  `attributes.uriParams` on this runtime, for either listener — merging
  them into one listener with `allowedMethods="GET, PATCH"` and a method
  `<choice>` fixes it. Same file.
- **Salesforce custom fields deployed via the Metadata API get zero
  field-level security by default**, on every profile including System
  Administrator. `sf project deploy start` reports the fields as
  successfully deployed either way — the failure only shows up as "No
  such column" on the first SOQL query. See
  `salesforce/force-app/main/default/permissionsets/`.
- **This broker's default STOMP acceptor doesn't map a `/queue/` prefix**
  onto the plain anycast addresses Mule's JMS side creates — a STOMP
  producer/consumer using `/queue/foo` and a JMS one using `foo` silently
  talk to two different addresses and never exchange a message. See
  `services/instrument-simulator/simulator.py`.

## Testing

- `services/feasibility`, `services/sfdc-auth`, `services/instrument-simulator`:
  `PYTHONPATH=. python3 -m pytest` in each directory.
- `dashboard`: `node --test tests/app.test.js`.
- `mule-app`: MUnit suites exist under `src/test/munit/` but don't execute
  in this environment (see the plan's Task 5 note for the full
  elimination trail); `mvn clean package -DskipMunitTests` is the build's
  acceptance bar instead. The suites are kept in sync with the flows they
  target so they're still useful reading.
