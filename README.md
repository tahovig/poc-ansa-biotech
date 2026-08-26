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
   simulated instrument reports telemetry. (See "Known issue" below —
   `mule-app` currently fails to deploy, so this end-to-end flow doesn't
   work yet.)

## Architecture

See `docs/superpowers/specs/2026-08-24-ansa-poc-design.md`.

## Known issue: `mule-app` does not currently deploy

`docker compose up` brings up `activemq`, `feasibility`, `instrument-simulator`,
and `dashboard` successfully, but `mule-app` fails to deploy with:

```
EnumConstantNotPresentException: org.mule.sdk.api.meta.JavaVersion.JAVA_25
```

This is a version conflict between the publicly-available `mule-java-module`
connector (needed for the Salesforce JWT signing step, `java:invoke-static`
in `salesforce-system-api.xml`) and the Mule 4.6.0 standalone runtime's own
bundled `mule-sdk-api`, which predates the `JAVA_25` enum constant that
connector's compiled metadata references. Swapping in a newer `mule-sdk-api`
fixes that specific error but breaks the runtime's own EE/DataWeave
transform module instead (`Can't resolve .../mule-ee.xsd`) — no publicly
resolvable version of either artifact satisfies both at once. See
`mule-app/pom.xml`'s comment on the `mule-java-module` dependency and
`mule-app/Dockerfile`'s comment near the (unapplied) sdk-api swap for the
full diagnostic trail, and Task 12's report for every combination ruled out.
