# Ansa Biotechnologies POC: DNA Synthesis Order & Fulfillment Integration Pipeline

**Status:** Approved for implementation planning
**Date:** 2026-08-24
**Repo:** https://github.com/tahovig/poc-ansa-biotech

## Purpose

A portfolio/demo project for a MuleSoft-focused role at Ansa Biotechnologies
(enzymatic DNA synthesis: https://ansabio.com/). Demonstrates API-led
integration and event-driven integration patterns on a domain modeled after
Ansa's actual business: customers order custom DNA constructs through a
Salesforce-based portal, synthesis runs on lab instruments, and Ansa promises
on-time delivery ("On-Time Guarantee").

The POC does not need to be production-hardened. It needs to demonstrate:

- Real MuleSoft API-led connectivity (Experience / Process / System API layers)
- Real Salesforce integration (not mocked) via the Mule Salesforce connector
- Event-driven integration (queue-based telemetry ingestion, correlation,
  stateful order progression)
- Domain understanding of Ansa's actual technical differentiators (synthesis
  feasibility scoring for GC-rich/repetitive/unstable sequences)

## Non-goals

- Production-grade resilience (idempotency keys, full retry/backoff tuning,
  autoscaling, security hardening beyond basic auth to Salesforce)
- Load/performance testing
- A real lab instrument integration — the "instrument" is a simulator
- CloudHub deployment — this runs locally via Docker so any reviewer can run
  it without an Anypoint account

## Architecture

Three-tier API-led design, plus one async event flow, meeting at Salesforce
as the shared system of record.

```
Dashboard (web UI)
    |
    v
Experience API  (order submission, order/status queries)
    |
    v
Process API  (orchestration + telemetry consumer)
    |         \
    v          v
Feasibility    Salesforce System API
System API         |
(Python svc)        v
              Salesforce Dev Org
              (Synthesis_Order__c)

Python instrument simulator --publishes--> ActiveMQ queue --consumed by--> Process API (telemetry listener)
```

Two independent flows share Salesforce as the system of record:

1. **Order flow (synchronous, request/response):** Dashboard -> Experience
   API -> Process API -> Feasibility System API -> Salesforce System API.
   Returns feasibility result and created order to the caller.
2. **Telemetry flow (asynchronous, event-driven):** Python instrument
   simulator -> ActiveMQ queue -> Process API listener -> Salesforce System
   API. Drives order status forward as synthesis progresses; the dashboard
   observes this by polling.

The whole stack (Mule runtime, ActiveMQ, the two Python services, and the
dashboard) runs via a single `docker-compose up`, configured against the
real (free-tier) Salesforce Developer org via connected-app credentials in
an env file. No CloudHub or Anypoint Platform account required to run or
review the demo.

## Components & Data Model

### Salesforce object model

Custom object `Synthesis_Order__c`:

| Field | Type | Notes |
|---|---|---|
| `Order_Name__c` | Text | Display name |
| `Account__c` | Lookup(Account) | Reuse standard Account object |
| `Sequence__c` | Long Text Area | Requested DNA sequence |
| `Length_bp__c` | Number | Sequence length in base pairs |
| `Status__c` | Picklist | Submitted / Feasible / Rejected / In_Synthesis / QC / Shipped / At_Risk |
| `Feasibility_Score__c` | Number | From feasibility service |
| `Rejection_Reason__c` | Long Text Area | Populated when Status = Rejected |
| `Promised_Ship_Date__c` | Date | SLA target |
| `Progress_Pct__c` | Number | Updated by telemetry consumer |
| `Batch_Id__c` | Text | Correlation key linking Salesforce record to telemetry events |

### Feasibility System API (wraps Python service)

`POST /feasibility`
```json
// request
{ "sequence": "ACGT..." }
// response
{
  "score": 0.0-1.0,
  "feasible": true,
  "reasons": ["..."],
  "flags": {
    "gc_content": 0.0-1.0,
    "max_homopolymer_run": 0,
    "repeat_regions": [{"start": 0, "end": 0, "unit": "..."}]
  }
}
```

Scoring logic mirrors Ansa's stated differentiators: GC-rich regions,
homopolymer/repeat runs, and other sequence features that make synthesis
hard. Implemented as real logic (not a stub), since this is the piece most
worth demonstrating domain understanding through.

### Salesforce System API

Thin CRUD + query wrapper over `Synthesis_Order__c` via the Mule Salesforce
connector. No business logic lives here.

### Order Process API

`POST /orders`
```json
// request
{ "sequence": "ACGT...", "accountId": "...", "requestedShipDate": "YYYY-MM-DD" }
```

Flow:
1. Call Feasibility System API.
2. If feasible: create `Synthesis_Order__c` (Status=Feasible) in Salesforce
   **first**, generate a `batchId`, **then** publish
   `SynthesisJobCreated{ batchId, orderId, requestedShipDate }` to the
   queue. Write-before-publish ordering avoids an orphaned synthesis job
   with no backing Salesforce record.
3. If infeasible: create the record as Rejected with reasons, no queue
   message.
4. Return the order + feasibility result to the caller.

### Telemetry consumer (Process API, queue listener)

Consumes `BatchTelemetry{ batchId, progressPct, event, timestamp }` where
`event` is one of `started | running | qc_pass | qc_fail | shipped`.

- Looks up the order by `batchId` (via Salesforce System API query).
- Updates `Status__c` / `Progress_Pct__c`.
- On each `running` event, recomputes On-Time Guarantee risk via a simple
  linear projection (current progress rate vs. time remaining to
  `Promised_Ship_Date__c`); sets `Status__c = At_Risk` if projected to miss.

### Python instrument simulator

Async script that, per active batch, publishes a synthetic progress
timeline (`started` -> several `running` ticks -> `qc_pass`/`qc_fail` ->
`shipped`) at accelerated speed so a full demo run completes in minutes.

### Dashboard

Minimal single-page UI. Polls the Experience API's `GET /orders` every few
seconds; shows order list, feasibility scores, and live status/progress. No
websockets/push — polling is enough for this scope.

## Error Handling

- **Feasibility service unavailable/timeout:** hard fail on order
  submission (502 to caller). No partial Salesforce record is created, no
  silent fallback scoring.
- **Salesforce write failure:** caught in the Process API; submission fails
  cleanly to the caller. Because Salesforce write happens before the queue
  publish, a failed write never leaves an orphaned queue message.
- **Telemetry for unknown `batchId`:** bounded redelivery via ActiveMQ's
  redelivery policy, then dead-lettered to a DLQ with payload intact
  (visible in Mule logs / dashboard health strip) rather than dropped.
- **Out-of-order telemetry:** consumer applies state-precedence — only
  advances `Status__c` forward along the defined lifecycle, logs a warning
  on stale/out-of-order events instead of corrupting the record.
- **Malformed/empty sequence input:** validated at the Experience API
  boundary (non-empty, valid IUPAC bases) before reaching the feasibility
  service — fast 400 instead of a confusing downstream error.
- **Duplicate order submission:** not handled (no idempotency key) —
  explicitly noted as a known simplification, not silently ignored.

This tier is intentionally thin for a POC: the goal is demonstrating
awareness of the failure modes that matter (Salesforce/queue consistency,
dead-letter visibility, event ordering), not exhaustive resilience.

## Testing Strategy

- **Feasibility service (Python):** unit tests on scoring logic against
  known GC-rich/repeat/homopolymer sequences with expected pass/fail
  outcomes — this is the one place domain correctness matters most.
- **Mule flows:** MUnit tests on the Process API, mocking the Salesforce
  and feasibility connectors; assert write-before-publish ordering and
  telemetry state-precedence logic.
- **Integration/demo-level:** a scripted end-to-end run (submit orders of
  varying feasibility, let the simulator drive them to shipped/at-risk)
  doubles as both the interview walkthrough and a smoke test.
- No load/performance testing — out of scope.

## Assumptions

- Salesforce: real free-tier Developer Edition org
  (`orgfarm-46688fa9f2-dev-ed.develop.my.salesforce.com`), connected app
  credentials supplied via env file, not committed.
- Mule runtime: Mule 4.x standalone / Community Edition, no CloudHub.
- Queue: ActiveMQ, run via Docker Compose alongside the rest of the stack.
- Instrument simulator and feasibility service: both Python, to keep the
  stack to two languages (Mule/DataWeave + Python) rather than three.
- Reviewer runbook: `docker-compose up` brings up the full stack; a demo
  script drives sample orders through the pipeline.
