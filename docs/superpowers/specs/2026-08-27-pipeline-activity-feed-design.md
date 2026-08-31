# Live Pipeline Activity Feed (STOMP-over-WebSocket)

**Status:** Approved for implementation planning
**Date:** 2026-08-27
**Repo:** https://github.com/tahovig/poc-ansa-biotech

## Purpose

The dashboard's "Pipeline Activity" panel (added in the dashboard redesign,
`dashboard-engagement-ui`) currently narrates order progress by diffing
consecutive polls of `GET /orders`. That's honest but shallow: it can only
report "status changed" or "progress increased," because the dashboard has
no visibility into what the backend actually did to cause that change. For a
MuleSoft integration portfolio piece, the *backend calls themselves* — the
Salesforce writes, the feasibility scoring, the queue publishes — are the
interesting part, and right now they're invisible.

This feature makes them visible, live, as they happen: Mule flows publish a
short activity event each time they make a business-significant backend
call; the dashboard subscribes to those events directly over a WebSocket and
renders them into the same activity feed in real time (sub-second, not
poll-cycle-lag).

## Non-goals

- Instrumenting routine reads. The dashboard's own `GET /orders` polling
  (every 3s) and the System API's `sf-get-orders-flow` it calls are
  explicitly excluded — instrumenting those would spam the feed with noise
  unrelated to any single order's journey, defeating the purpose.
- Instrumenting the Python `instrument-simulator` or `sfdc-auth` services.
  The simulator's telemetry publish is already narrated when
  `telemetry-consumer-flow` reports receiving it; adding a second, separate
  narration of the same event from the publishing side is redundant.
  `sfdc-auth`'s token fetch is mostly cache hits after the first request
  (see its 25-minute TTL cache) and isn't part of any single order's story.
- Deduplicating live WebSocket events against the existing poll-diff events.
  Both mechanisms stay; some overlap (a live "PATCH → In_Synthesis" followed
  shortly by a poll-confirmed "status changed to In Synthesis") is treated
  as two independent observation paths agreeing, not noise to suppress.
- Any change to `activemq/broker.xml` or `docker-compose.yml`. Both are
  unnecessary — see "Validated premises" below.
- Production-grade auth for the WebSocket connection. See "Accepted
  tradeoff" below.

## Validated premises

Spiked against a real Artemis 2.37.0 container (this project's standard
practice: verify runtime behavior empirically rather than trust
documentation, given how much of this project's existing behavior turned
out to differ from what the docs imply) before committing to this design:

1. **STOMP-over-WebSocket works on the existing plain `stomp` acceptor**
   (`tcp://0.0.0.0:61613?...protocols=STOMP...`) with no separate
   WebSocket-specific acceptor and no `broker.xml` change. Artemis's Netty
   transport layer auto-detects the WebSocket upgrade handshake on the same
   port. Confirmed: a Node WebSocket client connecting to
   `ws://<host>:61613/stomp` and sending a STOMP `CONNECT` frame receives a
   proper `CONNECTED` frame back.
2. **This port is already exposed to the host** in `docker-compose.yml`
   (`- "61613:61613"` under the `activemq` service) — the browser can reach
   it exactly as it stands today, no new port mapping needed.
3. **A new destination auto-creates as MULTICAST when first touched by a
   STOMP `SUBSCRIBE`**, and a message published to it from an independent
   client is correctly delivered to the WS subscriber. Confirmed via a
   three-way test: a Node WS-STOMP subscriber, a Python `stomp.py` publisher
   over plain TCP STOMP, both against the same broker — the subscriber
   received the published message.

The one premise *not* independently spiked (would require standing up the
full Mule app rather than just the broker) is that Mule's `<jms:publish>`
element can target that same MULTICAST destination when given
`destinationType="TOPIC"`. This is a standard, documented parameter of the
JMS connector rather than one of this broker's undocumented quirks, so it's
treated as a normal implementation detail to confirm during the build, not
a design risk to spike separately.

## Architecture

```
Mule flow (business call happens)
  -> <try> wraps a fire-and-forget <jms:publish destinationType="TOPIC">
       to "pipeline-activity"
  -> on-error-continue: a publish failure never breaks the business flow

ActiveMQ Artemis (existing broker, existing port 61613)
  -> "pipeline-activity" address, auto-created MULTICAST on first touch

Browser (dashboard/app.js)
  -> hand-rolled STOMP-over-WebSocket client (no external dependency,
     matching the project's no-build-step, no-CDN dashboard so far)
  -> connects to ws://<activemq-host>:61613/stomp on page load
  -> SUBSCRIBEs to "pipeline-activity"
  -> each MESSAGE frame's body (JSON) becomes one entry in the existing
     event feed, alongside the poll-diff-based events already there
  -> auto-reconnects on disconnect (matches the resilience pattern already
     used in services/instrument-simulator/simulator.py for the same
     broker)
```

## Event schema

Each activity event is a JSON object, published as the STOMP message body:

```json
{
  "timestamp": "2026-08-27T14:32:01.123Z",
  "batchId": "9adfe2a1-c4ae-460a-be5f-cbae4a11b234",
  "source": "process-api",
  "message": "Requesting feasibility score"
}
```

- `timestamp`: ISO 8601, set at the Mule flow via `now()`.
- `batchId`: correlates the event to an order. `experience-submit-order-flow`
  doesn't have a `batchId` yet at the point it fires its event (that's
  minted in `process-order-flow`); it publishes with `batchId: null`.
- `source`: which tier/flow published it (`experience-api`, `process-api`,
  `salesforce-system-api`), for possible future filtering — not used by the
  UI in this pass, but cheap to include now and awkward to add later.
- `message`: the human-readable line the dashboard renders directly.

## Publish points

Eight points across three Mule files (`telemetry-consumer-flow` alone
accounts for three, matching its three real backend interactions: receipt,
lookup, and update), each wrapped identically:

```xml
<try>
    <jms:publish config-ref="JMS_Config" destination="pipeline-activity" destinationType="TOPIC">
        <jms:message>
            <jms:body><![CDATA[#[output application/json --- { timestamp: now() as String, batchId: <expr>, source: "<source>", message: "<message>" }]]]></jms:body>
        </jms:message>
    </jms:publish>
    <error-handler>
        <on-error-continue type="ANY"/>
    </error-handler>
</try>
```

| File | Flow | Point | source | message |
|---|---|---|---|---|
| `experience-api.xml` | `experience-submit-order-flow` | after validation passes, before the Process API call | `experience-api` | "Order submission received" |
| `process-api.xml` | `process-order-flow` | before the feasibility `http:request` | `process-api` | "Requesting feasibility score" |
| `process-api.xml` | `process-order-flow` | before the Salesforce-create `http:request` | `process-api` | "Writing order to Salesforce" |
| `process-api.xml` | `process-order-flow` | after the `jms:publish` to `synthesis-jobs` | `process-api` | "Queued synthesis job" |
| `salesforce-system-api.xml` | `sf-create-order-flow` | before the Salesforce `POST` `http:request` | `salesforce-system-api` | "Salesforce: creating Synthesis_Order__c record" |
| `process-api.xml` | `telemetry-consumer-flow` | right after `#[read(payload, 'application/json')]` | `process-api` | "Received instrument telemetry: `<event>` (`<progressPct>`%)" |
| `process-api.xml` | `telemetry-consumer-flow` | before the SOQL `http:request` | `process-api` | "Salesforce: looking up order by batch" |
| `process-api.xml` | `telemetry-consumer-flow` | before the PATCH `http:request` (inside the `isForwardMove` branch) | `process-api` | "Salesforce: updating order status to `<finalStatus>`" |

## Browser-side STOMP client

A minimal, dependency-free client in `dashboard/app.js` (or a small
sibling file if it grows large enough to warrant separating from the
polling/rendering logic already there — a call to make during
implementation based on actual line count). Responsibilities:

- Open a `WebSocket` to `ws://localhost:61613/stomp`.
- On open, send a `CONNECT` frame with `login`/`passcode` (see "Accepted
  tradeoff" below) and `accept-version:1.2`.
- On the `CONNECTED` frame, send a `SUBSCRIBE` frame for
  `pipeline-activity`.
- Parse incoming frames: split on the first `\n\n` for headers vs. body,
  strip the trailing NUL. Only `MESSAGE` frames are meaningful for this
  feature; anything else is ignored.
- On `close` or `error`, wait 3 seconds (matching the dashboard's existing
  poll interval, for one consistent cadence in the file) and reconnect,
  indefinitely (matching `reconnect_attempts_max=-1`'s intent in the
  Python simulator — a demo left running shouldn't silently go quiet
  because the WebSocket dropped once).
- Each parsed `MESSAGE` body (JSON, per the schema above) is pushed into
  the same `eventLog` array and re-rendered through the existing
  `renderEventLog`, prefixed distinctly enough (e.g. the `source` tag) that
  a viewer can tell a live backend event apart from a poll-diff-derived one
  without it looking like two disconnected features bolted together.

## Error handling

- **Publish side (Mule):** `on-error-continue` on every activity publish,
  as shown above. The activity feed is observability, not the product;
  it must never be capable of failing an order.
- **Subscribe side (browser):** connection failures and drops trigger
  reconnect, not a visible error state — the rest of the dashboard
  (poll-based order cards, feasibility breakdown) keeps working
  independently of whether the live feed is currently connected.

## Testing

- **Mule side:** no new MUnit coverage planned beyond what already exists
  for the flows being touched — the activity-publish additions are
  side-effecting, order-independent, and wrapped to never affect the
  flow's existing tested behavior; the MUnit suites for
  `process-order-flow` etc. don't need new assertions about a queue this
  project's test environment can't execute against anyway (see the
  project's standing MUnit-execution limitation).
- **Browser side:** the STOMP frame parser and frame builders (`CONNECT`,
  `SUBSCRIBE`, parsing a raw frame string into `{command, headers, body}`)
  are pure functions, unit tested the same way `dashboard/app.js`'s
  existing rendering functions are (`node --test`). The WebSocket
  connect/reconnect lifecycle itself is not unit tested (no headless
  browser in this environment to exercise a real `WebSocket`) — verified
  by hand against a live stack instead, the same limitation already noted
  for the dashboard redesign.
- **End-to-end:** submit a real order against the real stack, confirm the
  activity feed shows live entries appearing within roughly a second of
  each backend call, ahead of the next 3-second poll cycle.

## Accepted tradeoff: broker credentials in browser source

The STOMP `CONNECT` frame's `login`/`passcode` (`admin`/`admin`) will be
visible in `app.js`'s source to anyone who opens dev tools — a static-file
dashboard has no server-side place to hold a secret. This is judged
acceptable because:

- These are the same default credentials already committed in
  `.env.example`.
- This is a local demo app, not a production system with real tenants or
  real secrets behind it.
- The alternative (a server-side proxy just to hide a demo credential)
  adds a real component for no real security benefit at this project's
  scope.

Called out explicitly here so it's a documented decision, not a silent gap.
