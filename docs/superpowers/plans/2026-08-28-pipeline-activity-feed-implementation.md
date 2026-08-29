# Live Pipeline Activity Feed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mule flows publish short activity events (what backend call is happening, for which order) to a new broadcast topic; the dashboard subscribes over STOMP-over-WebSocket and shows them live in the existing Pipeline Activity feed.

**Architecture:** Every business-significant `http:request`/Salesforce call in the order pipeline gets one additional fire-and-forget `<jms:publish>` to a `pipeline-activity` topic, wrapped in `<try>`/`on-error-continue` so a publish failure can never break real order processing. The browser connects directly to the existing ActiveMQ port (61613, already exposed) with a small hand-rolled STOMP client and merges live events into the same feed the dashboard redesign already built.

**Tech Stack:** Mule 4.6.0 (`<jms:publish>`), Apache ActiveMQ Artemis (existing broker, no config changes), vanilla browser JavaScript (`WebSocket`, no dependencies).

**Spec:** `docs/superpowers/specs/2026-08-27-pipeline-activity-feed-design.md`

## Global Constraints

- Destination name is `pipeline-activity`, published with `destinationType="TOPIC"` on every `<jms:publish>` (a MULTICAST/broadcast destination, not a work queue — multiple open dashboard tabs must all see every event, not compete for them).
- Every activity `<jms:publish>` is wrapped in `<try>...<error-handler><on-error-continue type="ANY"/></error-handler></try>`. No exceptions — a broker hiccup on this path must never fail an order.
- No changes to `activemq/broker.xml` or `docker-compose.yml`. The existing plain `stomp` acceptor and the existing `61613:61613` port mapping already support WebSocket clients (verified empirically before this plan was written — see the spec's "Validated premises").
- No new MUnit coverage. This is a deliberate scope decision in the spec's Testing section, not an oversight — the activity-publish additions are side-effecting and wrapped to never affect a flow's existing tested behavior, and this project's MUnit suites don't execute in this environment regardless (standing limitation, see the original implementation plan's Task 5). Each Mule task's acceptance bar is the project's existing structural bar instead: no `--` inside an XML comment (checked with the project's `check_comments.py` convention) and a clean `mvn clean package -DskipMunitTests`.
- **Use `~/tools/apache-maven-3.9.6/bin/mvn` for every Mule build/test command, never the system `mvn` (3.6.3, incompatible with `mule-maven-plugin` 4.10.1 — fails with an unrelated Aether `RemoteRepository$Builder.setBlocked` `NoSuchMethodError` that has nothing to do with any code change).** This was already documented in the original implementation plan's Global Constraints; restated here because Task 3 was dispatched without it and hit exactly this failure before the ledger caught it.
- Browser-side code stays dependency-free — no CDN scripts, no build step, no npm package. This dashboard has none today and this feature doesn't introduce the first one.
- Every activity event's JSON body has exactly these four fields: `timestamp` (ISO 8601 string, via `now() as String`), `batchId` (string or `null`), `source` (one of `experience-api`, `process-api`, `salesforce-system-api`), `message` (a human-readable string).
- The browser reconnects 3 seconds after any WebSocket close or error, indefinitely — matching the resilience pattern already used in `services/instrument-simulator/simulator.py` for the same broker.

---

### Task 1: STOMP frame parser and frame builders

**Files:**
- Create: `dashboard/stomp-client.js`
- Test: `dashboard/tests/stomp-client.test.js`

**Interfaces:**
- Produces: `buildConnectFrame({login, passcode})` → string. `buildSubscribeFrame({destination, id})` → string. `parseFrame(raw)` → `{command, headers, body}`. All three are plain global functions when loaded via `<script>` (no module wrapper in the browser), and exported via `module.exports` for `node --test` the same way `dashboard/app.js` already does. Consumed by Task 2.

- [ ] **Step 1: Write the failing tests**

`dashboard/tests/stomp-client.test.js`:

```js
const test = require('node:test');
const assert = require('node:assert');
const { buildConnectFrame, buildSubscribeFrame, parseFrame } = require('../stomp-client.js');

test('buildConnectFrame includes version, login, and passcode headers and ends with the frame terminator', () => {
  const frame = buildConnectFrame({ login: 'admin', passcode: 'admin' });
  assert.match(frame, /^CONNECT\n/);
  assert.match(frame, /accept-version:1\.2/);
  assert.match(frame, /login:admin/);
  assert.match(frame, /passcode:admin/);
  assert.ok(frame.endsWith('\0'));
});

test('buildSubscribeFrame includes destination, id, and ack headers', () => {
  const frame = buildSubscribeFrame({ destination: 'pipeline-activity', id: 'sub-1' });
  assert.match(frame, /^SUBSCRIBE\n/);
  assert.match(frame, /destination:pipeline-activity/);
  assert.match(frame, /id:sub-1/);
  assert.match(frame, /ack:auto/);
  assert.ok(frame.endsWith('\0'));
});

test('parseFrame parses a CONNECTED frame with no body', () => {
  const raw = 'CONNECTED\nversion:1.2\nsession:abc123\nserver:ActiveMQ-Artemis/2.37.0\n\n\0\n';
  const frame = parseFrame(raw);
  assert.strictEqual(frame.command, 'CONNECTED');
  assert.strictEqual(frame.headers.version, '1.2');
  assert.strictEqual(frame.headers.session, 'abc123');
  assert.strictEqual(frame.body, '');
});

test('parseFrame parses a MESSAGE frame and its JSON body', () => {
  const raw = 'MESSAGE\nsubscription:sub-1\ndestination:pipeline-activity\ncontent-length:19\n\n{"a":1,"b":"two"}\0\n';
  const frame = parseFrame(raw);
  assert.strictEqual(frame.command, 'MESSAGE');
  assert.strictEqual(frame.headers.destination, 'pipeline-activity');
  assert.deepStrictEqual(JSON.parse(frame.body), { a: 1, b: 'two' });
});

test('parseFrame handles a frame terminator with no trailing newline', () => {
  const raw = 'MESSAGE\ndestination:x\n\nbody-text\0';
  const frame = parseFrame(raw);
  assert.strictEqual(frame.body, 'body-text');
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd dashboard && node --test tests/stomp-client.test.js`
Expected: FAIL — `Cannot find module '../stomp-client.js'`

- [ ] **Step 3: Write the implementation**

`dashboard/stomp-client.js`:

```js
const FRAME_TERMINATOR = '\0';

function buildConnectFrame({ login, passcode }) {
  return `CONNECT\naccept-version:1.2\nhost:/\nlogin:${login}\npasscode:${passcode}\n\n${FRAME_TERMINATOR}`;
}

function buildSubscribeFrame({ destination, id }) {
  return `SUBSCRIBE\nid:${id}\ndestination:${destination}\nack:auto\n\n${FRAME_TERMINATOR}`;
}

// Artemis's STOMP-over-WebSocket frames end with the STOMP frame
// terminator (NUL) optionally followed by a trailing newline -- both
// forms have been observed against this broker, so both are stripped
// before parsing headers/body.
function parseFrame(raw) {
  const withoutTerminator = raw.replace(/\0\n?$/, '');
  const separatorIndex = withoutTerminator.indexOf('\n\n');
  const head = separatorIndex === -1 ? withoutTerminator : withoutTerminator.slice(0, separatorIndex);
  const body = separatorIndex === -1 ? '' : withoutTerminator.slice(separatorIndex + 2);
  const lines = head.split('\n');
  const command = lines[0];
  const headers = {};
  for (const line of lines.slice(1)) {
    const colonIndex = line.indexOf(':');
    if (colonIndex === -1) continue;
    headers[line.slice(0, colonIndex)] = line.slice(colonIndex + 1);
  }
  return { command, headers, body };
}

if (typeof module !== 'undefined') {
  module.exports = { buildConnectFrame, buildSubscribeFrame, parseFrame, FRAME_TERMINATOR };
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd dashboard && node --test tests/stomp-client.test.js`
Expected: PASS, 5 tests

- [ ] **Step 5: Commit**

```bash
git add dashboard/stomp-client.js dashboard/tests/stomp-client.test.js
git commit -m "Add a minimal dependency-free STOMP frame parser/builder"
```

---

### Task 2: Live activity stream in the dashboard

**Files:**
- Modify: `dashboard/app.js`
- Modify: `dashboard/index.html`
- Modify: `dashboard/tests/app.test.js`

**Interfaces:**
- Consumes: `buildConnectFrame`, `buildSubscribeFrame`, `parseFrame` (Task 1, loaded as browser globals via a `<script>` tag, not `require`d — `connectActivityStream` only ever runs in the browser branch, never during `node --test`, so no bridging is needed for tests to pass).
- Produces: `describeActivityEvent(event)` → `{time, text, live}`, a pure function other future work can reuse. `connectActivityStream()` is the browser-only entry point, called once at the bottom of `app.js` alongside the existing `refresh()`/`setInterval` calls.

- [ ] **Step 1: Write the failing test for the pure event-description function**

Add to `dashboard/tests/app.test.js` (alongside the existing `require` line, add `describeActivityEvent` to the destructured imports):

```js
const {
  statusBadgeClass,
  stageIndex,
  renderStageTracker,
  renderProgressBar,
  daysUntil,
  renderDeadlineBadge,
  describeTransition,
  renderEventLog,
  formatOrderCard,
  describeActivityEvent,
} = require('../app.js');
```

Then add these tests at the end of the file:

```js
test('describeActivityEvent formats a live backend event with its source tag', () => {
  const result = describeActivityEvent({
    timestamp: '2026-08-28T10:00:00.000Z',
    batchId: 'batch-1',
    source: 'process-api',
    message: 'Requesting feasibility score',
  });
  assert.match(result.text, /\[process-api\]/);
  assert.match(result.text, /Requesting feasibility score/);
  assert.strictEqual(result.live, true);
});

test('renderEventLog marks live entries with the event-live class', () => {
  const html = renderEventLog([{ time: '10:00:00 AM', text: '[process-api] test', live: true }]);
  assert.match(html, /class="event-live"/);
});

test('renderEventLog does not mark poll-diff entries as live', () => {
  const html = renderEventLog([{ time: '10:00:00 AM', text: 'ORD-1 shipped' }]);
  assert.doesNotMatch(html, /event-live/);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd dashboard && node --test tests/app.test.js`
Expected: FAIL — `describeActivityEvent` is not a function

- [ ] **Step 3: Update `renderEventLog` to render the live-entry class**

In `dashboard/app.js`, replace the existing `renderEventLog` function:

```js
function renderEventLog(entries) {
  if (entries.length === 0) {
    return '<li class="event-empty">No activity yet.</li>';
  }
  return entries.map((e) => `<li class="${e.live ? 'event-live' : ''}"><span class="event-time">${e.time}</span> ${e.text}</li>`).join('');
}
```

- [ ] **Step 4: Add `describeActivityEvent` and the connection lifecycle**

In `dashboard/app.js`, add near the top (after the existing `const MAX_EVENTS = 50;` line):

```js
const ACTIVITY_STREAM_URL = 'ws://localhost:61613/stomp';
const ACTIVITY_STREAM_LOGIN = 'admin';
const ACTIVITY_STREAM_PASSCODE = 'admin';
const ACTIVITY_STREAM_DESTINATION = 'pipeline-activity';
const RECONNECT_DELAY_MS = 3000;
```

Add this pure function near `describeTransition` (both describe something for the event feed, so they belong together):

```js
// Turns one activity event published by a Mule flow into the same
// {time, text} shape describeTransition/renderEventLog already use, with
// a `live` flag so renderEventLog can style it distinctly from a
// poll-diff-derived entry.
function describeActivityEvent(event) {
  const time = new Date(event.timestamp).toLocaleTimeString();
  return { time, text: `[${event.source}] ${event.message}`, live: true };
}
```

Add this browser-only function near the bottom, just above the `if (typeof module !== 'undefined')` block:

```js
// Connects to the broker directly from the browser and merges live
// backend-activity events into the same eventLog the poll-diff events
// already populate. Not unit tested -- it drives a real WebSocket, which
// this environment has no headless browser to exercise; verified by hand
// against a live stack instead (see the plan's Task 7).
function connectActivityStream() {
  const ws = new WebSocket(ACTIVITY_STREAM_URL);

  ws.addEventListener('open', () => {
    ws.send(buildConnectFrame({ login: ACTIVITY_STREAM_LOGIN, passcode: ACTIVITY_STREAM_PASSCODE }));
  });

  ws.addEventListener('message', async (event) => {
    const raw = event.data instanceof Blob ? await event.data.text() : event.data;
    const frame = parseFrame(raw);
    if (frame.command === 'CONNECTED') {
      ws.send(buildSubscribeFrame({ destination: ACTIVITY_STREAM_DESTINATION, id: 'dashboard-activity' }));
    } else if (frame.command === 'MESSAGE') {
      try {
        const activityEvent = JSON.parse(frame.body);
        eventLog = [describeActivityEvent(activityEvent), ...eventLog].slice(0, MAX_EVENTS);
        document.querySelector('#event-feed-list').innerHTML = renderEventLog(eventLog);
      } catch (err) {
        console.error('failed to parse activity event:', err);
      }
    }
  });

  ws.addEventListener('close', () => {
    setTimeout(connectActivityStream, RECONNECT_DELAY_MS);
  });

  ws.addEventListener('error', () => {
    ws.close();
  });
}
```

Update the `module.exports` list to include `describeActivityEvent`:

```js
if (typeof module !== 'undefined') {
  module.exports = {
    statusBadgeClass,
    stageIndex,
    renderStageTracker,
    renderProgressBar,
    daysUntil,
    renderDeadlineBadge,
    describeTransition,
    renderEventLog,
    formatOrderCard,
    describeActivityEvent,
  };
} else {
  document.querySelector('#order-form').addEventListener('submit', submitOrder);
  refresh();
  setInterval(refresh, 3000);
  connectActivityStream();
}
```

- [ ] **Step 5: Load `stomp-client.js` before `app.js` in the page**

In `dashboard/index.html`, change:

```html
  <script src="app.js"></script>
```

to:

```html
  <script src="stomp-client.js"></script>
  <script src="app.js"></script>
```

- [ ] **Step 6: Add the `.event-live` style**

In `dashboard/styles.css`, add near the existing `.event-time`/`.event-empty` rules:

```css
.event-live { border-left: 2px solid var(--blue); padding-left: 0.5rem; }
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd dashboard && node --test tests/app.test.js tests/stomp-client.test.js`
Expected: PASS, 30 tests total (22 existing in `app.test.js` + 3 new in
`app.test.js` from this task's Step 1 + 5 from Task 1's
`stomp-client.test.js`)

- [ ] **Step 8: Commit**

```bash
git add dashboard/app.js dashboard/index.html dashboard/styles.css dashboard/tests/app.test.js
git commit -m "Wire a live activity stream into the dashboard's pipeline activity feed"
```

---

### Task 3: Instrument the Experience API

**Files:**
- Modify: `mule-app/src/main/mule/experience-api.xml`

**Interfaces:**
- Produces: one activity event per order submission, published to `pipeline-activity`.

- [ ] **Step 1: Add the `jms` namespace**

In `mule-app/src/main/mule/experience-api.xml`, replace the root `<mule>` opening tag:

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd">
```

with:

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:jms="http://www.mulesoft.org/schema/mule/jms"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/jms http://www.mulesoft.org/schema/mule/jms/current/mule-jms.xsd">
```

- [ ] **Step 2: Add the activity publish**

In the same file, inside `experience-submit-order-flow`, replace:

```xml
        <choice>
            <when expression="#[not (isEmpty(payload.sequence default '')) and (payload.sequence matches /^[ACGTN]+$/)]">
                <http:request config-ref="Process_API_Request_Config" method="POST" path="/orders">
                    <http:body><![CDATA[#[payload]]]></http:body>
                </http:request>
            </when>
```

with:

```xml
        <choice>
            <when expression="#[not (isEmpty(payload.sequence default '')) and (payload.sequence matches /^[ACGTN]+$/)]">
                <try>
                    <jms:publish config-ref="JMS_Config" destination="pipeline-activity" destinationType="TOPIC">
                        <jms:message>
                            <jms:body><![CDATA[#[output application/json --- { timestamp: now() as String, batchId: null, source: "experience-api", message: "Order submission received" }]]]></jms:body>
                        </jms:message>
                    </jms:publish>
                    <error-handler>
                        <on-error-continue type="ANY"/>
                    </error-handler>
                </try>
                <http:request config-ref="Process_API_Request_Config" method="POST" path="/orders">
                    <http:body><![CDATA[#[payload]]]></http:body>
                </http:request>
            </when>
```

- [ ] **Step 3: Check for illegal `--` inside XML comments**

A plain `grep -- '--'` is not the right check here — it would false-positive
on every DataWeave `---` header separator (`output application/json\n---\n`),
which appears throughout these files outside any comment. The actual check
only looks for `--` inside `<!-- -->` blocks:

```bash
python3 -c "import re,sys; content=open(sys.argv[1]).read(); bad=[m.group(1) for m in re.finditer(r'<!--(.*?)-->', content, re.DOTALL) if '--' in m.group(1)]; print('FOUND: ' + repr(bad[0][:100]) if bad else 'clean')" mule-app/src/main/mule/experience-api.xml
```

Expected: `clean` (this file's existing comments were already clean; this
step only matters if a future edit adds a new comment)

- [ ] **Step 4: Verify the build**

Run: `cd mule-app && ~/tools/apache-maven-3.9.6/bin/mvn clean package -DskipMunitTests` (native Maven — see Global Constraints; the system `mvn` fails on an unrelated Aether incompatibility)
Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add mule-app/src/main/mule/experience-api.xml
git commit -m "Publish a pipeline-activity event when an order submission is received"
```

---

### Task 4: Instrument process-order-flow

**Files:**
- Modify: `mule-app/src/main/mule/process-api.xml`

**Interfaces:**
- Consumes: `vars.batchId` (set at the top of `process-order-flow` before any of this task's insertion points).
- Produces: three activity events per feasible order submission (feasibility requested, Salesforce write started, job queued); two for a rejected one (feasibility requested, Salesforce write started — no queue event, since infeasible orders never reach the `jms:publish` to `synthesis-jobs`).

`process-api.xml` already declares `xmlns:jms` — no namespace change needed in this task.

- [ ] **Step 1: Add the activity publish before the feasibility call**

In `mule-app/src/main/mule/process-api.xml`, inside `process-order-flow`, replace:

```xml
        <set-variable variableName="orderPayload" value="#[payload]"/>
        <set-variable variableName="batchId" value="#[uuid()]"/>

        <http:request config-ref="Feasibility_System_API_Request_Config" method="POST" path="/feasibility">
```

with:

```xml
        <set-variable variableName="orderPayload" value="#[payload]"/>
        <set-variable variableName="batchId" value="#[uuid()]"/>

        <try>
            <jms:publish config-ref="JMS_Config" destination="pipeline-activity" destinationType="TOPIC">
                <jms:message>
                    <jms:body><![CDATA[#[output application/json --- { timestamp: now() as String, batchId: vars.batchId, source: "process-api", message: "Requesting feasibility score" }]]]></jms:body>
                </jms:message>
            </jms:publish>
            <error-handler>
                <on-error-continue type="ANY"/>
            </error-handler>
        </try>

        <http:request config-ref="Feasibility_System_API_Request_Config" method="POST" path="/feasibility">
```

- [ ] **Step 2: Add the activity publish before the Salesforce-create call**

In the same file, replace:

```xml
    batchId: vars.batchId
}]'/>
                <http:request config-ref="SF_System_API_Request_Config" method="POST" path="/synthesis-orders">
                    <http:body><![CDATA[#[payload]]]></http:body>
                </http:request>
                <set-variable variableName="orderId" value="#[payload.id]"/>

                <jms:publish config-ref="JMS_Config" destination="synthesis-jobs">
```

with:

```xml
    batchId: vars.batchId
}]'/>
                <try>
                    <jms:publish config-ref="JMS_Config" destination="pipeline-activity" destinationType="TOPIC">
                        <jms:message>
                            <jms:body><![CDATA[#[output application/json --- { timestamp: now() as String, batchId: vars.batchId, source: "process-api", message: "Writing order to Salesforce" }]]]></jms:body>
                        </jms:message>
                    </jms:publish>
                    <error-handler>
                        <on-error-continue type="ANY"/>
                    </error-handler>
                </try>
                <http:request config-ref="SF_System_API_Request_Config" method="POST" path="/synthesis-orders">
                    <http:body><![CDATA[#[payload]]]></http:body>
                </http:request>
                <set-variable variableName="orderId" value="#[payload.id]"/>

                <jms:publish config-ref="JMS_Config" destination="synthesis-jobs">
```

**Note for the implementer:** this replace pattern (`batchId: vars.batchId\n}]'/>\n                <http:request config-ref="SF_System_API_Request_Config" method="POST" path="/synthesis-orders">`) also matches inside the `<otherwise>` branch below (the "Rejected" transform ends the same way). Only apply this specific edit to the **first** occurrence (inside `<when expression="#[vars.feasibilityResult.feasible]">`) — the `<otherwise>` branch does not get a "Writing order to Salesforce" event in this task; see Step 3.

- [ ] **Step 3: Add the activity publish after the queue publish**

In the same file, replace:

```xml
                <jms:publish config-ref="JMS_Config" destination="synthesis-jobs">
                    <jms:message>
                        <jms:body><![CDATA[#[output application/json --- { batchId: vars.batchId, orderId: vars.orderId, requestedShipDate: vars.orderPayload.requestedShipDate }]]]></jms:body>
                    </jms:message>
                </jms:publish>

                <set-payload value='#[%dw 2.0
output application/json
---
{
    orderId: vars.orderId,
```

with:

```xml
                <jms:publish config-ref="JMS_Config" destination="synthesis-jobs">
                    <jms:message>
                        <jms:body><![CDATA[#[output application/json --- { batchId: vars.batchId, orderId: vars.orderId, requestedShipDate: vars.orderPayload.requestedShipDate }]]]></jms:body>
                    </jms:message>
                </jms:publish>

                <try>
                    <jms:publish config-ref="JMS_Config" destination="pipeline-activity" destinationType="TOPIC">
                        <jms:message>
                            <jms:body><![CDATA[#[output application/json --- { timestamp: now() as String, batchId: vars.batchId, source: "process-api", message: "Queued synthesis job" }]]]></jms:body>
                        </jms:message>
                    </jms:publish>
                    <error-handler>
                        <on-error-continue type="ANY"/>
                    </error-handler>
                </try>

                <set-payload value='#[%dw 2.0
output application/json
---
{
    orderId: vars.orderId,
```

- [ ] **Step 4: Check for illegal `--` inside XML comments**

```bash
python3 -c "import re,sys; content=open(sys.argv[1]).read(); bad=[m.group(1) for m in re.finditer(r'<!--(.*?)-->', content, re.DOTALL) if '--' in m.group(1)]; print('FOUND: ' + repr(bad[0][:100]) if bad else 'clean')" mule-app/src/main/mule/process-api.xml
```

Expected: `clean`

- [ ] **Step 5: Verify the build**

Run: `cd mule-app && ~/tools/apache-maven-3.9.6/bin/mvn clean package -DskipMunitTests` (native Maven — see Global Constraints; the system `mvn` fails on an unrelated Aether incompatibility)
Expected: `BUILD SUCCESS`

- [ ] **Step 6: Commit**

```bash
git add mule-app/src/main/mule/process-api.xml
git commit -m "Publish pipeline-activity events from process-order-flow"
```

---

### Task 5: Instrument the Salesforce System API's create flow

**Files:**
- Modify: `mule-app/src/main/mule/salesforce-system-api.xml`

**Interfaces:**
- Consumes: `vars.recordPayload` (already set at the top of `sf-create-order-flow`; its `Batch_Id__c` field is this task's `batchId`).
- Produces: one activity event per Salesforce create call.

- [ ] **Step 1: Add the `jms` namespace**

In `mule-app/src/main/mule/salesforce-system-api.xml`, replace the root `<mule>` opening tag:

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd">
```

with:

```xml
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
      xmlns:http="http://www.mulesoft.org/schema/mule/http"
      xmlns:jms="http://www.mulesoft.org/schema/mule/jms"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="
        http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/current/mule.xsd
        http://www.mulesoft.org/schema/mule/http http://www.mulesoft.org/schema/mule/http/current/mule-http.xsd
        http://www.mulesoft.org/schema/mule/jms http://www.mulesoft.org/schema/mule/jms/current/mule-jms.xsd">
```

- [ ] **Step 2: Add the activity publish**

In the same file, inside `sf-create-order-flow`, replace:

```xml
        <set-variable variableName="recordPayload" value="#[payload]"/>

        <http:request config-ref="SFDC_Auth_Request_Config" method="GET" path="/token"/>
        <set-variable variableName="accessToken" value="#[payload.access_token]"/>

        <http:request config-ref="SFDC_Request_Config" method="POST" path="/services/data/v60.0/sobjects/Synthesis_Order__c">
```

with:

```xml
        <set-variable variableName="recordPayload" value="#[payload]"/>

        <http:request config-ref="SFDC_Auth_Request_Config" method="GET" path="/token"/>
        <set-variable variableName="accessToken" value="#[payload.access_token]"/>

        <try>
            <jms:publish config-ref="JMS_Config" destination="pipeline-activity" destinationType="TOPIC">
                <jms:message>
                    <jms:body><![CDATA[#[output application/json --- { timestamp: now() as String, batchId: vars.recordPayload.Batch_Id__c, source: "salesforce-system-api", message: "Salesforce: creating Synthesis_Order__c record" }]]]></jms:body>
                </jms:message>
            </jms:publish>
            <error-handler>
                <on-error-continue type="ANY"/>
            </error-handler>
        </try>

        <http:request config-ref="SFDC_Request_Config" method="POST" path="/services/data/v60.0/sobjects/Synthesis_Order__c">
```

- [ ] **Step 3: Check for illegal `--` inside XML comments**

```bash
python3 -c "import re,sys; content=open(sys.argv[1]).read(); bad=[m.group(1) for m in re.finditer(r'<!--(.*?)-->', content, re.DOTALL) if '--' in m.group(1)]; print('FOUND: ' + repr(bad[0][:100]) if bad else 'clean')" mule-app/src/main/mule/salesforce-system-api.xml
```

Expected: `clean`

- [ ] **Step 4: Verify the build**

Run: `cd mule-app && ~/tools/apache-maven-3.9.6/bin/mvn clean package -DskipMunitTests` (native Maven — see Global Constraints; the system `mvn` fails on an unrelated Aether incompatibility)
Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add mule-app/src/main/mule/salesforce-system-api.xml
git commit -m "Publish a pipeline-activity event from sf-create-order-flow"
```

---

### Task 6: Instrument telemetry-consumer-flow

**Files:**
- Modify: `mule-app/src/main/mule/process-api.xml`

**Interfaces:**
- Consumes: `vars.telemetry.batchId`/`.event`/`.progressPct` (set at the top of the flow), `vars.finalStatus` (set inside the `isForwardMove` branch, right before this task's third insertion point).
- Produces: one activity event on every telemetry receipt, one before every Salesforce lookup, one before every Salesforce status update (only reached when the event represents forward progress — a stale/out-of-order event that falls into the `<otherwise>` branch produces the first two events but not the third).

- [ ] **Step 1: Add the activity publish on telemetry receipt**

In `mule-app/src/main/mule/process-api.xml`, inside `telemetry-consumer-flow`, replace:

```xml
        <set-variable variableName="telemetry" value="#[read(payload, 'application/json')]"/>

        <!--
          This used to call back into this app's own SF System API
          (GET/PATCH SF_System_API_Request_Config's "/synthesis-orders/
          {batchId}") to look up and update the order by batch. Found
          running Task 12's end-to-end demo: that self-referencing HTTP
          hop (this flow, over HTTP, back into a listener this SAME
          Mule instance is also serving on the same port) consistently
          returned sf-get-orders-flow's full-list response instead of
          sf-get-order-by-batch-flow's single-record one, no matter how
          correct the interpolated path was (confirmed correct via a
          logger right before the call, and confirmed the identical URL
          worked correctly over a real external curl to the same port).
          Never tracked down which layer picks the wrong listener for a
          same-process loopback call; querying Salesforce directly here
          (the same SOQL/PATCH pattern salesforce-system-api.xml's
          sf-get-order-by-batch-flow already uses) sidesteps it rather
          than chasing it further, and cuts an unnecessary hop besides.
        -->
        <http:request config-ref="SFDC_Auth_Request_Config" method="GET" path="/token"/>
```

with:

```xml
        <set-variable variableName="telemetry" value="#[read(payload, 'application/json')]"/>

        <try>
            <jms:publish config-ref="JMS_Config" destination="pipeline-activity" destinationType="TOPIC">
                <jms:message>
                    <jms:body><![CDATA[#[output application/json --- { timestamp: now() as String, batchId: vars.telemetry.batchId, source: "process-api", message: "Received instrument telemetry: " ++ vars.telemetry.event ++ " (" ++ (vars.telemetry.progressPct as String) ++ "%)" }]]]></jms:body>
                </jms:message>
            </jms:publish>
            <error-handler>
                <on-error-continue type="ANY"/>
            </error-handler>
        </try>

        <!--
          This used to call back into this app's own SF System API
          (GET/PATCH SF_System_API_Request_Config's "/synthesis-orders/
          {batchId}") to look up and update the order by batch. Found
          running Task 12's end-to-end demo: that self-referencing HTTP
          hop (this flow, over HTTP, back into a listener this SAME
          Mule instance is also serving on the same port) consistently
          returned sf-get-orders-flow's full-list response instead of
          sf-get-order-by-batch-flow's single-record one, no matter how
          correct the interpolated path was (confirmed correct via a
          logger right before the call, and confirmed the identical URL
          worked correctly over a real external curl to the same port).
          Never tracked down which layer picks the wrong listener for a
          same-process loopback call; querying Salesforce directly here
          (the same SOQL/PATCH pattern salesforce-system-api.xml's
          sf-get-order-by-batch-flow already uses) sidesteps it rather
          than chasing it further, and cuts an unnecessary hop besides.
        -->
        <http:request config-ref="SFDC_Auth_Request_Config" method="GET" path="/token"/>
```

- [ ] **Step 2: Add the activity publish before the Salesforce lookup**

In the same file, replace:

```xml
"SELECT Id, Status__c, Promised_Ship_Date__c FROM Synthesis_Order__c WHERE Batch_Id__c = &apos;" ++ safeBatchId ++ "&apos;"]'/>

        <http:request config-ref="SFDC_Request_Config" method="GET" path="/services/data/v60.0/query">
```

with:

```xml
"SELECT Id, Status__c, Promised_Ship_Date__c FROM Synthesis_Order__c WHERE Batch_Id__c = &apos;" ++ safeBatchId ++ "&apos;"]'/>

        <try>
            <jms:publish config-ref="JMS_Config" destination="pipeline-activity" destinationType="TOPIC">
                <jms:message>
                    <jms:body><![CDATA[#[output application/json --- { timestamp: now() as String, batchId: vars.telemetry.batchId, source: "process-api", message: "Salesforce: looking up order by batch" }]]]></jms:body>
                </jms:message>
            </jms:publish>
            <error-handler>
                <on-error-continue type="ANY"/>
            </error-handler>
        </try>

        <http:request config-ref="SFDC_Request_Config" method="GET" path="/services/data/v60.0/query">
```

**Note for the implementer:** `"SELECT Id, Status__c, Promised_Ship_Date__c FROM ..."` and the `http:request` that follows it appear exactly once in this file — this replace is unambiguous, unlike Task 4's Step 2.

- [ ] **Step 3: Add the activity publish before the Salesforce status update**

In the same file, replace:

```xml
                        <set-variable variableName="finalStatus"
                            value="#[if (vars.projectedLate) 'At_Risk' else vars.candidateStatus]"/>
                        <set-payload value='#[%dw 2.0
output application/json
---
{ Status__c: vars.finalStatus, Progress_Pct__c: vars.telemetry.progressPct }]'/>
                        <http:request config-ref="SFDC_Request_Config" method="PATCH" path="#['/services/data/v60.0/sobjects/Synthesis_Order__c/' ++ vars.currentOrder.Id]">
```

with:

```xml
                        <set-variable variableName="finalStatus"
                            value="#[if (vars.projectedLate) 'At_Risk' else vars.candidateStatus]"/>
                        <try>
                            <jms:publish config-ref="JMS_Config" destination="pipeline-activity" destinationType="TOPIC">
                                <jms:message>
                                    <jms:body><![CDATA[#[output application/json --- { timestamp: now() as String, batchId: vars.telemetry.batchId, source: "process-api", message: "Salesforce: updating order status to " ++ vars.finalStatus }]]]></jms:body>
                                </jms:message>
                            </jms:publish>
                            <error-handler>
                                <on-error-continue type="ANY"/>
                            </error-handler>
                        </try>
                        <set-payload value='#[%dw 2.0
output application/json
---
{ Status__c: vars.finalStatus, Progress_Pct__c: vars.telemetry.progressPct }]'/>
                        <http:request config-ref="SFDC_Request_Config" method="PATCH" path="#['/services/data/v60.0/sobjects/Synthesis_Order__c/' ++ vars.currentOrder.Id]">
```

- [ ] **Step 4: Check for illegal `--` inside XML comments**

```bash
python3 -c "import re,sys; content=open(sys.argv[1]).read(); bad=[m.group(1) for m in re.finditer(r'<!--(.*?)-->', content, re.DOTALL) if '--' in m.group(1)]; print('FOUND: ' + repr(bad[0][:100]) if bad else 'clean')" mule-app/src/main/mule/process-api.xml
```

Expected: `clean`

- [ ] **Step 5: Verify the build**

Run: `cd mule-app && ~/tools/apache-maven-3.9.6/bin/mvn clean package -DskipMunitTests` (native Maven — see Global Constraints; the system `mvn` fails on an unrelated Aether incompatibility)
Expected: `BUILD SUCCESS`

- [ ] **Step 6: Commit**

```bash
git add mule-app/src/main/mule/process-api.xml
git commit -m "Publish pipeline-activity events from telemetry-consumer-flow"
```

---

### Task 7: End-to-end verification and docs

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: every prior task — this is the first point all eight publish points and the browser subscriber exist together.

- [ ] **Step 1: Bring the full stack up cold**

```bash
docker compose down
docker compose up -d --build
```

Wait for `mule-app` to report `DEPLOYED`:

```bash
until docker compose logs mule-app 2>&1 | grep -q DEPLOYED; do sleep 2; done
docker compose logs mule-app | grep -E "DEPLOYED|ERROR"
```

Expected: `DEPLOYED` for both the domain and the application, no `ERROR` lines.

- [ ] **Step 2: Submit a real order and watch the activity feed live**

Open `http://localhost:8090` in an actual browser (this environment has no
headless browser to do this automatically — this step needs a human, or an
agent with real browser access, to actually look at the page). Submit an
order with a real Salesforce Account Id and a feasible sequence (e.g.
`ACGTACGTACGTACGTACGTACGTACG`, which scores 0.90 with one flagged repeat
region — feasible, and exercises the "Rejected" path is NOT taken).

Expected, in order, within roughly a second of each other (not waiting for
the next 3-second poll):
1. `[experience-api] Order submission received`
2. `[process-api] Requesting feasibility score`
3. `[process-api] Writing order to Salesforce`
4. `[salesforce-system-api] Salesforce: creating Synthesis_Order__c record`
5. `[process-api] Queued synthesis job`

Then, as the instrument simulator reports telemetry over the next ~10
seconds, repeated for each event:
6. `[process-api] Received instrument telemetry: <event> (<pct>%)`
7. `[process-api] Salesforce: looking up order by batch`
8. `[process-api] Salesforce: updating order status to <status>` (only for
   events that represent forward progress — see Task 6's note)

- [ ] **Step 3: Confirm reconnect behavior**

With the dashboard tab still open, restart just the broker:

```bash
docker compose restart activemq
```

Expected: within a few seconds of the broker coming back, new activity
events resume appearing in the feed without a page reload (confirms the
3-second reconnect loop from Task 2 actually recovers a dropped
connection against this broker, not just in theory).

- [ ] **Step 4: Clean up the test order**

```bash
cd salesforce
sf data query --query "SELECT Id FROM Synthesis_Order__c" --target-org ansa-poc-dev --json
# delete whatever record(s) Step 2/3 created
sf data delete record --sobject Synthesis_Order__c --record-id <id> --target-org ansa-poc-dev
cd ..
```

- [ ] **Step 5: Tear down**

```bash
docker compose down
```

- [ ] **Step 6: Document the feature**

In `README.md`, add a new bullet to the "Notable runtime findings" list:

```markdown
- **A browser can speak STOMP-over-WebSocket directly to this broker's
  existing plain acceptor** — no separate WebSocket acceptor or
  `broker.xml` change needed; Artemis's Netty transport auto-detects the
  WebSocket upgrade handshake on the same port already used for plain
  TCP STOMP. The dashboard's live Pipeline Activity feed
  (`dashboard/stomp-client.js`) uses this to subscribe directly to a
  `pipeline-activity` topic Mule flows publish to, rather than polling.
```

- [ ] **Step 7: Commit**

```bash
git add README.md
git commit -m "Document the live pipeline activity feed's WebSocket transport finding"
```

---

## Self-review notes

- **Spec coverage:** every "Publish points" row in the spec has a
  corresponding task/step (Tasks 3-6). The browser client's every stated
  responsibility (connect, CONNECT/CONNECTED, SUBSCRIBE, parse MESSAGE,
  reconnect) is in Task 2. The event schema's four fields appear in every
  publish point's DataWeave body. The "Accepted tradeoff" section doesn't
  need its own task — it's a documented decision about Task 2's existing
  hardcoded credentials, not additional work.
- **Placeholder scan:** no TBD/TODO; every code block is complete,
  copy-pasteable content, not a description of what to write.
- **Type/name consistency:** `pipeline-activity` (destination name),
  `destinationType="TOPIC"`, and the four-field event schema
  (`timestamp`/`batchId`/`source`/`message`) are identical across every
  task. `describeActivityEvent`'s output shape (`{time, text, live}`)
  matches what `renderEventLog` (updated in Task 2) expects, and matches
  the shape `describeTransition`'s existing output already uses (`{time,
  text}`, `live` simply absent there) — confirmed no divergent shape
  between the two event sources feeding the same array.
