const EXPERIENCE_API_BASE = 'http://localhost:8081';
const MAX_EVENTS = 50;

const ACTIVITY_STREAM_URL = 'ws://localhost:61613/stomp';
const ACTIVITY_STREAM_LOGIN = 'admin';
const ACTIVITY_STREAM_PASSCODE = 'admin';
const ACTIVITY_STREAM_DESTINATION = 'pipeline-activity';
const RECONNECT_DELAY_MS = 3000;

const STAGES = [
  { key: 'feasibility', label: 'Feasibility' },
  { key: 'synthesis', label: 'Synthesis' },
  { key: 'qc', label: 'Quality Control' },
  { key: 'shipped', label: 'Shipped' },
];

const STATUS_STAGE_KEY = {
  Feasible: 'feasibility',
  In_Synthesis: 'synthesis',
  At_Risk: 'synthesis',
  QC: 'qc',
  Shipped: 'shipped',
};

const STATUS_LABELS = {
  Feasible: 'Feasible',
  Rejected: 'Rejected',
  In_Synthesis: 'In Synthesis',
  At_Risk: 'At Risk',
  QC: 'Quality Control',
  Shipped: 'Shipped',
};

function statusBadgeClass(status) {
  const map = {
    At_Risk: 'badge badge-warning',
    Rejected: 'badge badge-danger',
    Shipped: 'badge badge-success',
  };
  return map[status] || 'badge badge-neutral';
}

// Index into STAGES for a given order status. Rejected has no index of its
// own -- it's a terminal branch off the feasibility check, not a step along
// this line, and is handled separately by renderStageTracker.
function stageIndex(status) {
  const key = STATUS_STAGE_KEY[status];
  if (!key) return -1;
  return STAGES.findIndex((s) => s.key === key);
}

function renderStageTracker(status) {
  if (status === 'Rejected') {
    return `<ol class="stage-tracker stage-tracker-rejected">
      <li class="stage stage-done">Feasibility</li>
      <li class="stage stage-current stage-rejected">Rejected</li>
    </ol>`;
  }
  const current = stageIndex(status);
  const items = STAGES.map((stage, i) => {
    let cls = 'stage ';
    if (i < current) cls += 'stage-done';
    else if (i === current) cls += 'stage-current';
    else cls += 'stage-future';
    if (status === 'At_Risk' && i === current) cls += ' stage-at-risk';
    return `<li class="${cls}">${stage.label}</li>`;
  }).join('');
  return `<ol class="stage-tracker">${items}</ol>`;
}

function renderProgressBar(order) {
  const pct = Math.max(0, Math.min(100, Math.round(order.progressPct || 0)));
  const fillCls = order.status === 'At_Risk' ? 'progress-fill-at-risk'
    : order.status === 'Shipped' ? 'progress-fill-shipped'
      : order.status === 'Rejected' ? 'progress-fill-rejected'
        : 'progress-fill-normal';
  return `<div class="progress-row">
    <div class="progress-track"><div class="progress-fill ${fillCls}" style="width:${pct}%"></div></div>
    <span class="progress-label">${pct}%</span>
  </div>`;
}

// now is a parameter (not read internally from `new Date()`) so this stays
// pure and testable with a fixed clock.
function daysUntil(promisedShipDate, now = new Date()) {
  const promised = new Date(`${promisedShipDate}T00:00:00Z`);
  const diffMs = promised - now;
  return Math.ceil(diffMs / (1000 * 60 * 60 * 24));
}

function renderDeadlineBadge(promisedShipDate, now = new Date()) {
  if (!promisedShipDate) return '';
  const days = daysUntil(promisedShipDate, now);
  let cls;
  let text;
  if (days < 0) {
    cls = 'deadline-late';
    text = `${Math.abs(days)}d past promised date`;
  } else if (days <= 3) {
    cls = 'deadline-soon';
    text = `${days}d until promised date`;
  } else {
    cls = 'deadline-ok';
    text = `${days}d until promised date`;
  }
  return `<span class="deadline-badge ${cls}">${text}</span>`;
}

// Compares an order's previous and current polled state and describes what
// changed, in plain English. Returns [] on an order's first sighting this
// session (nothing to compare against yet) and when nothing changed.
function describeTransition(prevOrder, currOrder) {
  const events = [];
  if (!prevOrder) return events;
  if (prevOrder.status !== currOrder.status) {
    events.push(`${currOrder.name} status changed to ${STATUS_LABELS[currOrder.status] || currOrder.status}`);
  }
  const prevPct = prevOrder.progressPct || 0;
  const currPct = currOrder.progressPct || 0;
  if (currPct > prevPct) {
    events.push(`${currOrder.name} synthesis progress: ${Math.round(currPct)}%`);
  }
  return events;
}

// Turns one activity event published by a Mule flow into the same
// {time, text} shape describeTransition/renderEventLog already use, with
// a `live` flag so renderEventLog can style it distinctly from a
// poll-diff-derived entry.
function describeActivityEvent(event) {
  const time = new Date(event.timestamp).toLocaleTimeString();
  return { time, text: `[${event.source}] ${event.message}`, live: true };
}

function renderEventLog(entries) {
  if (entries.length === 0) {
    return '<li class="event-empty">No activity yet.</li>';
  }
  return entries.map((e) => `<li class="${e.live ? 'event-live' : ''}"><span class="event-time">${e.time}</span> ${e.text}</li>`).join('');
}

function formatOrderCard(order, feasibilityDetail) {
  const progressAndDeadline = order.status === 'Rejected'
    ? ''
    : `${renderProgressBar(order)}${order.status !== 'Shipped' ? renderDeadlineBadge(order.promisedShipDate) : ''}`;
  const feasibilitySection = feasibilityDetail
    ? `<div class="feasibility-detail">
        <strong>Feasibility score: ${feasibilityDetail.score.toFixed(2)}</strong>
        ${feasibilityDetail.reasons.length
    ? `<ul>${feasibilityDetail.reasons.map((r) => `<li>${r}</li>`).join('')}</ul>`
    : '<p>No issues flagged.</p>'}
      </div>`
    : '';
  return `<article class="order-card" data-batch-id="${order.batchId}">
    <header>
      <h3>${order.name}</h3>
      <span class="${statusBadgeClass(order.status)}">${STATUS_LABELS[order.status] || order.status}</span>
    </header>
    ${renderStageTracker(order.status)}
    ${progressAndDeadline}
    ${feasibilitySection}
    <footer><code>${order.batchId}</code></footer>
  </article>`;
}

async function fetchOrders() {
  const res = await fetch(`${EXPERIENCE_API_BASE}/orders`);
  if (!res.ok) throw new Error('failed to fetch orders');
  return res.json();
}

// Session-only cache of the score/reasons for orders submitted this session
// -- the API only ever returns the current status/progress, not the
// feasibility breakdown that scored it, so this is the only place that
// detail is available for display.
const sessionFeasibilityDetails = {};
let previousOrdersByBatch = {};
let eventLog = [];

async function refresh() {
  try {
    const orders = await fetchOrders();

    const changedBatchIds = new Set();
    const newEvents = [];
    for (const order of orders) {
      const prev = previousOrdersByBatch[order.batchId];
      const diffs = describeTransition(prev, order);
      if (diffs.length) changedBatchIds.add(order.batchId);
      for (const text of diffs) {
        newEvents.push({ time: new Date().toLocaleTimeString(), text });
      }
      previousOrdersByBatch[order.batchId] = order;
    }

    const container = document.querySelector('#orders-container');
    container.innerHTML = orders.map((o) => formatOrderCard(o, sessionFeasibilityDetails[o.batchId])).join('');
    for (const batchId of changedBatchIds) {
      const card = container.querySelector(`[data-batch-id="${batchId}"]`);
      if (card) {
        card.classList.add('card-pulse');
        setTimeout(() => card.classList.remove('card-pulse'), 1500);
      }
    }

    if (newEvents.length) {
      eventLog = [...newEvents.reverse(), ...eventLog].slice(0, MAX_EVENTS);
      document.querySelector('#event-feed-list').innerHTML = renderEventLog(eventLog);
    }
  } catch (err) {
    console.error('refresh failed:', err);
  }
}

async function submitOrder(event) {
  event.preventDefault();
  const form = event.target;
  const body = {
    sequence: form.sequence.value.trim().toUpperCase(),
    accountId: form.accountId.value.trim(),
    requestedShipDate: form.requestedShipDate.value,
  };
  const resultEl = document.querySelector('#submit-result');
  try {
    const res = await fetch(`${EXPERIENCE_API_BASE}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const result = await res.json();
    if (result.batchId) {
      sessionFeasibilityDetails[result.batchId] = { score: result.score, reasons: result.reasons || [] };
    }
    if (result.feasible === true) {
      resultEl.textContent = `Submitted — feasibility score ${result.score}`;
    } else if (result.feasible === false) {
      resultEl.textContent = `Rejected — score ${result.score}: ${(result.reasons || []).join(', ')}`;
    } else {
      resultEl.textContent = result.error || 'Submission failed';
    }
    await refresh();
  } catch (err) {
    resultEl.textContent = `Submission failed: ${err.message}`;
  }
}

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
