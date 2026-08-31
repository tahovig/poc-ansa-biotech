const test = require('node:test');
const assert = require('node:assert');
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

test('stageIndex orders the lifecycle stages correctly', () => {
  assert.strictEqual(stageIndex('Feasible'), 0);
  assert.strictEqual(stageIndex('In_Synthesis'), 1);
  assert.strictEqual(stageIndex('At_Risk'), 1);
  assert.strictEqual(stageIndex('QC'), 2);
  assert.strictEqual(stageIndex('Shipped'), 3);
});

test('renderStageTracker marks earlier stages done and the current one current', () => {
  const html = renderStageTracker('QC');
  assert.match(html, /stage-done">Feasibility/);
  assert.match(html, /stage-done">Synthesis/);
  assert.match(html, /stage-current">Quality Control/);
  assert.match(html, /stage-future">Shipped/);
});

test('renderStageTracker flags the synthesis stage as at-risk', () => {
  const html = renderStageTracker('At_Risk');
  assert.match(html, /stage-current stage-at-risk">Synthesis/);
});

test('renderStageTracker renders a distinct terminal branch for Rejected', () => {
  const html = renderStageTracker('Rejected');
  assert.match(html, /stage-rejected/);
  assert.doesNotMatch(html, /Shipped/);
});

test('renderProgressBar rounds progress and reflects status in fill class', () => {
  const html = renderProgressBar({ status: 'At_Risk', progressPct: 42.7 });
  assert.match(html, /width:43%/);
  assert.match(html, /progress-fill-at-risk/);
});

test('renderProgressBar clamps out-of-range progress', () => {
  const html = renderProgressBar({ status: 'In_Synthesis', progressPct: 137 });
  assert.match(html, /width:100%/);
});

test('daysUntil counts whole days from now to the promised date', () => {
  const now = new Date('2026-08-26T12:00:00Z');
  assert.strictEqual(daysUntil('2026-08-29', now), 3);
  assert.strictEqual(daysUntil('2026-08-23', now), -3);
});

test('renderDeadlineBadge flags dates within 3 days as soon and past dates as late', () => {
  const now = new Date('2026-08-26T12:00:00Z');
  assert.match(renderDeadlineBadge('2026-08-27', now), /deadline-soon/);
  assert.match(renderDeadlineBadge('2026-08-20', now), /deadline-late/);
  assert.match(renderDeadlineBadge('2026-09-30', now), /deadline-ok/);
});

test('renderDeadlineBadge renders nothing without a promised date', () => {
  assert.strictEqual(renderDeadlineBadge(undefined), '');
});

test('describeTransition reports no events on first sighting of an order', () => {
  const events = describeTransition(undefined, { name: 'ORD-1', status: 'Feasible', progressPct: null });
  assert.deepStrictEqual(events, []);
});

test('describeTransition reports a status change', () => {
  const prev = { name: 'ORD-1', status: 'Feasible', progressPct: null };
  const curr = { name: 'ORD-1', status: 'In_Synthesis', progressPct: 0 };
  const events = describeTransition(prev, curr);
  assert.strictEqual(events.length, 1);
  assert.match(events[0], /ORD-1 status changed to In Synthesis/);
});

test('describeTransition reports progress increasing even without a status change', () => {
  const prev = { name: 'ORD-1', status: 'In_Synthesis', progressPct: 25 };
  const curr = { name: 'ORD-1', status: 'In_Synthesis', progressPct: 50 };
  const events = describeTransition(prev, curr);
  assert.strictEqual(events.length, 1);
  assert.match(events[0], /progress: 50%/);
});

test('describeTransition reports nothing when nothing changed', () => {
  const order = { name: 'ORD-1', status: 'QC', progressPct: 90 };
  assert.deepStrictEqual(describeTransition(order, order), []);
});

test('renderEventLog shows a placeholder when there is no activity yet', () => {
  assert.match(renderEventLog([]), /No activity yet/);
});

test('renderEventLog renders each entry with its timestamp', () => {
  const html = renderEventLog([{ time: '10:00:00 AM', text: 'ORD-1 shipped' }]);
  assert.match(html, /10:00:00 AM/);
  assert.match(html, /ORD-1 shipped/);
});

test('formatOrderCard renders the order name, batch id, and stage tracker', () => {
  const html = formatOrderCard({ name: 'ORD-1', status: 'In_Synthesis', progressPct: 42.7, feasibilityScore: 0.91, batchId: 'batch-1' });
  assert.match(html, /ORD-1/);
  assert.match(html, /batch-1/);
  assert.match(html, /stage-tracker/);
});

test('formatOrderCard includes the feasibility breakdown when detail is available', () => {
  const html = formatOrderCard(
    { name: 'ORD-1', status: 'Rejected', progressPct: null, batchId: 'batch-1' },
    { score: 0.4, reasons: ['High GC content (75%) increases synthesis difficulty'] },
  );
  assert.match(html, /0\.40/);
  assert.match(html, /High GC content/);
});

test('formatOrderCard omits the feasibility breakdown when no detail is cached', () => {
  const html = formatOrderCard({ name: 'ORD-1', status: 'Feasible', progressPct: null, batchId: 'batch-1' });
  assert.doesNotMatch(html, /feasibility-detail/);
});

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
