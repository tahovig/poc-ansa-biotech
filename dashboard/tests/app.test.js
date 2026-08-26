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
