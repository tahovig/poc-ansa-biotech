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
