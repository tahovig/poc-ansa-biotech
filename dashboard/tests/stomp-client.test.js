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
