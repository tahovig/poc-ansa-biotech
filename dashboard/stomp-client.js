const FRAME_TERMINATOR = '\0';

function buildConnectFrame({ login, passcode }) {
  return `CONNECT\naccept-version:1.2\nhost:/\nlogin:${login}\npasscode:${passcode}\nheart-beat:10000,0\n\n${FRAME_TERMINATOR}`;
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
