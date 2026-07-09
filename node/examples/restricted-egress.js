'use strict';

// Production-shaped example: async key check + an egress allowlist so clients
// can only reach approved targets, plus session/stream caps.
//
//   node examples/restricted-egress.js

const { createTunnelServer } = require('..'); // require('tun-proto-server') in your app

// Pretend these come from a database / secrets manager.
const VALID_KEYS = new Set(['team-a-key', 'team-b-key']);
const ALLOWED = new Set(['10.0.0.5:5432', '10.0.0.6:6379']);

const tunnel = createTunnelServer({
  // Custom async authentication.
  authenticate: async (apiKey /*, req */) => VALID_KEYS.has(apiKey),

  // Only allow dialing an explicit set of targets (blocks SSRF to internal
  // metadata endpoints, localhost, etc.).
  allowTarget: (host, port) => ALLOWED.has(`${host}:${port}`),

  maxSessions: 50,
  maxStreams: 200,
  debug: true,
});

tunnel.on('stream-error', ({ error, address }) => {
  console.warn(`refused ${address ?? '?'}: ${error.message}`);
});

tunnel.listen(8080).then(() => {
  console.log('restricted tunnel server on ws://localhost:8080/tunnels');
});
