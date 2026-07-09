'use strict';

// Minimal tunnel server. A tun-proto client connecting with
//   wss://<host>:8080/tunnels   (Authorization: Bearer dev-secret)
// can forward local connections to any TCP target reachable from this process.
//
//   node examples/quickstart.js

const { createTunnelServer } = require('..'); // require('tun-proto-server') in your app

const tunnel = createTunnelServer({
  apiKeys: ['dev-secret'], // accepted Bearer tokens
  debug: true, // log sessions/streams to the console
});

tunnel.on('stream', ({ address, remoteAddress }) => {
  console.log(`opened ${remoteAddress} -> ${address}`);
});

tunnel.listen(8080).then(() => {
  console.log('tunnel server listening on ws://localhost:8080/tunnels');
});
