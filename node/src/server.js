'use strict';

const http = require('http');
const net = require('net');
const { EventEmitter } = require('events');
const { WebSocketServer, createWebSocketStream } = require('ws');
const { Server: YamuxServer } = require('yamux-js');

const { applyYamuxFlowControlFix } = require('./yamux-patch');
const { readOpenRequest } = require('./open-request');
const { parseAddress } = require('./address');
const { splice, closeYamuxStream } = require('./splice');

// Fix yamux-js's SYN window-update handling before any Session is created,
// otherwise bulk transfers deadlock against the Go client. See yamux-patch.js.
applyYamuxFlowControlFix();

const DEFAULT_PATH = '/tunnels';
// Matches the reference Go client's MaxStreamWindowSize so large transfers (e.g.
// Postgres result sets) are not serialised into many flow-control round-trips.
// Safe to use because yamux-patch.js repairs yamux-js's window handling.
const DEFAULT_WINDOW = 16 * 1024 * 1024; // 16 MiB
const DEFAULT_DIAL_TIMEOUT = 10_000; // ms

/**
 * A tun-proto tunnel server. Accepts authenticated WebSocket connections,
 * multiplexes them with yamux, and for each logical stream dials the target the
 * client named and splices bytes both ways.
 *
 * Consumers never touch yamux, WebSocket, or the OpenRequest framing — they
 * provide auth and (optionally) an egress policy, then call listen().
 */
class TunnelServer extends EventEmitter {
  constructor(options = {}) {
    super();

    this._path = options.path || DEFAULT_PATH;
    this._window = options.maxStreamWindowSize || DEFAULT_WINDOW;
    this._dialTimeout = options.dialTimeout || DEFAULT_DIAL_TIMEOUT;
    this._maxSessions = options.maxSessions || 0;
    this._maxStreams = options.maxStreams || 0;
    this._allowTarget = options.allowTarget || null;
    this._log = options.debug ? console.log.bind(console) : () => {};

    this._sessions = 0;
    this._streams = 0;

    this._auth = buildAuth(options);

    // Use a caller-supplied http.Server if given (so the tunnel can share a port
    // with an existing app); otherwise own one that answers 426 to plain GETs.
    this._ownServer = !options.server;
    this._http =
      options.server ||
      http.createServer((req, res) => {
        res.writeHead(426, { 'content-type': 'text/plain' });
        res.end(`Upgrade Required: connect to ${this._path} over WebSocket\n`);
      });

    this._wss = new WebSocketServer({ noServer: true });
    this._http.on('upgrade', (req, socket, head) => this._onUpgrade(req, socket, head));
  }

  /** Start listening. Resolves with the server once bound. */
  listen(port, host) {
    return new Promise((resolve, reject) => {
      const onError = (err) => reject(err);
      this._http.once('error', onError);
      this._http.listen(port, host, () => {
        this._http.removeListener('error', onError);
        this.emit('listening', this._http.address());
        resolve(this);
      });
    });
  }

  /** The bound address, or null if not listening. */
  address() {
    return this._http.address();
  }

  /** Stop accepting connections and close the owned http server. */
  close() {
    return new Promise((resolve) => {
      try {
        this._wss.close();
      } catch {
        /* ignore */
      }
      if (this._ownServer) this._http.close(() => resolve());
      else resolve();
    });
  }

  async _onUpgrade(req, socket, head) {
    try {
      const pathname = new URL(req.url, 'http://localhost').pathname;
      if (pathname !== this._path) return reject(socket, 404, 'Not Found');

      const key = bearer(req);
      let ok = false;
      try {
        ok = await this._auth(key, req);
      } catch {
        ok = false;
      }
      if (!ok) return reject(socket, 401, 'Unauthorized');

      if (this._maxSessions && this._sessions >= this._maxSessions) {
        return reject(socket, 503, 'Service Unavailable');
      }

      this._wss.handleUpgrade(req, socket, head, (ws) => this._onConnection(ws, req));
    } catch (err) {
      this.emit('error', err);
      try {
        socket.destroy();
      } catch {
        /* ignore */
      }
    }
  }

  _onConnection(ws, req) {
    this._sessions++;
    const remoteAddress = req.socket.remoteAddress;
    this.emit('session', { remoteAddress });
    this._log(`tunnel: session opened from ${remoteAddress} (${this._sessions} live)`);

    const transport = createWebSocketStream(ws);

    const yamux = new YamuxServer((stream) => this._onStream(stream, remoteAddress), {
      enableKeepAlive: true,
      keepAliveInterval: 30,
      maxStreamWindowSize: this._window,
      logger: this._log,
    });

    // Surface transport/yamux errors as a swallowed teardown, not a throw.
    transport.on('error', () => {});
    yamux.on('error', (err) => this.emit('error', err));

    // Bridge the WebSocket byte stream to yamux, both directions.
    //
    // We deliberately do NOT use `transport.pipe(yamux).pipe(transport)`.
    // yamux-js is a single Transform whose inbound processing (_transform)
    // pauses whenever its outbound readable side is backpressured. Under a
    // bulk transfer that couples the two directions into a deadlock: the peer's
    // WINDOW_UPDATE frames (which would unblock our sender) sit unprocessed
    // because the readable side is full of our own outbound data. Draining each
    // side in flowing mode ("data" events) keeps the readable near-empty so
    // _transform is never starved. yamux's own flow control still bounds the
    // in-flight bytes, so this does not buffer without limit.
    const fl = process.env.TUN_PROTO_FRAMELOG ? makeFrameLog() : null;
    transport.on('data', (chunk) => {
      if (fl) fl('IN ', chunk);
      yamux.write(chunk);
    });
    yamux.on('data', (chunk) => {
      if (fl) fl('OUT', chunk);
      try {
        transport.write(chunk);
      } catch {
        /* transport gone; teardown runs via ws 'close' */
      }
    });

    let cleaned = false;
    const cleanup = () => {
      if (cleaned) return;
      cleaned = true;
      this._sessions--;
      try {
        yamux.close();
      } catch {
        /* ignore */
      }
      try {
        transport.destroy();
      } catch {
        /* ignore */
      }
      this.emit('session-close', { remoteAddress });
      this._log(`tunnel: session closed from ${remoteAddress} (${this._sessions} live)`);
    };
    ws.on('close', cleanup);
    ws.on('error', cleanup);
  }

  async _onStream(stream, remoteAddress) {
    if (this._maxStreams && this._streams >= this._maxStreams) {
      this._log(`tunnel: stream cap reached (${this._maxStreams}); rejecting`);
      closeYamuxStream(stream);
      return;
    }

    let req;
    try {
      req = await readOpenRequest(stream);
    } catch (err) {
      this.emit('stream-error', { error: err, remoteAddress });
      closeYamuxStream(stream);
      return;
    }

    let target;
    try {
      target = parseAddress(req.address);
    } catch (err) {
      this.emit('stream-error', { error: err, address: req.address, remoteAddress });
      closeYamuxStream(stream);
      return;
    }

    if (this._allowTarget) {
      let allowed = false;
      try {
        allowed = await this._allowTarget(target.host, target.port, { remoteAddress });
      } catch {
        allowed = false;
      }
      if (!allowed) {
        this.emit('stream-error', {
          error: new Error(`target not allowed: ${req.address}`),
          address: req.address,
          remoteAddress,
        });
        closeYamuxStream(stream);
        return;
      }
    }

    const socket = net.connect({ host: target.host, port: target.port });
    const timer = setTimeout(() => {
      socket.destroy(new Error(`dial timeout: ${req.address}`));
    }, this._dialTimeout);

    const onDialError = (err) => {
      clearTimeout(timer);
      this.emit('stream-error', { error: err, address: req.address, remoteAddress });
      closeYamuxStream(stream);
      try {
        socket.destroy();
      } catch {
        /* ignore */
      }
    };

    socket.once('error', onDialError);
    socket.once('connect', () => {
      clearTimeout(timer);
      socket.removeListener('error', onDialError);
      this._streams++;
      this.emit('stream', { address: req.address, remoteAddress });
      this._log(`tunnel: proxying ${remoteAddress} -> ${req.address} (${this._streams} active)`);

      splice(stream, socket, () => {
        this._streams--;
        this.emit('stream-close', { address: req.address, remoteAddress });
        this._log(`tunnel: closed ${req.address} (${this._streams} active)`);
      });
    });
  }
}

// buildAuth turns the auth-related options into a single async predicate.
function buildAuth(options) {
  if (options.authDisabled) return async () => true;
  if (typeof options.authenticate === 'function') return options.authenticate;
  if (Array.isArray(options.apiKeys)) {
    const set = new Set(options.apiKeys);
    return async (key) => typeof key === 'string' && key.length > 0 && set.has(key);
  }
  throw new Error(
    'tun-proto: provide exactly one of { apiKeys, authenticate, authDisabled } in options'
  );
}

// makeFrameLog returns a best-effort yamux frame-header decoder for debugging
// (gated by TUN_PROTO_FRAMELOG). Assumes chunks begin on a frame boundary.
function makeFrameLog() {
  const TYPES = ['DATA', 'WINDOW_UPDATE', 'PING', 'GO_AWAY'];
  return (dir, buf) => {
    let off = 0;
    let n = 0;
    while (off + 12 <= buf.length && n < 8) {
      const type = buf[off + 1];
      const flags = buf.readUInt16BE(off + 2);
      const sid = buf.readUInt32BE(off + 4);
      const len = buf.readUInt32BE(off + 8);
      console.log(`  ${dir} type=${TYPES[type] || type} flags=${flags} sid=${sid} len=${len}`);
      off += 12 + (type === 0 ? len : 0); // only DATA has a payload
      n++;
    }
  };
}

function bearer(req) {
  const h = req.headers['authorization'] || '';
  return h.startsWith('Bearer ') ? h.slice('Bearer '.length) : '';
}

function reject(socket, code, message) {
  socket.write(`HTTP/1.1 ${code} ${message}\r\nConnection: close\r\n\r\n`);
  socket.destroy();
}

/** Factory sugar: createTunnelServer(opts) === new TunnelServer(opts). */
function createTunnelServer(options) {
  return new TunnelServer(options);
}

module.exports = { TunnelServer, createTunnelServer };
