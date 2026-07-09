import { EventEmitter } from 'events';
import { Server as HttpServer, IncomingMessage } from 'http';
import { AddressInfo } from 'net';

export interface TunnelServerOptions {
  /**
   * Authentication — provide exactly one of these three.
   * `apiKeys`: a static allowlist of accepted Bearer tokens.
   * `authenticate`: a custom predicate (sync or async) over the Bearer key.
   * `authDisabled`: accept everything (development only).
   */
  apiKeys?: string[];
  authenticate?: (apiKey: string, req: IncomingMessage) => boolean | Promise<boolean>;
  authDisabled?: boolean;

  /** WebSocket upgrade path. Default: "/tunnels". */
  path?: string;

  /**
   * Attach to an existing http.Server (share a port with your app) instead of
   * creating one. When set, close() will NOT close this server.
   */
  server?: HttpServer;

  /**
   * Egress policy. Return false to refuse dialing a target. Strongly
   * recommended in production — without it, any authenticated client can make
   * the server open a TCP connection to any host:port it names (SSRF surface).
   */
  allowTarget?: (
    host: string,
    port: number,
    ctx: { remoteAddress?: string }
  ) => boolean | Promise<boolean>;

  /** Max per-stream yamux window in bytes. Default: 16 MiB (matches the reference Go client). */
  maxStreamWindowSize?: number;
  /** Target dial timeout in ms. Default: 10000. */
  dialTimeout?: number;
  /** Max concurrent tunnel sessions; 0 = unlimited. Over-limit upgrades get 503. */
  maxSessions?: number;
  /** Max concurrent proxied streams across all sessions; 0 = unlimited. */
  maxStreams?: number;
  /** Log lifecycle events to console. Default: false. */
  debug?: boolean;
}

export interface SessionInfo {
  remoteAddress?: string;
}
export interface StreamInfo {
  address: string;
  remoteAddress?: string;
}
export interface StreamErrorInfo {
  error: Error;
  address?: string;
  remoteAddress?: string;
}

export class TunnelServer extends EventEmitter {
  constructor(options?: TunnelServerOptions);
  /** Start listening; resolves with the server once bound. */
  listen(port: number, host?: string): Promise<this>;
  /** The bound address, or null if not listening. */
  address(): AddressInfo | string | null;
  /** Stop accepting connections; closes the owned http server. */
  close(): Promise<void>;

  on(event: 'listening', listener: (address: AddressInfo | string | null) => void): this;
  on(event: 'session', listener: (info: SessionInfo) => void): this;
  on(event: 'session-close', listener: (info: SessionInfo) => void): this;
  on(event: 'stream', listener: (info: StreamInfo) => void): this;
  on(event: 'stream-close', listener: (info: StreamInfo) => void): this;
  on(event: 'stream-error', listener: (info: StreamErrorInfo) => void): this;
  on(event: 'error', listener: (err: Error) => void): this;
  on(event: string | symbol, listener: (...args: any[]) => void): this;
}

/** Sugar for `new TunnelServer(options)`. */
export function createTunnelServer(options?: TunnelServerOptions): TunnelServer;
