'use strict';

// parseAddress splits a "host:port" target into { host, port }.
// Supports hostnames, IPv4, and bracketed IPv6 ("[::1]:5432").
function parseAddress(address) {
  if (typeof address !== 'string' || address.length === 0) {
    throw new Error('empty target address');
  }

  let host;
  let portStr;

  if (address[0] === '[') {
    // [IPv6]:port
    const end = address.indexOf(']');
    if (end === -1) throw new Error(`invalid IPv6 address: ${address}`);
    host = address.slice(1, end);
    if (address[end + 1] !== ':') throw new Error(`missing port: ${address}`);
    portStr = address.slice(end + 2);
  } else {
    const i = address.lastIndexOf(':');
    if (i === -1) throw new Error(`missing port: ${address}`);
    host = address.slice(0, i);
    portStr = address.slice(i + 1);
  }

  if (host.length === 0) throw new Error(`empty host: ${address}`);

  const port = Number(portStr);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`invalid port: ${portStr}`);
  }

  return { host, port };
}

module.exports = { parseAddress };
