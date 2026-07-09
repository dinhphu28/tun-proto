'use strict';

// See SPEC.md §5. The first bytes of every yamux stream are a length-prefixed
// OpenRequest: a 4-byte big-endian length followed by that many JSON bytes.
const MAX_OPEN_REQUEST_SIZE = 4096;

// readOpenRequest consumes exactly the OpenRequest header from the front of a
// yamux stream and resolves with { network, address }. Any bytes that arrived
// after the header (the start of the tunneled payload) are pushed back onto the
// stream with unshift(), so the caller can pipe the stream straight to the
// target socket without losing a byte.
function readOpenRequest(stream, maxSize = MAX_OPEN_REQUEST_SIZE) {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    let needLen = null;
    let settled = false;

    const cleanup = () => {
      stream.removeListener('readable', onReadable);
      stream.removeListener('end', onEnd);
      stream.removeListener('error', onError);
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };

    const onReadable = () => {
      let chunk;
      while (!settled && (chunk = stream.read()) !== null) {
        buf = buf.length ? Buffer.concat([buf, chunk]) : chunk;

        if (needLen === null && buf.length >= 4) {
          needLen = buf.readUInt32BE(0);
          if (needLen === 0 || needLen > maxSize) {
            return fail(new Error(`invalid open request size: ${needLen}`));
          }
        }

        if (needLen !== null && buf.length >= 4 + needLen) {
          const jsonBuf = buf.subarray(4, 4 + needLen);
          const leftover = buf.subarray(4 + needLen);

          let req;
          try {
            req = JSON.parse(jsonBuf.toString('utf8'));
          } catch {
            return fail(new Error('invalid open request JSON'));
          }

          const network = req.network == null || req.network === '' ? 'tcp' : req.network;
          if (network !== 'tcp') return fail(new Error(`unsupported network: ${network}`));
          if (typeof req.address !== 'string' || req.address.length === 0) {
            return fail(new Error('empty target address'));
          }

          settled = true;
          cleanup();
          if (leftover.length) stream.unshift(leftover);
          return resolve({ network, address: req.address });
        }
      }
    };

    const onEnd = () => fail(new Error('stream closed before open request completed'));
    const onError = (err) => fail(err);

    stream.on('readable', onReadable);
    stream.on('end', onEnd);
    stream.on('error', onError);
    onReadable(); // drain anything already buffered
  });
}

module.exports = { readOpenRequest, MAX_OPEN_REQUEST_SIZE };
