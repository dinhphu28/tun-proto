'use strict';

// closeYamuxStream closes a yamux stream so the tunnel peer observes the close
// (a yamux FIN frame), then releases the underlying Node stream. A plain
// .destroy() does NOT emit any yamux frame, so without this the remote side
// would leak a half-open stream.
function closeYamuxStream(stream) {
  try {
    stream.close(); // yamux FIN
  } catch {
    /* already closing */
  }
  // Destroy on the next tick so the FIN is flushed to the transport first.
  setImmediate(() => {
    try {
      stream.destroy();
    } catch {
      /* ignore */
    }
  });
}

// splice copies bytes both ways between a yamux stream and the dialed target
// socket, and tears both down — sending a yamux FIN to the tunnel peer — as soon
// as either side ends or errors. Mirrors SPEC.md §5.3 ("closes both sides when
// either closes"). onClose runs exactly once.
function splice(yamuxStream, socket, onClose) {
  let done = false;
  const finish = () => {
    if (done) return;
    done = true;
    closeYamuxStream(yamuxStream);
    try {
      socket.destroy();
    } catch {
      /* ignore */
    }
    if (onClose) onClose();
  };

  // A reset on either side is a normal way for a tunneled connection to end;
  // react by tearing the pair down rather than throwing.
  yamuxStream.on('error', finish);
  yamuxStream.on('end', finish);
  yamuxStream.on('close', finish);
  socket.on('error', finish);
  socket.on('end', finish);
  socket.on('close', finish);

  yamuxStream.pipe(socket);
  socket.pipe(yamuxStream);
}

module.exports = { splice, closeYamuxStream };
