'use strict';

// Runtime fix for a flow-control bug in yamux-js@0.2.1.
//
// Upstream's Session.handleStreamMessage drops the window-size delta carried on
// a SYN-flagged WINDOW_UPDATE frame — it returns immediately after creating the
// stream, before applying the advertised window. A peer that advertises a large
// initial receive window on SYN (hashicorp/yamux and the Go tunnel client do:
// ~16 MiB) therefore leaves our send window stuck at the 256 KiB yamux initial.
// Because that peer won't re-advertise until it has consumed ~half its window,
// any bulk send larger than 256 KiB DEADLOCKS: we wait for a WINDOW_UPDATE that
// never comes. Confirmed against the real Go client (a 4 MiB download stalls at
// exactly 262144 bytes).
//
// Applying the SYN delta here restores correct flow control and lets us keep the
// full 16 MiB window for throughput. Pinned to yamux-js 0.2.1 (see
// package.json); revisit if that version changes or upstream fixes the bug.
let patched = false;

function applyYamuxFlowControlFix() {
  if (patched) return;
  const { Session } = require('yamux-js/lib/session');
  const { FLAGS, TYPES } = require('yamux-js/lib/constants');

  const original = Session.prototype.handleStreamMessage;
  Session.prototype.handleStreamMessage = function (header, fullPacket, encoding) {
    if (header.flags === FLAGS.SYN) {
      this.incomingStream(header.streamID);
      // The one line upstream is missing: honor the initial window the peer
      // advertised on the SYN frame.
      if (header.type === TYPES.WindowUpdate) {
        const stream = this.streams.get(header.streamID);
        if (stream) stream.incrSendWindow(header);
      }
      return;
    }
    return original.call(this, header, fullPacket, encoding);
  };

  patched = true;
}

module.exports = { applyYamuxFlowControlFix };
