#!/usr/bin/env node

const runtime = require('./runtime');
const { runTrack } = require('../runtime/track');

// The shipper runs detached and possibly much later, so remote-control state
// must be captured from the environment while the hook process still has it.
runTrack(runtime, (input) => ({
  ...input,
  hook_event_name: input.hook_event_name || process.argv[2] || null,
  devclocked_capture: {
    captured_at: new Date().toISOString(),
    remote: process.env.CLAUDE_CODE_REMOTE === 'true',
    bridge_session_id: process.env.CLAUDE_CODE_BRIDGE_SESSION_ID || null,
  },
}));
