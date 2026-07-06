const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// ship.js pulls in runtime state helpers that touch $HOME/.config/devclocked.
// Redirect HOME before requiring so tests never touch real plugin state.
process.env.HOME = fs.mkdtempSync(path.join(os.tmpdir(), 'devclocked-claude-ship-test-'));

const { isActivityTypeTransition, isLifecycleEvent } = require('./ship');
const runtime = require('./runtime');

test('only SessionStart/SessionEnd are lifecycle events (Stop is per-turn)', () => {
  assert.equal(isLifecycleEvent('SessionStart'), true);
  assert.equal(isLifecycleEvent('SessionEnd'), true);
  assert.equal(isLifecycleEvent('Stop'), false);
  assert.equal(isLifecycleEvent('PostToolUse'), false);
});

test('a differing classification counts as a transition and ships through the throttle', () => {
  assert.equal(isActivityTypeTransition({ last_activity_type: 'coding' }, 'planning'), true);
});

test('a same-type tick inside the window keeps throttling', () => {
  assert.equal(isActivityTypeTransition({ last_activity_type: 'coding' }, 'coding'), false);
});

test('no recorded prior type cannot be a transition', () => {
  assert.equal(isActivityTypeTransition(null, 'coding'), false);
  assert.equal(isActivityTypeTransition({}, 'coding'), false);
});

test('enqueueHookEvent writes an envelope into the claude-hook-queue dir', () => {
  const filePath = runtime.enqueueHookEvent({
    hook_event_name: 'PostToolUse',
    session_id: 'sess-q1',
    tool_name: 'Bash',
    devclocked_capture: { remote: false, bridge_session_id: null },
  });

  assert.ok(filePath.includes(path.join('.config', 'devclocked', 'claude-hook-queue')));
  const envelope = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
  assert.ok(envelope.id);
  assert.ok(envelope.captured_at);
  assert.equal(envelope.attempts, 0);
  assert.equal(envelope.input.hook_event_name, 'PostToolUse');
  assert.equal(envelope.input.session_id, 'sess-q1');

  const listed = runtime.listQueueFiles();
  assert.ok(listed.includes(filePath));
});

test('shipper batch payload is a single-tick ticks array', () => {
  const stream = runtime.resolveStream('PostToolUse', { session_id: 'sess-q2', tool_name: 'Bash' });
  const payload = runtime.buildTrackTickRequest(
    'PostToolUse',
    { session_id: 'sess-q2', tool_name: 'Bash' },
    stream,
    { branch: 'main', repo_name: 'example' },
    { repoUrl: null, repoFullName: null, workspaceFingerprint: 'fp-1' }
  );

  assert.ok(Array.isArray(payload.ticks));
  assert.equal(payload.ticks.length, 1);
  assert.equal(payload.workspace_fingerprint, 'fp-1');
  assert.equal(payload.ticks[0].activity_context.ai_tool.tool, 'claude-code');
});
