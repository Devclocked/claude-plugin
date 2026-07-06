const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const TRACK_PATH = path.join(__dirname, 'track.js');

function runTrack(stdinJson, { home, env = {}, argv = [] } = {}) {
  const result = spawnSync(process.execPath, [TRACK_PATH, ...argv], {
    input: stdinJson,
    encoding: 'utf-8',
    env: {
      PATH: process.env.PATH,
      HOME: home,
      ...env,
    },
    timeout: 15_000,
  });
  return result;
}

function queueDir(home) {
  return path.join(home, '.config', 'devclocked', 'claude-hook-queue');
}

function readQueuedInputs(home) {
  const dir = queueDir(home);
  let names = [];
  try {
    names = fs.readdirSync(dir).filter((name) => name.endsWith('.json'));
  } catch {
    return [];
  }
  return names
    .sort()
    .map((name) => JSON.parse(fs.readFileSync(path.join(dir, name), 'utf-8')).input);
}

test('track.js enqueues each supported hook event from stdin', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'devclocked-claude-track-test-'));
  const events = [
    { hook_event_name: 'SessionStart', session_id: 's1', source: 'startup', model: 'claude-opus-4-6' },
    { hook_event_name: 'UserPromptSubmit', session_id: 's1' },
    { hook_event_name: 'PostToolUse', session_id: 's1', tool_name: 'Edit', tool_input: { file_path: '/tmp/a.ts' } },
    { hook_event_name: 'Stop', session_id: 's1' },
    { hook_event_name: 'SubagentStart', session_id: 's1', agent_id: 'a1', agent_type: 'Explore' },
    { hook_event_name: 'SubagentStop', session_id: 's1', agent_id: 'a1' },
    { hook_event_name: 'SessionEnd', session_id: 's1' },
  ];

  for (const event of events) {
    const result = runTrack(JSON.stringify(event), { home });
    assert.equal(result.status, 0, `track.js should exit 0 for ${event.hook_event_name}: ${result.stderr}`);
    assert.equal(result.stdout, '{}');
  }

  const inputs = readQueuedInputs(home);
  assert.equal(inputs.length, events.length);
  const queuedEvents = inputs.map((input) => input.hook_event_name).sort();
  assert.deepEqual(queuedEvents, events.map((event) => event.hook_event_name).sort());

  for (const input of inputs) {
    assert.ok(input.devclocked_capture, 'capture metadata is stamped at enqueue time');
    assert.equal(input.devclocked_capture.remote, false);
    assert.equal(input.devclocked_capture.bridge_session_id, null);
    assert.ok(input.devclocked_capture.captured_at);
  }
});

test('track.js captures CLAUDE_CODE_REMOTE + bridge session id from the hook env', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'devclocked-claude-track-test-'));
  const result = runTrack(
    JSON.stringify({ hook_event_name: 'UserPromptSubmit', session_id: 's2' }),
    {
      home,
      env: {
        CLAUDE_CODE_REMOTE: 'true',
        CLAUDE_CODE_BRIDGE_SESSION_ID: 'bridge-7',
      },
    }
  );
  assert.equal(result.status, 0);

  const inputs = readQueuedInputs(home);
  assert.equal(inputs.length, 1);
  assert.equal(inputs[0].devclocked_capture.remote, true);
  assert.equal(inputs[0].devclocked_capture.bridge_session_id, 'bridge-7');
});

test('track.js falls back to the argv event name when stdin omits it', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'devclocked-claude-track-test-'));
  const result = runTrack(JSON.stringify({ session_id: 's3' }), { home, argv: ['SessionStart'] });
  assert.equal(result.status, 0);

  const inputs = readQueuedInputs(home);
  assert.equal(inputs.length, 1);
  assert.equal(inputs[0].hook_event_name, 'SessionStart');
});

test('track.js exits cleanly on empty and malformed stdin without enqueueing', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'devclocked-claude-track-test-'));

  const empty = runTrack('', { home });
  assert.equal(empty.status, 0);
  assert.equal(empty.stdout, '{}');

  const malformed = runTrack('not-json{', { home });
  assert.equal(malformed.status, 0);
  assert.equal(malformed.stdout, '{}');

  assert.equal(readQueuedInputs(home).length, 0);
});
