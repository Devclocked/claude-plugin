const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Stream/state helpers below touch disk under $HOME/.config/devclocked. Point
// HOME at a throwaway dir before requiring runtime.js so tests never read or
// write the real plugin state.
process.env.HOME = fs.mkdtempSync(path.join(os.tmpdir(), 'devclocked-claude-runtime-test-'));

const {
  buildTrackTickRequest,
  classifyActivity,
  isTrackTickProcessed,
  rememberSessionModel,
  resolveExecutionContext,
  resolveModel,
  resolveRepo,
  resolveStream,
  saveStreamState,
  sessionModel,
} = require('./runtime');

const NO_GIT = { repoUrl: null, repoFullName: null, workspaceFingerprint: null };
const REPO = { branch: 'main', repo_name: 'example' };

function primaryStream(sessionId) {
  return resolveStream('PostToolUse', { session_id: sessionId });
}

test('resolveStream keys primary streams by session_id', () => {
  const stream = resolveStream('PostToolUse', { session_id: 'sess-1', tool_name: 'Bash' });
  assert.equal(stream.streamId, 'sess-1');
  assert.equal(stream.rootStreamId, 'sess-1');
  assert.equal(stream.throttleId, 'sess-1');
  assert.equal(stream.isSubagent, false);
});

test('resolveStream isolates subagent streams under the session', () => {
  const stream = resolveStream('SubagentStart', {
    session_id: 'sess-1',
    agent_id: 'agent-9',
    agent_type: 'Explore',
  });
  assert.equal(stream.streamId, 'sess-1:agent-9');
  assert.equal(stream.rootStreamId, 'sess-1');
  assert.equal(stream.parentStreamId, 'sess-1');
  assert.equal(stream.throttleId, 'sess-1:agent-9');
  assert.equal(stream.isSubagent, true);
  assert.equal(stream.agentType, 'Explore');
});

test('resolveStream survives missing/unknown fields', () => {
  const stream = resolveStream('SubagentStop', {});
  assert.equal(stream.sessionId, 'unknown');
  assert.equal(stream.isSubagent, false);
});

test('lifecycle events use the session entity', () => {
  const payload = buildTrackTickRequest(
    'SessionStart',
    { session_id: 'sess-2', cwd: '/tmp' },
    primaryStream('sess-2'),
    REPO,
    NO_GIT
  );
  assert.equal(payload.ticks[0].entity, 'claude://session/sess-2');
  assert.equal(payload.ticks[0].entity_type, 'window');
});

test('Edit tool events use the file path entity and mark is_write', () => {
  const payload = buildTrackTickRequest(
    'PostToolUse',
    {
      session_id: 'sess-3',
      tool_name: 'Edit',
      tool_input: { file_path: '/tmp/project/src/index.ts' },
    },
    primaryStream('sess-3'),
    REPO,
    NO_GIT
  );
  const tick = payload.ticks[0];
  assert.equal(tick.entity, '/tmp/project/src/index.ts');
  assert.equal(tick.entity_type, 'file');
  assert.equal(tick.is_write, true);
  assert.equal(tick.activity_context.ai_tool.work_signature.write_count, 1);
});

test('non-file tool events use the claude://tool entity', () => {
  const payload = buildTrackTickRequest(
    'PostToolUse',
    { session_id: 'sess-3', tool_name: 'Bash', tool_input: { command: 'ls' } },
    primaryStream('sess-3'),
    REPO,
    NO_GIT
  );
  const tick = payload.ticks[0];
  assert.equal(tick.entity, 'claude://tool/Bash');
  assert.equal(tick.is_write, false);
  assert.equal(tick.activity_context.ai_tool.work_signature.exec_count, 1);
});

test('ai_tool identity merges with the daemon stream (no run_id)', () => {
  const payload = buildTrackTickRequest(
    'PostToolUse',
    { session_id: 'sess-4', tool_name: 'Bash' },
    primaryStream('sess-4'),
    REPO,
    NO_GIT
  );
  const aiTool = payload.ticks[0].activity_context.ai_tool;
  assert.equal(aiTool.tool, 'claude-code');
  assert.equal(aiTool.session_file_id, 'sess-4');
  assert.equal(aiTool.stream_id, 'claude-code:sess-4');
  assert.equal(aiTool.root_stream_id, 'claude-code:sess-4');
  assert.equal(aiTool.stream_role, 'primary');
  assert.equal(aiTool.ai_tool_version, 1);
  assert.ok(!('run_id' in aiTool), 'run_id must be omitted so plugin and daemon streams merge');
});

test('ai_tool omits runtime and token measurement fields entirely', () => {
  const payload = buildTrackTickRequest(
    'Stop',
    { session_id: 'sess-5' },
    primaryStream('sess-5'),
    REPO,
    NO_GIT
  );
  const aiTool = payload.ticks[0].activity_context.ai_tool;
  for (const forbidden of [
    'run_id',
    'runtime_ms',
    'runtime_started_at',
    'runtime_ended_at',
    'measurement_quality',
    'request_key',
    'token_usage',
    'token_usage_by_model',
    'token_usage_by_model_role',
    'agent_turns',
  ]) {
    assert.ok(!(forbidden in aiTool), `${forbidden} must not be shipped by the plugin`);
  }
});

test('subagent events carry agent_id and sidechain stream identity', () => {
  const stream = resolveStream('SubagentStop', {
    session_id: 'sess-6',
    agent_id: 'agent-2',
    agent_type: 'Explore',
  });
  const payload = buildTrackTickRequest(
    'SubagentStop',
    { session_id: 'sess-6', agent_id: 'agent-2', agent_type: 'Explore' },
    stream,
    REPO,
    NO_GIT
  );
  const aiTool = payload.ticks[0].activity_context.ai_tool;
  assert.equal(payload.ticks[0].entity, 'claude://agent/Explore/agent-2');
  assert.equal(aiTool.agent_id, 'agent-2');
  assert.equal(aiTool.agent_type, 'Explore');
  assert.equal(aiTool.is_sidechain, true);
  assert.equal(aiTool.stream_id, 'claude-code:sess-6:agent-2');
  assert.equal(aiTool.parent_stream_id, 'claude-code:sess-6');
  assert.equal(aiTool.stream_role, 'sidechain');
});

test('execution environment defaults to local desktop', () => {
  const payload = buildTrackTickRequest(
    'UserPromptSubmit',
    { session_id: 'sess-7', devclocked_capture: { remote: false, bridge_session_id: null } },
    primaryStream('sess-7'),
    REPO,
    NO_GIT
  );
  const aiTool = payload.ticks[0].activity_context.ai_tool;
  assert.equal(aiTool.execution_environment, 'local');
  assert.equal(aiTool.control_surface, 'desktop');
  assert.ok(!('bridge_session_id' in aiTool) || aiTool.bridge_session_id === undefined);
});

test('remote control sessions ship local_remote_control + bridge id', () => {
  const payload = buildTrackTickRequest(
    'UserPromptSubmit',
    {
      session_id: 'sess-8',
      devclocked_capture: { remote: true, bridge_session_id: 'bridge-42' },
    },
    primaryStream('sess-8'),
    REPO,
    NO_GIT
  );
  const aiTool = payload.ticks[0].activity_context.ai_tool;
  assert.equal(aiTool.execution_environment, 'local_remote_control');
  assert.equal(aiTool.control_surface, 'unknown');
  assert.equal(aiTool.bridge_session_id, 'bridge-42');
});

test('resolveExecutionContext handles missing capture defensively', () => {
  const ctx = resolveExecutionContext({});
  assert.equal(ctx.execution_environment, 'local');
  assert.equal(ctx.control_surface, 'desktop');
  assert.equal(ctx.bridge_session_id, null);
});

test('model captured at SessionStart is reused for later session events', () => {
  const stream = primaryStream('sess-model-1');
  rememberSessionModel(stream, { session_id: 'sess-model-1', model: 'claude-opus-4-6' });

  const payload = buildTrackTickRequest(
    'PostToolUse',
    { session_id: 'sess-model-1', tool_name: 'Bash' },
    stream,
    REPO,
    NO_GIT
  );
  const aiTool = payload.ticks[0].activity_context.ai_tool;
  assert.equal(aiTool.model, 'claude-opus-4-6');
  assert.equal(aiTool.model_provider, 'anthropic');
});

test('sentinel model values are dropped', () => {
  assert.equal(resolveModel('default'), null);
  assert.equal(resolveModel('AUTO'), null);
  assert.equal(resolveModel('  '), null);
  assert.equal(resolveModel('claude-sonnet-4-8'), 'claude-sonnet-4-8');

  const stream = primaryStream('sess-model-2');
  rememberSessionModel(stream, { session_id: 'sess-model-2', model: 'default' });
  assert.equal(sessionModel(stream, {}), null);
});

test('Stop/SessionEnd reuse the last real activity type recorded for the stream', () => {
  const stream = primaryStream('sess-9');
  saveStreamState('sess-9', { last_tick_at: Date.now() - 60_000, last_activity_type: 'reading' });

  assert.deepEqual(classifyActivity('Stop', {}, stream), {
    activity_type: 'reading',
    sub_type: 'turn_complete',
  });
  assert.deepEqual(classifyActivity('SessionEnd', {}, stream), {
    activity_type: 'reading',
    sub_type: 'session_end',
  });
});

test('Stop omits activity_type when the stream has no recorded activity', () => {
  const stream = primaryStream('sess-10');
  const activity = classifyActivity('Stop', {}, stream);
  assert.equal(activity.activity_type, undefined);
});

test('unknown hook events classify defensively', () => {
  const activity = classifyActivity('SomethingNew', {}, primaryStream('sess-11'));
  assert.equal(activity.activity_type, 'coding');
  assert.equal(activity.sub_type, 'somethingnew');
});

test('workspace_fingerprint rides on the request when known', () => {
  const payload = buildTrackTickRequest(
    'SessionStart',
    { session_id: 'sess-12' },
    primaryStream('sess-12'),
    REPO,
    { ...NO_GIT, workspaceFingerprint: 'abc123' }
  );
  assert.equal(payload.workspace_fingerprint, 'abc123');
});

test('workspace_path rides on the request when the workspace resolved', () => {
  const payload = buildTrackTickRequest(
    'SessionStart',
    { session_id: 'sess-13' },
    primaryStream('sess-13'),
    REPO,
    { ...NO_GIT, workspacePath: '/users/dev/projects/example' }
  );
  assert.equal(payload.workspace_path, '/users/dev/projects/example');
});

test('deferred resolution ships no workspace_path and no fingerprint', () => {
  const payload = buildTrackTickRequest(
    'SessionStart',
    { session_id: 'sess-14' },
    primaryStream('sess-14'),
    { branch: null, repo_name: null },
    { ...NO_GIT, workspacePath: null, resolution: 'deferred' }
  );
  assert.equal(payload.workspace_path, undefined);
  assert.equal(payload.workspace_fingerprint, undefined);
  assert.equal(payload.ticks[0].project_name, undefined);
});

test('resolveRepo mirrors the git context and never basename-guesses on deferral', () => {
  const named = resolveRepo(
    { cwd: '/users/dev/projects/example/sub' },
    primaryStream('sess-15'),
    { resolution: 'git', repoName: 'example', branch: 'main' }
  );
  assert.deepEqual(named, { branch: 'main', repo_name: 'example' });

  const deferred = resolveRepo(
    { cwd: '/users/dev/projects/example/sub' },
    primaryStream('sess-15'),
    { resolution: 'deferred', repoName: null, branch: null }
  );
  assert.deepEqual(deferred, { branch: null, repo_name: null });
});

test('isTrackTickProcessed rejects accepted but unprocessed responses', () => {
  assert.equal(isTrackTickProcessed({
    status: 200,
    body: JSON.stringify({ processed_count: 0, session_updated: false }),
  }), false);
  assert.equal(isTrackTickProcessed({
    status: 200,
    body: JSON.stringify({ processed_count: 1, session_updated: false }),
  }), true);
});

// --- DEV-936 F1: the payload must be a pure function of the queued envelope ---
// Envelopes queue RAW hook input, so the payload is assembled at SHIP time. The
// backend dedupes a tick on SHA-256(source|timestamp|entity|entity_type) and
// upserts on (user_id, tick_key), so `timestamp` moving with the shipper's
// clock is exactly what would make a dead-letter replay count twice. Unlike the
// Cursor plugin there is no request_key to lean on: this plugin deliberately
// ships neither run_id nor request_key (they would split the daemon's stream),
// so tick_key is the whole idempotency story and timestamp is the only part of
// it that was ever ship-derived.

const IDEMPOTENCY_ENVELOPE = {
  id: '0f8c4a1e-1111-4222-8333-444455556666',
  captured_at: '2026-08-14T07:00:00.000Z',
};

function buildTwice(hookEvent, input, stream, envelope) {
  const repo = { repo_name: 'widget', branch: 'main' };
  const gitContext = { repoUrl: null, repoFullName: null, workspaceFingerprint: null, workspacePath: null };
  const first = buildTrackTickRequest(hookEvent, input, stream, repo, gitContext, envelope);
  const second = buildTrackTickRequest(hookEvent, input, stream, repo, gitContext, envelope);
  return [first.ticks[0], second.ticks[0]];
}

test('the same envelope built twice yields an identical tick identity (DEV-936)', () => {
  const input = { session_id: 'sess-936-idem', tool_name: 'Edit', tool_input: { file_path: '/tmp/project/src/app.ts' } };
  const stream = primaryStream('sess-936-idem');

  const [first] = buildTwice('PostToolUse', input, stream, IDEMPOTENCY_ENVELOPE);
  // Rebuild after wall-clock time has moved on, which is exactly what a replay
  // hours or days later does.
  const before = Date.now;
  Date.now = () => before() + 90_000;
  let second;
  try {
    [, second] = buildTwice('PostToolUse', input, stream, IDEMPOTENCY_ENVELOPE);
  } finally {
    Date.now = before;
  }

  // These four are precisely the tick_key preimage.
  assert.equal(first.timestamp, second.timestamp, 'timestamp must not move with ship time');
  assert.equal(first.entity, second.entity);
  assert.equal(first.entity_type, second.entity_type);
  assert.equal(
    first.activity_context.ai_tool.timestamp,
    second.activity_context.ai_tool.timestamp
  );
});

test('the tick is stamped at capture time, not at reconnect time (DEV-936)', () => {
  const input = {
    session_id: 'sess-936-idem',
    tool_name: 'Edit',
    tool_input: { file_path: '/tmp/project/src/app.ts' },
    devclocked_capture: { captured_at: '2026-08-14T07:00:00.000Z', remote: false, bridge_session_id: null },
  };
  const stream = primaryStream('sess-936-idem');

  const [tick] = buildTwice('PostToolUse', input, stream, IDEMPOTENCY_ENVELOPE);

  // A six-day-old replayed tick must be recorded when it happened.
  assert.equal(tick.timestamp, '2026-08-14T07:00:00.000Z');
  assert.equal(tick.activity_context.ai_tool.timestamp, '2026-08-14T07:00:00.000Z');
});

test('an envelope carries the capture instant when the hook input lost it (DEV-936)', () => {
  // A replayed envelope whose input predates devclocked_capture still has to be
  // stamped from the queue, never from the shipper's clock.
  const input = { session_id: 'sess-936-no-capture', tool_name: 'Bash' };
  const stream = primaryStream('sess-936-no-capture');

  const [tick] = buildTwice('PostToolUse', input, stream, IDEMPOTENCY_ENVELOPE);

  assert.equal(tick.timestamp, IDEMPOTENCY_ENVELOPE.captured_at);
});

test('the hook capture instant wins over the enqueue instant (DEV-936)', () => {
  // devclocked_capture is stamped inside the hook process; captured_at is
  // stamped a moment later at enqueue. The earlier, more precise one wins.
  const input = {
    session_id: 'sess-936-precedence',
    tool_name: 'Bash',
    devclocked_capture: { captured_at: '2026-08-14T07:00:00.000Z' },
  };
  const stream = primaryStream('sess-936-precedence');

  const [tick] = buildTwice('PostToolUse', input, stream, { id: 'x', captured_at: '2026-08-14T09:99:99.999Z' });

  assert.equal(tick.timestamp, '2026-08-14T07:00:00.000Z');
});

test('an envelope-less build still works and falls back to ship time (DEV-936)', () => {
  const input = { session_id: 'sess-936-idem', tool_name: 'Bash' };
  const stream = primaryStream('sess-936-idem');

  const [tick] = buildTwice('PostToolUse', input, stream, undefined);

  // The legacy 5-arg call must not throw.
  assert.ok(Number.isFinite(Date.parse(tick.timestamp)));
});
