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
