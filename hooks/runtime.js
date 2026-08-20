#!/usr/bin/env node

const path = require('path');
const { createPluginRuntime } = require('../runtime/core');

const runtime = createPluginRuntime({
  namespace: 'claude-hook',
  source: 'claude-plugin',
  shipperPath: path.join(__dirname, 'ship.js'),
});

const WRITE_TOOLS = new Set(['Edit', 'Write', 'NotebookEdit']);
const READ_TOOLS = new Set(['Read', 'Glob', 'Grep']);
const NON_MODEL_SENTINELS = new Set(['default', 'auto', 'inherit', '']);
const SUBAGENT_EVENTS = new Set(['SubagentStart', 'SubagentStop']);

function normalizedToolName(input) {
  return runtime.firstOpaqueId(input.tool_name, input.tool?.name) || null;
}

function toolFilePath(input) {
  const filePath = input.tool_input?.file_path || input.tool_input?.notebook_path;
  return typeof filePath === 'string' && filePath.trim() ? filePath : null;
}

function resolveModel(rawModel) {
  const raw = typeof rawModel === 'string' ? rawModel.trim() : '';
  if (!raw) return null;
  if (NON_MODEL_SENTINELS.has(raw.toLowerCase())) return null;
  return raw;
}

function inferModelProvider(model) {
  if (!model) return null;
  const m = model.toLowerCase();
  if (m.includes('claude') || m.includes('fable') || m.includes('opus') || m.includes('sonnet') || m.includes('haiku')) {
    return 'anthropic';
  }
  if (m.startsWith('gpt') || m.startsWith('o1') || m.startsWith('o3') || m.startsWith('o4') || m.includes('codex')) {
    return 'openai';
  }
  if (m.includes('gemini')) return 'google';
  if (m.includes('grok')) return 'xai';
  if (m.includes('deepseek')) return 'deepseek';
  return null;
}

function resolveExecutionContext(input) {
  const capture = input.devclocked_capture || {};
  const remote = capture.remote === true;
  return {
    execution_environment: remote ? 'local_remote_control' : 'local',
    control_surface: remote ? 'unknown' : 'desktop',
    bridge_session_id: runtime.firstOpaqueId(capture.bridge_session_id) || null,
  };
}

function resolveStream(hookEvent, input) {
  const sessionId = runtime.firstOpaqueId(input.session_id) || 'unknown';
  const agentId = runtime.firstOpaqueId(input.agent_id);

  if (SUBAGENT_EVENTS.has(hookEvent) && agentId) {
    return {
      sessionId,
      agentId,
      streamId: `${sessionId}:${agentId}`,
      rootStreamId: sessionId,
      parentStreamId: sessionId,
      throttleId: `${sessionId}:${agentId}`,
      isSubagent: true,
      agentType: typeof input.agent_type === 'string' ? input.agent_type : null,
    };
  }

  return {
    sessionId,
    agentId: null,
    streamId: sessionId,
    rootStreamId: sessionId,
    parentStreamId: null,
    throttleId: sessionId,
    isSubagent: false,
    agentType: null,
  };
}

// Naming is decided entirely by the identity ladder in resolveGitContext —
// a deferred resolution must ship the tick with NO project name rather than
// fall back to a basename guess (DEV-674).
function resolveRepo(input, stream, gitContext) {
  if (gitContext.resolution === 'deferred') return { branch: null, repo_name: null };
  return {
    branch: gitContext.branch || null,
    repo_name: gitContext.repoName || null,
  };
}

// Model is only present on SessionStart input; persist it on the session's
// stream state so every later event in the session can stamp it.
function rememberSessionModel(stream, input) {
  const model = resolveModel(input.model);
  if (!model) return;
  const state = runtime.getStreamState(stream.sessionId) || {};
  if (state.model === model) return;
  state.model = model;
  runtime.saveStreamState(stream.sessionId, state);
}

function sessionModel(stream, input) {
  const fromInput = resolveModel(input.model);
  if (fromInput) return fromInput;
  const state = runtime.getStreamState(stream.sessionId);
  return resolveModel(state?.model) || null;
}

function classifyActivity(hookEvent, input, stream) {
  const toolName = normalizedToolName(input);

  switch (hookEvent) {
    case 'SessionStart':
      return { activity_type: 'coding', sub_type: 'session_start' };
    case 'UserPromptSubmit':
      return { activity_type: 'planning', sub_type: 'prompt_submit' };
    case 'PostToolUse':
      if (toolName && WRITE_TOOLS.has(toolName)) return { activity_type: 'coding', sub_type: 'file_edit' };
      if (toolName && READ_TOOLS.has(toolName)) return { activity_type: 'reading', sub_type: 'file_read' };
      if (toolName === 'Bash') return { activity_type: 'coding', sub_type: 'bash' };
      if (toolName === 'Task') return { activity_type: 'planning', sub_type: 'task_spawn' };
      if (toolName === 'TodoWrite') return { activity_type: 'planning', sub_type: 'todo' };
      return { activity_type: 'coding', sub_type: 'tool_use' };
    case 'Stop':
    case 'SessionEnd': {
      // Don't fabricate a "coding" tick at turn/session close (DEV-530 class
      // bug) — reuse the last real classification for this stream, if any.
      const priorState = stream ? runtime.getStreamState(stream.throttleId) : null;
      const subType = hookEvent === 'SessionEnd' ? 'session_end' : 'turn_complete';
      if (priorState && priorState.last_activity_type) {
        return { activity_type: priorState.last_activity_type, sub_type: subType };
      }
      return { activity_type: undefined, sub_type: subType };
    }
    case 'SubagentStart':
      return { activity_type: 'planning', sub_type: 'stream_start' };
    case 'SubagentStop':
      return { activity_type: 'coding', sub_type: 'stream_end' };
    default:
      return { activity_type: 'coding', sub_type: String(hookEvent || 'unknown').toLowerCase() };
  }
}

// Ship-time values used to leak into every tick. The dead-letter replay
// (DEV-936) makes re-sending a queued envelope routine, and the backend dedupes
// a tick on SHA-256(source|timestamp|entity|entity_type) — so if `timestamp`
// moved with the shipper's clock, the same envelope replayed would hash to a
// new tick_key and be counted twice. It is now a pure function of the envelope:
// the hook's own capture instant, then the enqueue instant, and only then the
// wall clock. A tick replayed six days later is recorded when it happened
// rather than at reconnect time.
function tickInstant(input, envelope) {
  const captured = input?.devclocked_capture?.captured_at;
  if (typeof captured === 'string' && Number.isFinite(Date.parse(captured))) return captured;
  const enqueued = Date.parse(envelope?.captured_at);
  if (Number.isFinite(enqueued)) return new Date(enqueued).toISOString();
  return new Date().toISOString();
}

function buildTrackTickRequest(hookEvent, input, stream, repo, gitContext, envelope) {
  const now = tickInstant(input, envelope);
  const toolName = normalizedToolName(input);

  let entity = `claude://session/${stream.sessionId}`;
  let entityType = 'window';
  let isWrite = false;

  if (hookEvent === 'PostToolUse') {
    const filePath = toolFilePath(input);
    if (toolName && WRITE_TOOLS.has(toolName) && filePath) {
      entity = filePath;
      entityType = 'file';
      isWrite = true;
    } else if (toolName === 'Read' && filePath) {
      entity = filePath;
      entityType = 'file';
    } else {
      entity = `claude://tool/${toolName || 'unknown'}`;
      if (toolName && WRITE_TOOLS.has(toolName)) isWrite = true;
    }
  } else if (SUBAGENT_EVENTS.has(hookEvent)) {
    entity = `claude://agent/${stream.agentType || 'agent'}/${stream.agentId || stream.streamId}`;
  }

  const activity = classifyActivity(hookEvent, input, stream);
  const model = sessionModel(stream, input);
  const modelProvider = inferModelProvider(model);
  const execution = resolveExecutionContext(input);

  const workSignature = {
    read_count: activity.activity_type === 'reading' ? 1 : 0,
    write_count: isWrite ? 1 : 0,
    exec_count: activity.sub_type === 'bash' ? 1 : 0,
    plan_count: activity.activity_type === 'planning' ? 1 : 0,
    total_turns: 1,
  };

  const sessionFileId = stream.sessionId !== 'unknown' ? stream.sessionId : undefined;
  // Stream identity note: the daemon parses Claude Code transcripts and keys
  // its streams as `claude-code:<sessionId>[:agentId]`. The backend prefers
  // ai_tool.run_id (then stream_id) when deriving stream identity, so this
  // plugin must NOT ship run_id and must keep stream_id on the daemon's
  // convention — otherwise plugin ticks and daemon ticks split into two
  // streams instead of merging.
  const streamId = `claude-code:${stream.sessionId}${stream.isSubagent && stream.agentId ? `:${stream.agentId}` : ''}`;
  const rootStreamId = `claude-code:${stream.sessionId}`;

  const tick = {
    entity,
    entity_type: entityType,
    timestamp: now,
    is_write: isWrite,
    project_name: repo.repo_name || undefined,
    branch: repo.branch || undefined,
    repo_url: gitContext.repoUrl || undefined,
    repository_full_name: gitContext.repoFullName || undefined,
    repos: gitContext.repoFullName ? { full_name: gitContext.repoFullName } : undefined,
    activity_context: {
      ai_tool: {
        tool: 'claude-code',
        activity_type: activity.activity_type,
        work_signature: workSignature,
        summary: `Claude Code ${activity.sub_type}`,
        timestamp: now,
        model: model || undefined,
        model_provider: modelProvider || undefined,
        session_file_id: sessionFileId,
        agent_id: stream.isSubagent ? stream.agentId : undefined,
        // Registry type name ('Explore', 'general-purpose') — only SubagentStart
        // carries it, and one tick is enough for the stream to be named (DEV-816).
        agent_type: stream.agentType || undefined,
        is_sidechain: stream.isSubagent,
        stream_id: streamId,
        parent_stream_id: stream.isSubagent ? rootStreamId : undefined,
        root_stream_id: rootStreamId,
        stream_role: stream.isSubagent ? 'sidechain' : 'primary',
        execution_environment: execution.execution_environment,
        control_surface: execution.control_surface,
        bridge_session_id: execution.bridge_session_id || undefined,
        ai_tool_version: 1,
        // Intentionally no run_id (would split the daemon's stream), and no
        // runtime_ms / token_usage / measurement fields — the daemon owns
        // precise runtime + token measurement from transcripts; shipping
        // estimates here would double-count when both trackers run.
      },
    },
  };

  const request = { ticks: [tick] };
  if (gitContext.workspaceFingerprint) {
    request.workspace_fingerprint = gitContext.workspaceFingerprint;
  }
  // Absolute path of the resolved workspace root (git root, or session cwd
  // for non-git projects), sent alongside the one-way fingerprint hash so the
  // backend can do directory-containment checks even when naming is deferred
  // (DEV-551 contract, same field the daemon and cursor-plugin send).
  if (gitContext.workspacePath) {
    request.workspace_path = gitContext.workspacePath;
  }
  return request;
}

module.exports = {
  ...runtime,
  buildTrackTickRequest,
  classifyActivity,
  inferModelProvider,
  normalizedToolName,
  rememberSessionModel,
  tickInstant,
  resolveExecutionContext,
  resolveModel,
  resolveRepo,
  resolveStream,
  sessionModel,
};
