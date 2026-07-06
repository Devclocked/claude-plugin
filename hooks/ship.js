#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { runShipper } = require('../runtime/ship');
const runtime = require('./runtime');
const {
  MAX_SHIP_ATTEMPTS,
  appendLog,
  buildTrackTickRequest,
  callEdgeFunction,
  classifyActivity,
  discardEnvelope,
  getStreamState,
  markEnvelopeRetry,
  readJsonFile,
  rememberSessionModel,
  removeStreamState,
  resolveGitContext,
  resolveRepo,
  resolveStream,
  saveStreamState,
  shouldRetryEnvelope,
  shouldThrottle,
} = runtime;

// Unlike Codex, Claude Code's Stop fires at the end of every assistant turn —
// only SessionStart/SessionEnd are true lifecycle boundaries.
function isLifecycleEvent(hookEvent) {
  return ['SessionStart', 'SessionEnd'].includes(hookEvent);
}

// True when the newly classified tick's activity type differs from the last
// one shipped for this stream. Lets transitions through the 30s throttle
// window instead of hard-dropping them.
function isActivityTypeTransition(priorState, newActivityType) {
  return Boolean(priorState?.last_activity_type) && priorState.last_activity_type !== newActivityType;
}

function initializeLifecycleState(hookEvent, stream, input) {
  if (hookEvent === 'SessionStart') {
    const prior = getStreamState(stream.sessionId) || {};
    saveStreamState(stream.sessionId, {
      ...prior,
      started_at: prior.started_at || Date.now(),
      last_tick_at: prior.last_tick_at || null,
      root_stream_id: stream.rootStreamId,
      source: input.source || null,
    });
  }
  rememberSessionModel(stream, input);
}

async function processEnvelope(filePath, apiKey) {
  const envelope = readJsonFile(filePath);
  if (!shouldRetryEnvelope(envelope)) return;

  const input = envelope.input || {};
  const hookEvent = input.hook_event_name;
  if (!hookEvent) {
    discardEnvelope(filePath, envelope, 'missing_hook_event_name');
    return;
  }

  const stream = resolveStream(hookEvent, input);
  initializeLifecycleState(hookEvent, stream, input);

  const throttleStateId = stream.throttleId;
  if (!isLifecycleEvent(hookEvent) && shouldThrottle(throttleStateId)) {
    const priorState = getStreamState(throttleStateId);
    const newActivity = classifyActivity(hookEvent, input, stream);
    if (!isActivityTypeTransition(priorState, newActivity.activity_type)) {
      discardEnvelope(filePath, envelope, 'throttled');
      return;
    }
  }

  const gitContext = resolveGitContext(input);
  const repo = resolveRepo(input, stream, gitContext);
  const payload = buildTrackTickRequest(hookEvent, input, stream, repo, gitContext);

  try {
    const response = await callEdgeFunction(apiKey, 'track-tick', payload);
    if (!runtime.isTrackTickProcessed(response)) {
      discardEnvelope(filePath, envelope, 'track_tick_unprocessed');
      appendLog('shipper', 'Dropping hook event because track-tick processed no activity', {
        file: path.basename(filePath),
        hook_event_name: hookEvent,
      });
      return;
    }

    if (!isLifecycleEvent(hookEvent)) {
      const state = getStreamState(throttleStateId) || {};
      state.last_tick_at = Date.now();
      state.last_activity_type =
        payload.ticks[0]?.activity_context?.ai_tool?.activity_type || state.last_activity_type;
      saveStreamState(throttleStateId, state);
    }

    runtime.recordPluginActivity({
      workspaceFingerprint: gitContext.workspaceFingerprint,
      rootStreamId: stream.rootStreamId,
      streamId: stream.streamId,
      sessionFileId: payload.ticks[0]?.activity_context?.ai_tool?.session_file_id || null,
      observedAt: payload.ticks[0]?.timestamp,
    });

    if (hookEvent === 'SessionEnd') {
      removeStreamState(stream.sessionId);
    }
    if (hookEvent === 'SubagentStop') {
      removeStreamState(stream.throttleId);
    }

    fs.unlinkSync(filePath);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'unknown_error';
    if ((envelope.attempts || 0) + 1 >= MAX_SHIP_ATTEMPTS) {
      discardEnvelope(filePath, envelope, `max_attempts:${message}`);
      return;
    }
    markEnvelopeRetry(filePath, envelope, message);
    appendLog('shipper', 'Queued hook event failed to send', {
      file: path.basename(filePath),
      hook_event_name: hookEvent,
      attempts: (envelope.attempts || 0) + 1,
      error: message,
    });
  }
}

if (require.main === module) {
  runShipper(runtime, processEnvelope);
}

module.exports = { isActivityTypeTransition, isLifecycleEvent, processEnvelope };
