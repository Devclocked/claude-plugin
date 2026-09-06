#!/usr/bin/env node
var __getOwnPropNames = Object.getOwnPropertyNames;
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};

// packages/plugin-runtime/core.js
var require_core = __commonJS({
  "packages/plugin-runtime/core.js"(exports2, module2) {
    var fs = require("fs");
    var path = require("path");
    var os = require("os");
    var https = require("https");
    var { execSync } = require("child_process");
    var { createHash, randomUUID } = require("crypto");
    var SUPABASE_URL = "https://api.devclocked.com";
    var SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhhcWZna2ttZWdseXJ1bG1waXN0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTIwMDYyODcsImV4cCI6MjA2NzU4MjI4N30.fTonLdDRqqtV44tBcl0Z7ryvaSD5Gczy-OTkzHUw0o4";
    var TICK_INTERVAL_MS = 3e4;
    var LOCK_STALE_MS = 6e4;
    var GIT_CACHE_TTL_MS = 6e4;
    var MAX_SHIP_ATTEMPTS = 5;
    var RETRY_BACKOFF_MS = 15e3;
    var DEAD_LETTER_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1e3;
    var DEAD_LETTER_MAX_BYTES = 5 * 1024 * 1024;
    var DEAD_LETTER_REPLAY_LIMIT = 25;
    var PLUGIN_ACTIVITY_RETENTION_MS = 72 * 60 * 60 * 1e3;
    var MAX_PLUGIN_ACTIVITY_ENTRIES = 1e3;
    var STREAM_STATE_TTL_MS = 6 * 60 * 60 * 1e3;
    function resolveHomeDir() {
      try {
        const home = os.homedir();
        return typeof home === "string" ? home : "";
      } catch {
        return "";
      }
    }
    function devclockedHome() {
      return path.join(resolveHomeDir() || "~", ".config", "devclocked");
    }
    function readPluginVersion(shipperPath, manifestPath) {
      try {
        const root = path.dirname(path.dirname(shipperPath));
        const resolved = manifestPath || path.join(root, "package.json");
        const manifest = JSON.parse(fs.readFileSync(resolved, "utf-8"));
        return typeof manifest.version === "string" && manifest.version ? manifest.version : "unknown";
      } catch {
        return "unknown";
      }
    }
    function ensureDir(dirPath) {
      fs.mkdirSync(dirPath, { recursive: true, mode: 448 });
      try {
        fs.chmodSync(dirPath, 448);
      } catch {
      }
    }
    function safeId(value) {
      return String(value).replace(/[^a-zA-Z0-9_-]/g, "_");
    }
    function writeJsonFile(filePath, value) {
      ensureDir(path.dirname(filePath));
      const tmpPath = `${filePath}.${process.pid}.tmp`;
      try {
        fs.writeFileSync(tmpPath, JSON.stringify(value, null, 2), { mode: 384 });
        try {
          fs.chmodSync(tmpPath, 384);
        } catch {
        }
        fs.renameSync(tmpPath, filePath);
      } catch (error) {
        try {
          fs.unlinkSync(tmpPath);
        } catch {
        }
        throw error;
      }
    }
    function sanitizeRepoUrl(url) {
      if (!url) return url;
      const raw = String(url).trim();
      if (!raw) return raw;
      try {
        const parsed = new URL(raw);
        if (parsed.username || parsed.password) {
          parsed.username = "";
          parsed.password = "";
          return parsed.toString();
        }
        return raw;
      } catch {
        return raw.replace(/\/\/[^@/]+@/, "//");
      }
    }
    var HOOK_INPUT_SCALAR_FIELDS = [
      "hook_event_name",
      "timestamp",
      // stream / session identity
      "session_id",
      "conversation_id",
      "parent_conversation_id",
      "thread_id",
      "turn_id",
      "prompt_id",
      "request_id",
      "call_id",
      "tool_call_id",
      "message_id",
      "id",
      "generation_id",
      "interaction_id",
      "composer_id",
      "subagent_id",
      // classification / repo hints
      "tool_name",
      "file_path",
      "git_branch",
      "model",
      "subagent_type",
      "task",
      "is_parallel_worker",
      // git-context working-dir hint
      "cwd"
    ];
    function newlinePlaceholder(value) {
      const count = typeof value === "string" ? value.split("\n").length : 1;
      return "\n".repeat(Math.max(0, count - 1));
    }
    function sanitizeHookInput(input) {
      if (!input || typeof input !== "object") return input;
      const clean = {};
      for (const key of HOOK_INPUT_SCALAR_FIELDS) {
        if (input[key] !== void 0) clean[key] = input[key];
      }
      if (input.tool && typeof input.tool === "object") {
        clean.tool = { name: input.tool.name };
      }
      if (input.payload && typeof input.payload === "object") {
        clean.payload = { tool_name: input.payload.tool_name, name: input.payload.name };
      }
      if (input.tool_input && typeof input.tool_input === "object") {
        clean.tool_input = { file_path: input.tool_input.file_path };
      }
      if (input.devclocked_capture && typeof input.devclocked_capture === "object") {
        const capture = {};
        for (const [key, value] of Object.entries(input.devclocked_capture)) {
          if (value === null || ["string", "number", "boolean"].includes(typeof value)) {
            capture[key] = value;
          }
        }
        clean.devclocked_capture = capture;
      }
      if (Array.isArray(input.workspace_roots)) {
        clean.workspace_roots = input.workspace_roots.filter((r) => typeof r === "string");
      }
      if (Array.isArray(input.modified_files)) {
        clean.modified_files = input.modified_files.filter((r) => typeof r === "string").slice(0, 1);
      }
      if (typeof input.command === "string") {
        clean.command = input.command.split(/\s/)[0];
      }
      if (Array.isArray(input.edits)) {
        clean.edits = input.edits.map((edit) => ({
          new_string: newlinePlaceholder(edit && edit.new_string),
          old_string: newlinePlaceholder(edit && edit.old_string)
        }));
      }
      return clean;
    }
    function readJsonFile(filePath) {
      return JSON.parse(fs.readFileSync(filePath, "utf-8"));
    }
    function normalizeOpaqueId(value) {
      if (value === null || value === void 0) return null;
      const lines = String(value).split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
      if (lines.length === 0) return null;
      const preferredCallId = lines.find((line) => line.startsWith("call_"));
      if (preferredCallId) return preferredCallId;
      return lines[0];
    }
    function firstOpaqueId(...values) {
      for (const value of values) {
        const normalized = normalizeOpaqueId(value);
        if (normalized) return normalized;
      }
      return null;
    }
    function createPluginRuntime(options) {
      const namespace = options.namespace;
      const source = options.source;
      const shipperPath = options.shipperPath;
      const execSyncImpl = options.execSyncImpl || execSync;
      const pluginVersion = options.pluginVersion || readPluginVersion(shipperPath, options.manifestPath);
      const DEVCLOCKED_HOME = devclockedHome();
      const CLI_CONFIG_PATH = path.join(DEVCLOCKED_HOME, "cli.json");
      const PLUGIN_ACTIVITY_DIR = path.join(DEVCLOCKED_HOME, "plugin-activity");
      const STATE_DIR = path.join(DEVCLOCKED_HOME, `${namespace}-state`);
      const QUEUE_DIR = path.join(DEVCLOCKED_HOME, `${namespace}-queue`);
      const LOG_DIR = path.join(DEVCLOCKED_HOME, `${namespace}-logs`);
      const GIT_CACHE_DIR = path.join(DEVCLOCKED_HOME, `${namespace}-cache`);
      const DEAD_LETTER_DIR = path.join(DEVCLOCKED_HOME, `${namespace}-dead-letter`);
      const QUARANTINE_DIR = path.join(DEVCLOCKED_HOME, `${namespace}-corrupt`);
      const SHIPPER_LOCK_PATH = path.join(QUEUE_DIR, "shipper.lock");
      const PLUGIN_ACTIVITY_PATH = path.join(PLUGIN_ACTIVITY_DIR, `${source}.json`);
      function appendLog(name, message, extra) {
        try {
          ensureDir(LOG_DIR);
          const entry = {
            timestamp: (/* @__PURE__ */ new Date()).toISOString(),
            message,
            ...extra ? { extra } : {}
          };
          fs.appendFileSync(path.join(LOG_DIR, `${name}.log`), `${JSON.stringify(entry)}
`);
        } catch {
        }
      }
      function loadAuth() {
        try {
          const config = readJsonFile(CLI_CONFIG_PATH);
          return config.api_key || null;
        } catch {
          return null;
        }
      }
      function getStreamState(streamId) {
        try {
          return readJsonFile(path.join(STATE_DIR, `stream_${safeId(streamId)}.json`));
        } catch {
          return null;
        }
      }
      function saveStreamState(streamId, state) {
        writeJsonFile(path.join(STATE_DIR, `stream_${safeId(streamId)}.json`), state);
      }
      function removeStreamState(streamId) {
        try {
          fs.unlinkSync(path.join(STATE_DIR, `stream_${safeId(streamId)}.json`));
        } catch {
        }
      }
      function shouldThrottle(streamId) {
        const state = getStreamState(streamId);
        if (!state || !state.last_tick_at) return false;
        return Date.now() - state.last_tick_at < TICK_INTERVAL_MS;
      }
      function pruneStaleStreamState(now = Date.now(), ttlMs = STREAM_STATE_TTL_MS) {
        let removed = 0;
        let files;
        try {
          files = fs.readdirSync(STATE_DIR);
        } catch {
          return 0;
        }
        for (const name of files) {
          if (!name.startsWith("stream_") || !name.endsWith(".json")) continue;
          const filePath = path.join(STATE_DIR, name);
          let stale = true;
          try {
            const state = readJsonFile(filePath);
            const lastSeen = state.last_tick_at || state.started_at;
            stale = !lastSeen || now - lastSeen > ttlMs;
          } catch {
            stale = true;
          }
          if (!stale) continue;
          try {
            fs.unlinkSync(filePath);
            removed += 1;
          } catch {
          }
        }
        return removed;
      }
      function toAbsoluteDir(maybePath) {
        if (!maybePath || typeof maybePath !== "string") return null;
        const candidate = path.isAbsolute(maybePath) ? maybePath : path.join(resolveHomeDir() || "/", maybePath);
        try {
          const stat = fs.statSync(candidate);
          if (stat.isDirectory()) return candidate;
          return path.dirname(candidate);
        } catch {
          return null;
        }
      }
      const GIT_PATH_FALLBACK = "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin";
      function classifyGitFailure(error) {
        if (!error || typeof error !== "object") return "git_unavailable";
        if (error.code === "ETIMEDOUT" || error.signal === "SIGTERM" || error.killed === true) {
          return "timeout";
        }
        if (error.code === "ENOENT" || error.code === "EAGAIN") return "git_unavailable";
        if (typeof error.status === "number") {
          if (error.status === 127 || error.status === 126) return "git_unavailable";
          const stderr = String(error.stderr || "");
          if (error.status === 128 && /not a git repository/i.test(stderr)) return "not_a_repo";
          return "git_error";
        }
        return "git_unavailable";
      }
      function gitExecClassified(cwd, command) {
        try {
          const stdout = execSyncImpl(command, {
            cwd,
            timeout: 3e3,
            encoding: "utf-8",
            stdio: ["pipe", "pipe", "pipe"],
            env: {
              ...process.env,
              PATH: `${process.env.PATH || ""}:${GIT_PATH_FALLBACK}`
            }
          }).trim();
          return { ok: true, stdout };
        } catch (error) {
          return { ok: false, failure: classifyGitFailure(error) };
        }
      }
      function gitExec(cwd, command) {
        const result = gitExecClassified(cwd, command);
        return result.ok ? result.stdout : null;
      }
      function parseRepoFullName(repoUrl) {
        if (!repoUrl) return null;
        let match = repoUrl.match(/^git@[^:]+:([^/]+)\/(.+?)(?:\.git)?$/i);
        if (!match) match = repoUrl.match(/^https?:\/\/[^/]+\/([^/]+)\/(.+?)(?:\.git)?(?:\/)?$/i);
        if (!match) match = repoUrl.match(/^ssh:\/\/git@[^/]+\/([^/]+)\/(.+?)(?:\.git)?$/i);
        if (!match) return null;
        return `${match[1]}/${match[2]}`.toLowerCase();
      }
      function getGitCachePath(workingDir) {
        try {
          const resolved = fs.realpathSync(workingDir).replace(/\/+$/, "").toLowerCase();
          const cacheKey = createHash("sha256").update(resolved).digest("hex");
          return path.join(GIT_CACHE_DIR, `${cacheKey}.json`);
        } catch {
          return null;
        }
      }
      function loadCachedGitContext(workingDir) {
        const cachePath = getGitCachePath(workingDir);
        if (!cachePath) return null;
        try {
          const cached = readJsonFile(cachePath);
          if (!cached.cached_at || Date.now() - cached.cached_at > GIT_CACHE_TTL_MS) {
            return null;
          }
          return cached.git_context || null;
        } catch {
          return null;
        }
      }
      function saveCachedGitContext(workingDir, gitContext) {
        const cachePath = getGitCachePath(workingDir);
        if (!cachePath) return;
        writeJsonFile(cachePath, {
          cached_at: Date.now(),
          git_context: gitContext
        });
      }
      function fingerprintPath(dirPath) {
        try {
          const resolved = fs.realpathSync(dirPath).replace(/\/+$/, "").toLowerCase();
          return createHash("sha256").update(resolved).digest("hex");
        } catch {
          return null;
        }
      }
      function deferredGitContext(workspacePath, failure) {
        return {
          workspaceFingerprint: null,
          repoUrl: null,
          repoFullName: null,
          repoName: null,
          branch: null,
          gitRoot: null,
          workspacePath: workspacePath || null,
          resolution: "deferred",
          resolutionFailure: failure || null
        };
      }
      function buildRepoGitContext(gitRoot) {
        const remoteResult = gitExecClassified(gitRoot, "git remote get-url origin");
        const remoteProbeDegraded = !remoteResult.ok && remoteResult.failure !== "git_error";
        const repoUrl = sanitizeRepoUrl(remoteResult.ok ? remoteResult.stdout : null);
        const repoFullName = parseRepoFullName(repoUrl);
        const branch = gitExec(gitRoot, "git rev-parse --abbrev-ref HEAD");
        const repoName = repoFullName ? repoFullName.split("/").pop() : path.basename(gitRoot);
        return {
          workspaceFingerprint: fingerprintPath(gitRoot),
          repoUrl: repoUrl || null,
          repoFullName,
          repoName: repoName || null,
          branch: branch || null,
          gitRoot,
          workspacePath: gitRoot,
          resolution: "git",
          resolutionFailure: null,
          ...remoteProbeDegraded ? { remoteProbeDegraded: true } : {}
        };
      }
      function isGuardedRoot(dirPath) {
        let resolved = dirPath;
        try {
          resolved = fs.realpathSync(dirPath);
        } catch {
        }
        const normalized = resolved.replace(/\/+$/, "") || "/";
        let home = resolveHomeDir();
        try {
          if (home) home = fs.realpathSync(home);
        } catch {
        }
        home = home.replace(/\/+$/, "");
        return normalized === "/" || Boolean(home) && normalized === home;
      }
      function resolveGitContext(input) {
        const roots = Array.isArray(input.workspace_roots) ? input.workspace_roots : [];
        const remote = input.devclocked_capture?.remote === true;
        const sessionDir = toAbsoluteDir(input.cwd) || toAbsoluteDir(roots[0]);
        const fileDirCandidates = [
          input.file_path,
          input.tool_input?.file_path,
          Array.isArray(input.modified_files) ? input.modified_files[0] : null
        ];
        let fileDir = null;
        for (const candidate of fileDirCandidates) {
          const abs = toAbsoluteDir(candidate);
          if (abs) {
            fileDir = abs;
            break;
          }
        }
        const identityDir = sessionDir || fileDir;
        if (!identityDir) return deferredGitContext(null, "no_working_dir");
        const cached = loadCachedGitContext(identityDir);
        if (cached && cached.resolution) return cached;
        const rootResult = gitExecClassified(identityDir, "git rev-parse --show-toplevel");
        if (rootResult.ok) {
          const gitContext2 = buildRepoGitContext(rootResult.stdout);
          if (!gitContext2.remoteProbeDegraded) saveCachedGitContext(identityDir, gitContext2);
          return gitContext2;
        }
        if (rootResult.failure !== "not_a_repo") {
          appendLog("shipper", "Deferring project identity because git could not run", {
            failure: rootResult.failure,
            dir: identityDir,
            path_env: process.env.PATH || null
          });
          return deferredGitContext(sessionDir, rootResult.failure);
        }
        if (fileDir && fileDir !== identityDir) {
          const fileRootResult = gitExecClassified(fileDir, "git rev-parse --show-toplevel");
          if (fileRootResult.ok) {
            const gitContext2 = buildRepoGitContext(fileRootResult.stdout);
            if (!gitContext2.remoteProbeDegraded) saveCachedGitContext(identityDir, gitContext2);
            return gitContext2;
          }
        }
        if (!sessionDir || remote || isGuardedRoot(sessionDir)) {
          return deferredGitContext(sessionDir, remote ? "remote_non_git" : "unnameable_dir");
        }
        const gitContext = {
          workspaceFingerprint: fingerprintPath(sessionDir),
          repoUrl: null,
          repoFullName: null,
          repoName: path.basename(sessionDir) || null,
          branch: null,
          gitRoot: null,
          workspacePath: sessionDir,
          resolution: "cwd",
          resolutionFailure: null
        };
        saveCachedGitContext(identityDir, gitContext);
        return gitContext;
      }
      function stampPluginVersion(body) {
        if (!body || !Array.isArray(body.ticks)) return;
        for (const tick of body.ticks) {
          const aiTool = tick && tick.activity_context && tick.activity_context.ai_tool;
          if (aiTool && typeof aiTool === "object" && aiTool.plugin_version === void 0) {
            aiTool.plugin_version = pluginVersion;
          }
        }
      }
      function callEdgeFunction(apiKey, fnName, body) {
        return new Promise((resolve, reject) => {
          const url = new URL(`/functions/v1/${fnName}`, SUPABASE_URL);
          if (fnName === "track-tick") stampPluginVersion(body);
          const data = JSON.stringify(body);
          const req = https.request(
            url,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": `Bearer ${SUPABASE_ANON_KEY}`,
                "x-devclocked-key": apiKey,
                "x-devclocked-source": source,
                "x-devclocked-plugin-version": pluginVersion,
                "Content-Length": Buffer.byteLength(data)
              },
              timeout: 1e4
            },
            (res) => {
              let responseBody = "";
              res.on("data", (chunk) => responseBody += chunk);
              res.on("end", () => {
                if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
                  resolve({ status: res.statusCode, body: responseBody });
                  return;
                }
                reject(new Error(`edge_function_${res.statusCode || "unknown"}`));
              });
            }
          );
          req.on("error", reject);
          req.on("timeout", () => {
            req.destroy();
            reject(new Error("timeout"));
          });
          req.write(data);
          req.end();
        });
      }
      function isTrackTickProcessed(response) {
        try {
          const body = typeof response?.body === "string" ? JSON.parse(response.body) : response?.body;
          return body?.session_updated === true || Number(body?.processed_count || 0) > 0;
        } catch {
          return false;
        }
      }
      function nextQueueFilePath() {
        ensureDir(QUEUE_DIR);
        return path.join(QUEUE_DIR, `${Date.now()}-${process.pid}-${randomUUID()}.json`);
      }
      function enqueueHookEvent(input) {
        const envelope = {
          id: randomUUID(),
          captured_at: (/* @__PURE__ */ new Date()).toISOString(),
          attempts: 0,
          input: sanitizeHookInput(input)
        };
        const filePath = nextQueueFilePath();
        writeJsonFile(filePath, envelope);
        return filePath;
      }
      function listQueueFiles() {
        try {
          ensureDir(QUEUE_DIR);
          return fs.readdirSync(QUEUE_DIR).filter((name) => name.endsWith(".json")).sort().map((name) => path.join(QUEUE_DIR, name));
        } catch {
          return [];
        }
      }
      function isProcessAlive(pid) {
        try {
          process.kill(pid, 0);
          return true;
        } catch {
          return false;
        }
      }
      function acquireShipperLock() {
        ensureDir(QUEUE_DIR);
        try {
          const fd = fs.openSync(SHIPPER_LOCK_PATH, "wx");
          fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, started_at: Date.now() }));
          return fd;
        } catch (error) {
          if (error.code !== "EEXIST") return null;
          let reclaimable = false;
          try {
            const existing = readJsonFile(SHIPPER_LOCK_PATH);
            const stale = !existing.started_at || Date.now() - existing.started_at > LOCK_STALE_MS;
            const dead = !existing.pid || !isProcessAlive(existing.pid);
            reclaimable = stale || dead;
          } catch {
            try {
              reclaimable = Date.now() - fs.statSync(SHIPPER_LOCK_PATH).mtimeMs > LOCK_STALE_MS;
            } catch {
              return null;
            }
          }
          if (!reclaimable) return null;
          try {
            fs.unlinkSync(SHIPPER_LOCK_PATH);
          } catch {
            return null;
          }
          return acquireShipperLock();
        }
      }
      function releaseShipperLock(fd) {
        try {
          fs.closeSync(fd);
        } catch {
        }
        try {
          fs.unlinkSync(SHIPPER_LOCK_PATH);
        } catch {
        }
      }
      function markEnvelopeRetry(filePath, envelope, errorMessage) {
        envelope.attempts = (envelope.attempts || 0) + 1;
        envelope.last_error = errorMessage;
        envelope.last_attempt_at = (/* @__PURE__ */ new Date()).toISOString();
        envelope.retry_after = new Date(Date.now() + RETRY_BACKOFF_MS).toISOString();
        writeJsonFile(filePath, envelope);
      }
      function shouldRetryEnvelope(envelope) {
        if (!envelope.retry_after) return true;
        return Date.now() >= new Date(envelope.retry_after).getTime();
      }
      function discardEnvelope(filePath, envelope, reason) {
        appendLog("shipper", "Dropping queued hook event", {
          file: path.basename(filePath),
          reason,
          hook_event_name: envelope.input?.hook_event_name || null,
          attempts: envelope.attempts || 0
        });
        try {
          fs.unlinkSync(filePath);
        } catch {
        }
      }
      function listDeadLetterFiles() {
        try {
          ensureDir(DEAD_LETTER_DIR);
          return fs.readdirSync(DEAD_LETTER_DIR).filter((name) => name.endsWith(".json")).sort().map((name) => path.join(DEAD_LETTER_DIR, name));
        } catch {
          return [];
        }
      }
      function deadLetterEnvelope(filePath, envelope, reason) {
        const target = path.join(DEAD_LETTER_DIR, path.basename(filePath));
        envelope.dead_lettered_at = (/* @__PURE__ */ new Date()).toISOString();
        envelope.dead_letter_reason = reason;
        try {
          writeJsonFile(target, envelope);
          fs.unlinkSync(filePath);
        } catch (error) {
          appendLog("shipper", "Failed to dead-letter queued hook event", {
            file: path.basename(filePath),
            reason,
            error: error instanceof Error ? error.message : "unknown_error"
          });
          return null;
        }
        appendLog("shipper", "Dead-lettered queued hook event, will replay on reconnect", {
          file: path.basename(filePath),
          reason,
          hook_event_name: envelope.input?.hook_event_name || null,
          attempts: envelope.attempts || 0
        });
        return target;
      }
      function sweepStaleFiles(dirPath, { now, maxAgeMs, filter, reason }) {
        let names;
        try {
          names = fs.readdirSync(dirPath);
        } catch {
          return 0;
        }
        let removed = 0;
        for (const name of names) {
          if (!filter(name)) continue;
          const filePath = path.join(dirPath, name);
          let stats;
          try {
            stats = fs.statSync(filePath);
          } catch {
            continue;
          }
          if (now - stats.mtimeMs <= maxAgeMs) continue;
          try {
            fs.unlinkSync(filePath);
          } catch {
            continue;
          }
          removed += 1;
          appendLog("shipper", "Swept stale file", { file: name, dir: dirPath, reason });
        }
        return removed;
      }
      function pruneDeadLetter(now = Date.now(), options2 = {}) {
        const maxAgeMs = options2.maxAgeMs ?? DEAD_LETTER_MAX_AGE_MS;
        const maxBytes = options2.maxBytes ?? DEAD_LETTER_MAX_BYTES;
        const isTmp = (name) => name.endsWith(".tmp");
        sweepStaleFiles(QUEUE_DIR, { now, maxAgeMs, filter: isTmp, reason: "orphaned_tmp" });
        sweepStaleFiles(DEAD_LETTER_DIR, { now, maxAgeMs, filter: isTmp, reason: "orphaned_tmp" });
        sweepStaleFiles(QUARANTINE_DIR, { now, maxAgeMs, filter: () => true, reason: "quarantine_max_age" });
        const entries = [];
        for (const filePath of listDeadLetterFiles()) {
          let stats;
          try {
            stats = fs.statSync(filePath);
          } catch {
            continue;
          }
          let capturedAtMs = NaN;
          try {
            capturedAtMs = Date.parse(readJsonFile(filePath).captured_at);
          } catch {
          }
          if (!Number.isFinite(capturedAtMs)) capturedAtMs = stats.mtimeMs;
          entries.push({ filePath, bytes: stats.size, capturedAtMs });
        }
        let evicted = 0;
        const evict = (entry, reason, detail) => {
          try {
            fs.unlinkSync(entry.filePath);
          } catch {
            return false;
          }
          evicted += 1;
          appendLog("shipper", "Evicted dead-lettered hook event \u2014 activity permanently lost", {
            file: path.basename(entry.filePath),
            reason,
            captured_at: new Date(entry.capturedAtMs).toISOString(),
            bytes: entry.bytes,
            ...detail
          });
          return true;
        };
        const survivors = [];
        for (const entry of entries) {
          if (now - entry.capturedAtMs > maxAgeMs) {
            if (!evict(entry, "max_age", { max_age_ms: maxAgeMs })) survivors.push(entry);
            continue;
          }
          survivors.push(entry);
        }
        survivors.sort((a, b) => a.capturedAtMs - b.capturedAtMs);
        let totalBytes = survivors.reduce((sum, entry) => sum + entry.bytes, 0);
        for (const entry of survivors) {
          if (totalBytes <= maxBytes) break;
          if (evict(entry, "max_bytes", { max_bytes: maxBytes, total_bytes: totalBytes })) {
            totalBytes -= entry.bytes;
          }
        }
        return evicted;
      }
      function replayDeadLetterFile(filePath) {
        let envelope;
        try {
          envelope = readJsonFile(filePath);
        } catch (error) {
          return { queuedPath: null, quarantined: quarantineEnvelope(filePath, error) };
        }
        envelope.attempts = 0;
        delete envelope.retry_after;
        envelope.replayed_at = (/* @__PURE__ */ new Date()).toISOString();
        const target = path.join(QUEUE_DIR, path.basename(filePath));
        try {
          writeJsonFile(target, envelope);
          fs.unlinkSync(filePath);
        } catch (error) {
          appendLog("shipper", "Failed to replay dead-lettered hook event", {
            file: path.basename(filePath),
            error: error instanceof Error ? error.message : "unknown_error"
          });
          return { queuedPath: null, quarantined: false };
        }
        return { queuedPath: target, quarantined: false };
      }
      function replayDeadLetter(limit = DEAD_LETTER_REPLAY_LIMIT) {
        let replayed = 0;
        for (const filePath of listDeadLetterFiles().slice(0, limit)) {
          if (replayDeadLetterFile(filePath).queuedPath) replayed += 1;
        }
        if (replayed > 0) {
          appendLog("shipper", "Replayed dead-lettered hook events into the queue", { count: replayed });
        }
        return replayed;
      }
      function quarantineEnvelope(filePath, error) {
        if (!fs.existsSync(filePath)) return false;
        try {
          readJsonFile(filePath);
          appendLog("shipper", "Queue file failed to process but still parses \u2014 left in place", {
            file: path.basename(filePath),
            error: error instanceof Error ? error.message : "unknown_error"
          });
          return false;
        } catch {
        }
        const target = path.join(QUARANTINE_DIR, path.basename(filePath));
        let moved = false;
        try {
          ensureDir(QUARANTINE_DIR);
          fs.renameSync(filePath, target);
          moved = true;
        } catch {
          try {
            fs.unlinkSync(filePath);
          } catch {
          }
        }
        appendLog("shipper", "Quarantined unreadable queue file", {
          file: path.basename(filePath),
          moved_to: moved ? target : null,
          error: error instanceof Error ? error.message : "unknown_error"
        });
        return moved;
      }
      function wakeShipper() {
        try {
          const { spawn } = require("child_process");
          const child = spawn(process.execPath, [shipperPath], {
            detached: true,
            stdio: "ignore"
          });
          child.unref();
        } catch (error) {
          appendLog("hook", "Failed to wake shipper", {
            error: error instanceof Error ? error.message : "unknown_error"
          });
        }
      }
      function readPluginActivity() {
        try {
          const raw = readJsonFile(PLUGIN_ACTIVITY_PATH);
          const entries = Array.isArray(raw.entries) ? raw.entries : [];
          return {
            version: 1,
            entries
          };
        } catch {
          return {
            version: 1,
            entries: []
          };
        }
      }
      function recordPluginActivity(entry) {
        const observedAtMs = new Date(entry.observedAt || Date.now()).getTime();
        const cutoff = Date.now() - PLUGIN_ACTIVITY_RETENTION_MS;
        const current = readPluginActivity();
        const retained = current.entries.filter((item) => {
          const ts = new Date(item.observedAt || 0).getTime();
          return Number.isFinite(ts) && ts >= cutoff;
        });
        retained.push({
          workspaceFingerprint: entry.workspaceFingerprint || null,
          rootStreamId: entry.rootStreamId || null,
          streamId: entry.streamId || null,
          sessionFileId: entry.sessionFileId || null,
          observedAt: Number.isFinite(observedAtMs) ? new Date(observedAtMs).toISOString() : (/* @__PURE__ */ new Date()).toISOString()
        });
        const deduped = [];
        const seen = /* @__PURE__ */ new Set();
        for (const item of retained.slice(-MAX_PLUGIN_ACTIVITY_ENTRIES)) {
          const key = [
            item.workspaceFingerprint || "",
            item.rootStreamId || "",
            item.streamId || "",
            item.sessionFileId || "",
            item.observedAt || ""
          ].join("::");
          if (seen.has(key)) continue;
          seen.add(key);
          deduped.push(item);
        }
        writeJsonFile(PLUGIN_ACTIVITY_PATH, {
          version: 1,
          updated_at: (/* @__PURE__ */ new Date()).toISOString(),
          entries: deduped
        });
      }
      return {
        SUPABASE_URL,
        SUPABASE_ANON_KEY,
        CLI_CONFIG_PATH,
        DEVCLOCKED_HOME,
        STATE_DIR,
        QUEUE_DIR,
        LOG_DIR,
        GIT_CACHE_DIR,
        DEAD_LETTER_DIR,
        QUARANTINE_DIR,
        SHIPPER_LOCK_PATH,
        SHIPPER_PATH: shipperPath,
        MAX_SHIP_ATTEMPTS,
        DEAD_LETTER_MAX_AGE_MS,
        DEAD_LETTER_MAX_BYTES,
        DEAD_LETTER_REPLAY_LIMIT,
        appendLog,
        acquireShipperLock,
        callEdgeFunction,
        classifyGitFailure,
        deadLetterEnvelope,
        discardEnvelope,
        enqueueHookEvent,
        ensureDir,
        firstOpaqueId,
        getStreamState,
        isTrackTickProcessed,
        listDeadLetterFiles,
        listQueueFiles,
        loadAuth,
        markEnvelopeRetry,
        normalizeOpaqueId,
        pruneDeadLetter,
        quarantineEnvelope,
        readJsonFile,
        recordPluginActivity,
        releaseShipperLock,
        removeStreamState,
        replayDeadLetter,
        replayDeadLetterFile,
        resolveGitContext,
        saveStreamState,
        shouldRetryEnvelope,
        shouldThrottle,
        pruneStaleStreamState,
        wakeShipper,
        writeJsonFile,
        pluginVersion
      };
    }
    module2.exports = {
      MAX_SHIP_ATTEMPTS,
      DEAD_LETTER_MAX_AGE_MS,
      DEAD_LETTER_MAX_BYTES,
      DEAD_LETTER_REPLAY_LIMIT,
      createPluginRuntime
    };
  }
});

// packages/plugin-runtime/shellThreadNames.js
var require_shellThreadNames = __commonJS({
  "packages/plugin-runtime/shellThreadNames.js"(exports2, module2) {
    var fs = require("fs");
    var os = require("os");
    var path = require("path");
    var MAX_LABEL_LEN = 72;
    var MAX_SOURCE_LEN = 32;
    function shellStoreCandidates(env = process.env, home = safeHomedir()) {
      const bases = [];
      const configured = typeof env.T3CODE_HOME === "string" ? env.T3CODE_HOME.trim() : "";
      if (configured) bases.push(configured);
      if (home) bases.push(path.join(home, ".demuxx"), path.join(home, ".t3"));
      const seen = /* @__PURE__ */ new Set();
      const stores = [];
      for (const base of bases) {
        const dbPath = path.join(base, "userdata", "state.sqlite");
        if (seen.has(dbPath)) continue;
        seen.add(dbPath);
        stores.push({ path: dbPath, source: path.basename(base) === ".demuxx" ? "demuxx" : "t3code" });
      }
      return stores;
    }
    function safeHomedir() {
      try {
        const home = os.homedir();
        return typeof home === "string" ? home : "";
      } catch {
        return "";
      }
    }
    function truncateLabel(value, max = MAX_LABEL_LEN) {
      const clean = String(value).replace(/\s+/g, " ").trim();
      if (clean.length <= max) return clean;
      const cut = clean.slice(0, max - 1);
      const lastSpace = cut.lastIndexOf(" ");
      const head = lastSpace > max * 0.6 ? cut.slice(0, lastSpace) : cut;
      return `${head.trimEnd()}\u2026`;
    }
    var CLAUDE_SESSION_QUERY = `
  SELECT t.title AS title
    FROM provider_session_runtime r
    JOIN projection_threads t ON t.thread_id = r.thread_id
   WHERE lower(r.provider_name) LIKE 'claude%'
     AND json_extract(r.resume_cursor_json, '$.resume') = ?
     AND t.deleted_at IS NULL
     AND t.title IS NOT NULL
     AND trim(t.title) <> ''
     AND t.title <> coalesce(
           json_extract(
             (SELECT e.payload_json
                FROM orchestration_events e
               WHERE e.aggregate_kind = 'thread'
                 AND e.stream_id = t.thread_id
                 AND e.event_type = 'thread.created'
               ORDER BY e.sequence
               LIMIT 1),
             '$.title'),
           '')
   LIMIT 1
`;
    var CODEX_THREAD_QUERY = CLAUDE_SESSION_QUERY.replace("lower(r.provider_name) LIKE 'claude%'", "lower(r.provider_name) LIKE 'codex%'").replace("json_extract(r.resume_cursor_json, '$.resume') = ?", "json_extract(r.resume_cursor_json, '$.threadId') = ?");
    var sqliteModule;
    var sqliteUnavailable = false;
    function loadSqlite() {
      if (sqliteModule) return sqliteModule;
      if (sqliteUnavailable) return null;
      const originalEmitWarning = process.emitWarning;
      process.emitWarning = function quietSqliteWarning(warning, ...rest) {
        const text = typeof warning === "string" ? warning : warning && warning.message;
        if (typeof text === "string" && text.includes("SQLite")) return void 0;
        return originalEmitWarning.call(process, warning, ...rest);
      };
      try {
        sqliteModule = require("node:sqlite");
        return sqliteModule;
      } catch {
        sqliteUnavailable = true;
        return null;
      } finally {
        process.emitWarning = originalEmitWarning;
      }
    }
    function queryStore(store, sql, id) {
      const sqlite = loadSqlite();
      if (!sqlite) return null;
      let exists = false;
      try {
        exists = fs.statSync(store.path).isFile();
      } catch {
        return null;
      }
      if (!exists) return null;
      let db;
      try {
        db = new sqlite.DatabaseSync(store.path, { readOnly: true });
        const row = db.prepare(sql).get(id);
        const title = row && typeof row.title === "string" ? truncateLabel(row.title) : "";
        return title ? { title, source: store.source.slice(0, MAX_SOURCE_LEN) } : null;
      } catch {
        return null;
      } finally {
        try {
          if (db) db.close();
        } catch {
        }
      }
    }
    function lookup(sql, id, stores) {
      if (typeof id !== "string" || !id.trim()) return null;
      for (const store of stores) {
        const hit = queryStore(store, sql, id.trim());
        if (hit) return hit;
      }
      return null;
    }
    function lookupClaudeSession(sessionId, stores = shellStoreCandidates()) {
      return lookup(CLAUDE_SESSION_QUERY, sessionId, stores);
    }
    function lookupCodexThread(threadId, stores = shellStoreCandidates()) {
      return lookup(CODEX_THREAD_QUERY, threadId, stores);
    }
    function shellTitlesEnabled(env = process.env, configPath = defaultDaemonConfigPath()) {
      const fromEnv = typeof env.DEVCLOCKED_TRACK_SESSION_TITLES === "string" ? env.DEVCLOCKED_TRACK_SESSION_TITLES.trim().toLowerCase() : "";
      if (fromEnv) return !["0", "false", "off", "no"].includes(fromEnv);
      try {
        const config = JSON.parse(fs.readFileSync(configPath, "utf8"));
        if (config && typeof config === "object") {
          if (config.track_session_titles === false || config.trackSessionTitles === false) return false;
        }
      } catch {
      }
      return true;
    }
    function defaultDaemonConfigPath() {
      const home = safeHomedir();
      return home ? path.join(home, ".config", "devclocked", "daemon-config.json") : "";
    }
    module2.exports = {
      shellStoreCandidates,
      lookupClaudeSession,
      lookupCodexThread,
      shellTitlesEnabled,
      truncateLabel,
      CLAUDE_SESSION_QUERY,
      CODEX_THREAD_QUERY
    };
  }
});

// packages/claude-plugin/hooks/runtime.js
var require_runtime = __commonJS({
  "packages/claude-plugin/hooks/runtime.js"(exports2, module2) {
    var path = require("path");
    var { createPluginRuntime } = require_core();
    var shellThreadNames = require_shellThreadNames();
    var runtime2 = createPluginRuntime({
      namespace: "claude-hook",
      source: "claude-plugin",
      shipperPath: path.join(__dirname, "ship.js")
    });
    var WRITE_TOOLS = /* @__PURE__ */ new Set(["Edit", "Write", "NotebookEdit"]);
    var READ_TOOLS = /* @__PURE__ */ new Set(["Read", "Glob", "Grep"]);
    var NON_MODEL_SENTINELS = /* @__PURE__ */ new Set(["default", "auto", "inherit", ""]);
    var SUBAGENT_EVENTS = /* @__PURE__ */ new Set(["SubagentStart", "SubagentStop"]);
    function normalizedToolName(input) {
      return runtime2.firstOpaqueId(input.tool_name, input.tool?.name) || null;
    }
    function toolFilePath(input) {
      const filePath = input.tool_input?.file_path || input.tool_input?.notebook_path;
      return typeof filePath === "string" && filePath.trim() ? filePath : null;
    }
    function resolveModel(rawModel) {
      const raw = typeof rawModel === "string" ? rawModel.trim() : "";
      if (!raw) return null;
      if (NON_MODEL_SENTINELS.has(raw.toLowerCase())) return null;
      return raw;
    }
    function inferModelProvider(model) {
      if (!model) return null;
      const m = model.toLowerCase();
      if (m.includes("claude") || m.includes("fable") || m.includes("opus") || m.includes("sonnet") || m.includes("haiku")) {
        return "anthropic";
      }
      if (m.startsWith("gpt") || m.startsWith("o1") || m.startsWith("o3") || m.startsWith("o4") || m.includes("codex")) {
        return "openai";
      }
      if (m.includes("gemini")) return "google";
      if (m.includes("grok")) return "xai";
      if (m.includes("deepseek")) return "deepseek";
      return null;
    }
    function resolveExecutionContext(input) {
      const capture = input.devclocked_capture || {};
      const remote = capture.remote === true;
      return {
        execution_environment: remote ? "local_remote_control" : "local",
        control_surface: remote ? "unknown" : "desktop",
        bridge_session_id: runtime2.firstOpaqueId(capture.bridge_session_id) || null
      };
    }
    function resolveStream(hookEvent, input) {
      const sessionId = runtime2.firstOpaqueId(input.session_id) || "unknown";
      const agentId = runtime2.firstOpaqueId(input.agent_id);
      if (SUBAGENT_EVENTS.has(hookEvent) && agentId) {
        return {
          sessionId,
          agentId,
          streamId: `${sessionId}:${agentId}`,
          rootStreamId: sessionId,
          parentStreamId: sessionId,
          throttleId: `${sessionId}:${agentId}`,
          isSubagent: true,
          agentType: typeof input.agent_type === "string" ? input.agent_type : null
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
        agentType: null
      };
    }
    function resolveRepo(input, stream, gitContext) {
      if (gitContext.resolution === "deferred") return { branch: null, repo_name: null };
      return {
        branch: gitContext.branch || null,
        repo_name: gitContext.repoName || null
      };
    }
    function rememberSessionModel(stream, input) {
      const model = resolveModel(input.model);
      if (!model) return;
      const state = runtime2.getStreamState(stream.sessionId) || {};
      if (state.model === model) return;
      state.model = model;
      runtime2.saveStreamState(stream.sessionId, state);
    }
    function sessionModel(stream, input) {
      const fromInput = resolveModel(input.model);
      if (fromInput) return fromInput;
      const state = runtime2.getStreamState(stream.sessionId);
      return resolveModel(state?.model) || null;
    }
    function classifyActivity(hookEvent, input, stream) {
      const toolName = normalizedToolName(input);
      switch (hookEvent) {
        case "SessionStart":
          return { activity_type: "coding", sub_type: "session_start" };
        case "UserPromptSubmit":
          return { activity_type: "planning", sub_type: "prompt_submit" };
        case "PostToolUse":
          if (toolName && WRITE_TOOLS.has(toolName)) return { activity_type: "coding", sub_type: "file_edit" };
          if (toolName && READ_TOOLS.has(toolName)) return { activity_type: "reading", sub_type: "file_read" };
          if (toolName === "Bash") return { activity_type: "coding", sub_type: "bash" };
          if (toolName === "Task") return { activity_type: "planning", sub_type: "task_spawn" };
          if (toolName === "TodoWrite") return { activity_type: "planning", sub_type: "todo" };
          return { activity_type: "coding", sub_type: "tool_use" };
        case "Stop":
        case "SessionEnd": {
          const priorState = stream ? runtime2.getStreamState(stream.throttleId) : null;
          const subType = hookEvent === "SessionEnd" ? "session_end" : "turn_complete";
          if (priorState && priorState.last_activity_type) {
            return { activity_type: priorState.last_activity_type, sub_type: subType };
          }
          return { activity_type: void 0, sub_type: subType };
        }
        case "SubagentStart":
          return { activity_type: "planning", sub_type: "stream_start" };
        case "SubagentStop":
          return { activity_type: "coding", sub_type: "stream_end" };
        default:
          return { activity_type: "coding", sub_type: String(hookEvent || "unknown").toLowerCase() };
      }
    }
    function tickInstant(input, envelope) {
      const captured = input?.devclocked_capture?.captured_at;
      if (typeof captured === "string" && Number.isFinite(Date.parse(captured))) return captured;
      const enqueued = Date.parse(envelope?.captured_at);
      if (Number.isFinite(enqueued)) return new Date(enqueued).toISOString();
      return (/* @__PURE__ */ new Date()).toISOString();
    }
    var SHELL_TITLE_TTL_MS = 6e4;
    var shellTitleResolver = (sessionId) => shellThreadNames.lookupClaudeSession(sessionId);
    function shellTitleFor(stream) {
      if (stream.isSubagent || stream.sessionId === "unknown") return null;
      if (!shellThreadNames.shellTitlesEnabled()) return null;
      const state = runtime2.getStreamState(stream.sessionId) || {};
      const cached = state.shell_title;
      const nowMs = Date.now();
      if (cached && typeof cached.checked_at === "number" && nowMs - cached.checked_at < SHELL_TITLE_TTL_MS) {
        return cached.title ? { title: cached.title, source: cached.source } : null;
      }
      let hit = null;
      try {
        hit = shellTitleResolver(stream.sessionId) || null;
      } catch {
        hit = null;
      }
      state.shell_title = { title: hit?.title || null, source: hit?.source || null, checked_at: nowMs };
      runtime2.saveStreamState(stream.sessionId, state);
      return hit;
    }
    function buildTrackTickRequest(hookEvent, input, stream, repo, gitContext, envelope) {
      const now = tickInstant(input, envelope);
      const toolName = normalizedToolName(input);
      const shellTitle = shellTitleFor(stream);
      let entity = `claude://session/${stream.sessionId}`;
      let entityType = "window";
      let isWrite = false;
      if (hookEvent === "PostToolUse") {
        const filePath = toolFilePath(input);
        if (toolName && WRITE_TOOLS.has(toolName) && filePath) {
          entity = filePath;
          entityType = "file";
          isWrite = true;
        } else if (toolName === "Read" && filePath) {
          entity = filePath;
          entityType = "file";
        } else {
          entity = `claude://tool/${toolName || "unknown"}`;
          if (toolName && WRITE_TOOLS.has(toolName)) isWrite = true;
        }
      } else if (SUBAGENT_EVENTS.has(hookEvent)) {
        entity = `claude://agent/${stream.agentType || "agent"}/${stream.agentId || stream.streamId}`;
      }
      const activity = classifyActivity(hookEvent, input, stream);
      const model = sessionModel(stream, input);
      const modelProvider = inferModelProvider(model);
      const execution = resolveExecutionContext(input);
      const workSignature = {
        read_count: activity.activity_type === "reading" ? 1 : 0,
        write_count: isWrite ? 1 : 0,
        exec_count: activity.sub_type === "bash" ? 1 : 0,
        plan_count: activity.activity_type === "planning" ? 1 : 0,
        total_turns: 1
      };
      const sessionFileId = stream.sessionId !== "unknown" ? stream.sessionId : void 0;
      const streamId = `claude-code:${stream.sessionId}${stream.isSubagent && stream.agentId ? `:${stream.agentId}` : ""}`;
      const rootStreamId = `claude-code:${stream.sessionId}`;
      const tick = {
        entity,
        entity_type: entityType,
        timestamp: now,
        is_write: isWrite,
        project_name: repo.repo_name || void 0,
        branch: repo.branch || void 0,
        repo_url: gitContext.repoUrl || void 0,
        repository_full_name: gitContext.repoFullName || void 0,
        repos: gitContext.repoFullName ? { full_name: gitContext.repoFullName } : void 0,
        activity_context: {
          ai_tool: {
            tool: "claude-code",
            activity_type: activity.activity_type,
            work_signature: workSignature,
            summary: `Claude Code ${activity.sub_type}`,
            timestamp: now,
            model: model || void 0,
            model_provider: modelProvider || void 0,
            session_file_id: sessionFileId,
            agent_id: stream.isSubagent ? stream.agentId : void 0,
            // Registry type name ('Explore', 'general-purpose') — only SubagentStart
            // carries it, and one tick is enough for the stream to be named (DEV-816).
            agent_type: stream.agentType || void 0,
            // The shell's name for this session, with where it came from. Absent
            // for sessions the agent named itself (the daemon ships those) and for
            // every session when titles are switched off (DEV-1055).
            stream_title: shellTitle ? shellTitle.title : void 0,
            stream_title_source: shellTitle ? shellTitle.source : void 0,
            is_sidechain: stream.isSubagent,
            stream_id: streamId,
            parent_stream_id: stream.isSubagent ? rootStreamId : void 0,
            root_stream_id: rootStreamId,
            stream_role: stream.isSubagent ? "sidechain" : "primary",
            execution_environment: execution.execution_environment,
            control_surface: execution.control_surface,
            bridge_session_id: execution.bridge_session_id || void 0,
            ai_tool_version: 1
            // Intentionally no run_id (would split the daemon's stream), and no
            // runtime_ms / token_usage / measurement fields — the daemon owns
            // precise runtime + token measurement from transcripts; shipping
            // estimates here would double-count when both trackers run.
          }
        }
      };
      const request = { ticks: [tick] };
      if (gitContext.workspaceFingerprint) {
        request.workspace_fingerprint = gitContext.workspaceFingerprint;
      }
      if (gitContext.workspacePath) {
        request.workspace_path = gitContext.workspacePath;
      }
      return request;
    }
    function setShellTitleResolver(resolver) {
      shellTitleResolver = resolver;
    }
    module2.exports = {
      ...runtime2,
      buildTrackTickRequest,
      setShellTitleResolver,
      shellTitleFor,
      classifyActivity,
      inferModelProvider,
      normalizedToolName,
      rememberSessionModel,
      tickInstant,
      resolveExecutionContext,
      resolveModel,
      resolveRepo,
      resolveStream,
      sessionModel
    };
  }
});

// packages/plugin-runtime/status.js
var require_status = __commonJS({
  "packages/plugin-runtime/status.js"(exports2, module2) {
    var fs = require("fs");
    var path = require("path");
    function exists(filePath) {
      try {
        fs.accessSync(filePath);
        return true;
      } catch {
        return false;
      }
    }
    function safeList(dirPath, filter = () => true) {
      try {
        return fs.readdirSync(dirPath).filter(filter).sort();
      } catch {
        return [];
      }
    }
    function fileStats(filePath) {
      try {
        return fs.statSync(filePath);
      } catch {
        return null;
      }
    }
    function tailLines(filePath, count) {
      try {
        const content = fs.readFileSync(filePath, "utf-8").trim();
        if (!content) return [];
        return content.split("\n").slice(-count);
      } catch {
        return [];
      }
    }
    function newestFile(dirPath, names) {
      const withStats = names.map((name) => ({ name, stats: fileStats(path.join(dirPath, name)) })).filter((entry) => entry.stats);
      withStats.sort((a, b) => b.stats.mtimeMs - a.stats.mtimeMs);
      return withStats[0] || null;
    }
    function buildStatus(runtime2) {
      const queueFiles = safeList(runtime2.QUEUE_DIR, (name) => name.endsWith(".json"));
      const oldest = queueFiles[0] ? fileStats(path.join(runtime2.QUEUE_DIR, queueFiles[0])) : null;
      const deadLetterFiles = safeList(runtime2.DEAD_LETTER_DIR, (name) => name.endsWith(".json"));
      const oldestDeadLetter = deadLetterFiles[0] ? fileStats(path.join(runtime2.DEAD_LETTER_DIR, deadLetterFiles[0])) : null;
      const quarantineFiles = safeList(runtime2.QUARANTINE_DIR);
      const oldestQuarantine = quarantineFiles[0] ? fileStats(path.join(runtime2.QUARANTINE_DIR, quarantineFiles[0])) : null;
      const stateFiles = safeList(runtime2.STATE_DIR, (name) => name.endsWith(".json"));
      const cacheFiles = safeList(runtime2.GIT_CACHE_DIR, (name) => name.endsWith(".json"));
      const newestCache = newestFile(runtime2.GIT_CACHE_DIR, cacheFiles);
      const logFiles = safeList(runtime2.LOG_DIR, (name) => name.endsWith(".log"));
      const logs = {};
      for (const file of logFiles) {
        logs[file] = tailLines(path.join(runtime2.LOG_DIR, file), 5);
      }
      let shipperLock = {
        path: runtime2.SHIPPER_LOCK_PATH,
        present: false,
        holder: null
      };
      if (exists(runtime2.SHIPPER_LOCK_PATH)) {
        try {
          shipperLock = {
            path: runtime2.SHIPPER_LOCK_PATH,
            present: true,
            holder: runtime2.readJsonFile(runtime2.SHIPPER_LOCK_PATH)
          };
        } catch {
          shipperLock = {
            path: runtime2.SHIPPER_LOCK_PATH,
            present: true,
            holder: "unreadable"
          };
        }
      }
      return {
        auth: {
          configPath: runtime2.CLI_CONFIG_PATH,
          configPresent: exists(runtime2.CLI_CONFIG_PATH),
          apiKeyPresent: Boolean(runtime2.loadAuth())
        },
        queue: {
          dir: runtime2.QUEUE_DIR,
          pending: queueFiles.length,
          oldestQueuedAt: oldest ? oldest.mtime.toISOString() : null
        },
        deadLetter: {
          dir: runtime2.DEAD_LETTER_DIR || null,
          unsent: deadLetterFiles.length,
          oldestDeadLetteredAt: oldestDeadLetter ? oldestDeadLetter.mtime.toISOString() : null
        },
        quarantine: {
          dir: runtime2.QUARANTINE_DIR || null,
          count: quarantineFiles.length,
          oldestAt: oldestQuarantine ? oldestQuarantine.mtime.toISOString() : null
        },
        // Everything captured that the backend has not accepted yet. Quarantined
        // files are excluded: they are unreadable, not unsent.
        unsent: queueFiles.length + deadLetterFiles.length,
        state: {
          dir: runtime2.STATE_DIR,
          activeStreams: stateFiles.length
        },
        cache: {
          dir: runtime2.GIT_CACHE_DIR,
          entries: cacheFiles.length,
          newestUpdatedAt: newestCache ? newestCache.stats.mtime.toISOString() : null
        },
        shipperLock,
        logs: {
          dir: runtime2.LOG_DIR,
          files: logFiles,
          recent: logs
        }
      };
    }
    function printStatus2(runtime2, label) {
      const status = buildStatus(runtime2);
      if (process.argv.includes("--json")) {
        process.stdout.write(`${JSON.stringify(status, null, 2)}
`);
        process.exit(0);
      }
      const lines = [
        `DevClocked ${label} Hook Status`,
        `auth: ${status.auth.apiKeyPresent ? "configured" : "missing"} (${status.auth.configPath})`,
        `queue: ${status.queue.pending} pending${status.queue.oldestQueuedAt ? `, oldest ${status.queue.oldestQueuedAt}` : ""}`,
        // Dead-lettered envelopes are captured activity the backend has not taken
        // yet — worth calling out, because a number that never falls means the
        // replay is not reaching the backend (DEV-936).
        status.deadLetter.unsent > 0 ? `dead-letter: ! ${status.deadLetter.unsent} unsent, replays on reconnect${status.deadLetter.oldestDeadLetteredAt ? `, oldest ${status.deadLetter.oldestDeadLetteredAt}` : ""} (${status.deadLetter.dir})` : "dead-letter: 0 unsent",
        // Corrupt envelopes that will never ship. Normally zero; a non-zero count
        // that keeps climbing means something is truncating queue writes.
        status.quarantine.count > 0 ? `quarantine: ! ${status.quarantine.count} unreadable file(s)${status.quarantine.oldestAt ? `, oldest ${status.quarantine.oldestAt}` : ""} (${status.quarantine.dir})` : "quarantine: 0 files",
        `streams: ${status.state.activeStreams} active state file(s)`,
        `git cache: ${status.cache.entries} entry(s)${status.cache.newestUpdatedAt ? `, newest ${status.cache.newestUpdatedAt}` : ""}`,
        `shipper lock: ${status.shipperLock.present ? "present" : "not present"}`
      ];
      if (status.logs.files.length) {
        lines.push(`logs: ${status.logs.files.join(", ")}`);
        for (const file of status.logs.files) {
          lines.push(`recent ${file}:`);
          for (const line of status.logs.recent[file]) {
            lines.push(line);
          }
        }
      } else {
        lines.push(`logs: none (${runtime2.LOG_DIR})`);
      }
      process.stdout.write(`${lines.join("\n")}
`);
    }
    module2.exports = {
      buildStatus,
      printStatus: printStatus2
    };
  }
});

// packages/claude-plugin/hooks/status.js
var runtime = require_runtime();
var { printStatus } = require_status();
printStatus(runtime, "Claude Code");
