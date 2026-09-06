#!/usr/bin/env node
var __getOwnPropNames = Object.getOwnPropertyNames;
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};

// packages/plugin-runtime/ship.js
var require_ship = __commonJS({
  "packages/plugin-runtime/ship.js"(exports2, module2) {
    function defaultSleep(ms) {
      return new Promise((resolve) => setTimeout(resolve, ms));
    }
    async function acquireShipperLockWithWait(runtime2, { timeoutMs = 5e3, intervalMs = 100, sleep = defaultSleep } = {}) {
      const deadline = Date.now() + timeoutMs;
      let waitedMs = 0;
      for (; ; ) {
        const fd = runtime2.acquireShipperLock();
        if (fd) return fd;
        if (waitedMs >= timeoutMs || Date.now() >= deadline) return null;
        await sleep(intervalMs);
        waitedMs += intervalMs;
      }
    }
    function normalizeResult(value) {
      if (value && typeof value === "object") {
        const shipped2 = Boolean(value.shipped);
        return {
          shipped: shipped2,
          reachable: value.reachable === void 0 ? shipped2 : Boolean(value.reachable),
          failed: Boolean(value.failed)
        };
      }
      const shipped = Boolean(value);
      return { shipped, reachable: shipped, failed: false };
    }
    async function drainQueue(runtime2, processEnvelope2, {
      maxPasses = 25,
      apiKey = runtime2.loadAuth(),
      replay = true,
      replayLimit = 25
    } = {}) {
      const attempted = /* @__PURE__ */ new Set();
      const state = { passes: 0, shipped: 0, replayed: 0, quarantined: 0, reachable: false };
      async function attempt(filePath) {
        attempted.add(filePath);
        let result;
        try {
          result = normalizeResult(await processEnvelope2(filePath, apiKey));
        } catch (error) {
          if (typeof runtime2.quarantineEnvelope === "function" && runtime2.quarantineEnvelope(filePath, error)) {
            state.quarantined += 1;
          }
          runtime2.appendLog("shipper", "Queued hook event could not be processed", {
            error: error instanceof Error ? error.message : "unknown_error"
          });
          return null;
        }
        if (result.shipped) state.shipped += 1;
        if (result.reachable) state.reachable = true;
        return result;
      }
      let files = runtime2.listQueueFiles();
      while (files.length > 0 && state.passes < maxPasses) {
        state.passes += 1;
        for (const filePath of files) {
          await attempt(filePath);
        }
        files = runtime2.listQueueFiles().filter((filePath) => !attempted.has(filePath));
      }
      if (replay) {
        if (typeof runtime2.pruneDeadLetter === "function") runtime2.pruneDeadLetter();
        const canReplay = typeof runtime2.replayDeadLetterFile === "function" && typeof runtime2.listDeadLetterFiles === "function" && // Either the backend answered this run, or there was nothing live to ask
        // with and the first replayed envelope becomes the probe.
        (state.reachable || attempted.size === 0);
        if (canReplay) {
          for (const parkedPath of runtime2.listDeadLetterFiles().slice(0, replayLimit)) {
            const { queuedPath, quarantined } = runtime2.replayDeadLetterFile(parkedPath);
            if (quarantined) state.quarantined += 1;
            if (!queuedPath) continue;
            state.replayed += 1;
            const result = await attempt(queuedPath);
            if (result && result.failed) break;
          }
        }
      }
      return {
        passes: state.passes,
        attempted: attempted.size,
        shipped: state.shipped,
        replayed: state.replayed,
        quarantined: state.quarantined,
        reachable: state.reachable
      };
    }
    async function runShipper2(runtime2, processEnvelope2) {
      const lockFd = await acquireShipperLockWithWait(runtime2);
      if (!lockFd) {
        runtime2.appendLog("shipper", "Shipper lock busy, deferring to holder");
        process.exit(0);
      }
      try {
        const apiKey = runtime2.loadAuth();
        if (!apiKey) {
          runtime2.appendLog("shipper", "Skipping ship pass because auth is missing");
          process.exit(0);
        }
        const result = await drainQueue(runtime2, processEnvelope2, { apiKey });
        if (result.replayed > 0 || result.quarantined > 0) {
          runtime2.appendLog("shipper", "Shipper run finished", {
            replayed: result.replayed,
            quarantined: result.quarantined,
            shipped: result.shipped,
            reachable: result.reachable
          });
        }
        if (typeof runtime2.pruneStaleStreamState === "function") {
          const pruned = runtime2.pruneStaleStreamState();
          if (pruned > 0) {
            runtime2.appendLog("shipper", "Pruned stale stream state", { count: pruned });
          }
        }
      } catch (error) {
        runtime2.appendLog("shipper", "Shipper crashed", {
          error: error instanceof Error ? error.message : "unknown_error"
        });
      } finally {
        runtime2.releaseShipperLock(lockFd);
      }
      process.exit(0);
    }
    module2.exports = {
      acquireShipperLockWithWait,
      drainQueue,
      normalizeResult,
      runShipper: runShipper2
    };
  }
});

// packages/plugin-runtime/core.js
var require_core = __commonJS({
  "packages/plugin-runtime/core.js"(exports2, module2) {
    var fs2 = require("fs");
    var path2 = require("path");
    var os = require("os");
    var https = require("https");
    var { execSync } = require("child_process");
    var { createHash, randomUUID } = require("crypto");
    var SUPABASE_URL = "https://api.devclocked.com";
    var SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhhcWZna2ttZWdseXJ1bG1waXN0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTIwMDYyODcsImV4cCI6MjA2NzU4MjI4N30.fTonLdDRqqtV44tBcl0Z7ryvaSD5Gczy-OTkzHUw0o4";
    var TICK_INTERVAL_MS = 3e4;
    var LOCK_STALE_MS = 6e4;
    var GIT_CACHE_TTL_MS = 6e4;
    var MAX_SHIP_ATTEMPTS2 = 5;
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
      return path2.join(resolveHomeDir() || "~", ".config", "devclocked");
    }
    function readPluginVersion(shipperPath, manifestPath) {
      try {
        const root = path2.dirname(path2.dirname(shipperPath));
        const resolved = manifestPath || path2.join(root, "package.json");
        const manifest = JSON.parse(fs2.readFileSync(resolved, "utf-8"));
        return typeof manifest.version === "string" && manifest.version ? manifest.version : "unknown";
      } catch {
        return "unknown";
      }
    }
    function ensureDir(dirPath) {
      fs2.mkdirSync(dirPath, { recursive: true, mode: 448 });
      try {
        fs2.chmodSync(dirPath, 448);
      } catch {
      }
    }
    function safeId(value) {
      return String(value).replace(/[^a-zA-Z0-9_-]/g, "_");
    }
    function writeJsonFile(filePath, value) {
      ensureDir(path2.dirname(filePath));
      const tmpPath = `${filePath}.${process.pid}.tmp`;
      try {
        fs2.writeFileSync(tmpPath, JSON.stringify(value, null, 2), { mode: 384 });
        try {
          fs2.chmodSync(tmpPath, 384);
        } catch {
        }
        fs2.renameSync(tmpPath, filePath);
      } catch (error) {
        try {
          fs2.unlinkSync(tmpPath);
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
    function readJsonFile2(filePath) {
      return JSON.parse(fs2.readFileSync(filePath, "utf-8"));
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
      const CLI_CONFIG_PATH = path2.join(DEVCLOCKED_HOME, "cli.json");
      const PLUGIN_ACTIVITY_DIR = path2.join(DEVCLOCKED_HOME, "plugin-activity");
      const STATE_DIR = path2.join(DEVCLOCKED_HOME, `${namespace}-state`);
      const QUEUE_DIR = path2.join(DEVCLOCKED_HOME, `${namespace}-queue`);
      const LOG_DIR = path2.join(DEVCLOCKED_HOME, `${namespace}-logs`);
      const GIT_CACHE_DIR = path2.join(DEVCLOCKED_HOME, `${namespace}-cache`);
      const DEAD_LETTER_DIR = path2.join(DEVCLOCKED_HOME, `${namespace}-dead-letter`);
      const QUARANTINE_DIR = path2.join(DEVCLOCKED_HOME, `${namespace}-corrupt`);
      const SHIPPER_LOCK_PATH = path2.join(QUEUE_DIR, "shipper.lock");
      const PLUGIN_ACTIVITY_PATH = path2.join(PLUGIN_ACTIVITY_DIR, `${source}.json`);
      function appendLog2(name, message, extra) {
        try {
          ensureDir(LOG_DIR);
          const entry = {
            timestamp: (/* @__PURE__ */ new Date()).toISOString(),
            message,
            ...extra ? { extra } : {}
          };
          fs2.appendFileSync(path2.join(LOG_DIR, `${name}.log`), `${JSON.stringify(entry)}
`);
        } catch {
        }
      }
      function loadAuth() {
        try {
          const config = readJsonFile2(CLI_CONFIG_PATH);
          return config.api_key || null;
        } catch {
          return null;
        }
      }
      function getStreamState2(streamId) {
        try {
          return readJsonFile2(path2.join(STATE_DIR, `stream_${safeId(streamId)}.json`));
        } catch {
          return null;
        }
      }
      function saveStreamState2(streamId, state) {
        writeJsonFile(path2.join(STATE_DIR, `stream_${safeId(streamId)}.json`), state);
      }
      function removeStreamState2(streamId) {
        try {
          fs2.unlinkSync(path2.join(STATE_DIR, `stream_${safeId(streamId)}.json`));
        } catch {
        }
      }
      function shouldThrottle2(streamId) {
        const state = getStreamState2(streamId);
        if (!state || !state.last_tick_at) return false;
        return Date.now() - state.last_tick_at < TICK_INTERVAL_MS;
      }
      function pruneStaleStreamState(now = Date.now(), ttlMs = STREAM_STATE_TTL_MS) {
        let removed = 0;
        let files;
        try {
          files = fs2.readdirSync(STATE_DIR);
        } catch {
          return 0;
        }
        for (const name of files) {
          if (!name.startsWith("stream_") || !name.endsWith(".json")) continue;
          const filePath = path2.join(STATE_DIR, name);
          let stale = true;
          try {
            const state = readJsonFile2(filePath);
            const lastSeen = state.last_tick_at || state.started_at;
            stale = !lastSeen || now - lastSeen > ttlMs;
          } catch {
            stale = true;
          }
          if (!stale) continue;
          try {
            fs2.unlinkSync(filePath);
            removed += 1;
          } catch {
          }
        }
        return removed;
      }
      function toAbsoluteDir(maybePath) {
        if (!maybePath || typeof maybePath !== "string") return null;
        const candidate = path2.isAbsolute(maybePath) ? maybePath : path2.join(resolveHomeDir() || "/", maybePath);
        try {
          const stat = fs2.statSync(candidate);
          if (stat.isDirectory()) return candidate;
          return path2.dirname(candidate);
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
          const resolved = fs2.realpathSync(workingDir).replace(/\/+$/, "").toLowerCase();
          const cacheKey = createHash("sha256").update(resolved).digest("hex");
          return path2.join(GIT_CACHE_DIR, `${cacheKey}.json`);
        } catch {
          return null;
        }
      }
      function loadCachedGitContext(workingDir) {
        const cachePath = getGitCachePath(workingDir);
        if (!cachePath) return null;
        try {
          const cached = readJsonFile2(cachePath);
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
          const resolved = fs2.realpathSync(dirPath).replace(/\/+$/, "").toLowerCase();
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
        const repoName = repoFullName ? repoFullName.split("/").pop() : path2.basename(gitRoot);
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
          resolved = fs2.realpathSync(dirPath);
        } catch {
        }
        const normalized = resolved.replace(/\/+$/, "") || "/";
        let home = resolveHomeDir();
        try {
          if (home) home = fs2.realpathSync(home);
        } catch {
        }
        home = home.replace(/\/+$/, "");
        return normalized === "/" || Boolean(home) && normalized === home;
      }
      function resolveGitContext2(input) {
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
          appendLog2("shipper", "Deferring project identity because git could not run", {
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
          repoName: path2.basename(sessionDir) || null,
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
      function callEdgeFunction2(apiKey, fnName, body) {
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
        return path2.join(QUEUE_DIR, `${Date.now()}-${process.pid}-${randomUUID()}.json`);
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
          return fs2.readdirSync(QUEUE_DIR).filter((name) => name.endsWith(".json")).sort().map((name) => path2.join(QUEUE_DIR, name));
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
          const fd = fs2.openSync(SHIPPER_LOCK_PATH, "wx");
          fs2.writeFileSync(fd, JSON.stringify({ pid: process.pid, started_at: Date.now() }));
          return fd;
        } catch (error) {
          if (error.code !== "EEXIST") return null;
          let reclaimable = false;
          try {
            const existing = readJsonFile2(SHIPPER_LOCK_PATH);
            const stale = !existing.started_at || Date.now() - existing.started_at > LOCK_STALE_MS;
            const dead = !existing.pid || !isProcessAlive(existing.pid);
            reclaimable = stale || dead;
          } catch {
            try {
              reclaimable = Date.now() - fs2.statSync(SHIPPER_LOCK_PATH).mtimeMs > LOCK_STALE_MS;
            } catch {
              return null;
            }
          }
          if (!reclaimable) return null;
          try {
            fs2.unlinkSync(SHIPPER_LOCK_PATH);
          } catch {
            return null;
          }
          return acquireShipperLock();
        }
      }
      function releaseShipperLock(fd) {
        try {
          fs2.closeSync(fd);
        } catch {
        }
        try {
          fs2.unlinkSync(SHIPPER_LOCK_PATH);
        } catch {
        }
      }
      function markEnvelopeRetry2(filePath, envelope, errorMessage) {
        envelope.attempts = (envelope.attempts || 0) + 1;
        envelope.last_error = errorMessage;
        envelope.last_attempt_at = (/* @__PURE__ */ new Date()).toISOString();
        envelope.retry_after = new Date(Date.now() + RETRY_BACKOFF_MS).toISOString();
        writeJsonFile(filePath, envelope);
      }
      function shouldRetryEnvelope2(envelope) {
        if (!envelope.retry_after) return true;
        return Date.now() >= new Date(envelope.retry_after).getTime();
      }
      function discardEnvelope2(filePath, envelope, reason) {
        appendLog2("shipper", "Dropping queued hook event", {
          file: path2.basename(filePath),
          reason,
          hook_event_name: envelope.input?.hook_event_name || null,
          attempts: envelope.attempts || 0
        });
        try {
          fs2.unlinkSync(filePath);
        } catch {
        }
      }
      function listDeadLetterFiles() {
        try {
          ensureDir(DEAD_LETTER_DIR);
          return fs2.readdirSync(DEAD_LETTER_DIR).filter((name) => name.endsWith(".json")).sort().map((name) => path2.join(DEAD_LETTER_DIR, name));
        } catch {
          return [];
        }
      }
      function deadLetterEnvelope2(filePath, envelope, reason) {
        const target = path2.join(DEAD_LETTER_DIR, path2.basename(filePath));
        envelope.dead_lettered_at = (/* @__PURE__ */ new Date()).toISOString();
        envelope.dead_letter_reason = reason;
        try {
          writeJsonFile(target, envelope);
          fs2.unlinkSync(filePath);
        } catch (error) {
          appendLog2("shipper", "Failed to dead-letter queued hook event", {
            file: path2.basename(filePath),
            reason,
            error: error instanceof Error ? error.message : "unknown_error"
          });
          return null;
        }
        appendLog2("shipper", "Dead-lettered queued hook event, will replay on reconnect", {
          file: path2.basename(filePath),
          reason,
          hook_event_name: envelope.input?.hook_event_name || null,
          attempts: envelope.attempts || 0
        });
        return target;
      }
      function sweepStaleFiles(dirPath, { now, maxAgeMs, filter, reason }) {
        let names;
        try {
          names = fs2.readdirSync(dirPath);
        } catch {
          return 0;
        }
        let removed = 0;
        for (const name of names) {
          if (!filter(name)) continue;
          const filePath = path2.join(dirPath, name);
          let stats;
          try {
            stats = fs2.statSync(filePath);
          } catch {
            continue;
          }
          if (now - stats.mtimeMs <= maxAgeMs) continue;
          try {
            fs2.unlinkSync(filePath);
          } catch {
            continue;
          }
          removed += 1;
          appendLog2("shipper", "Swept stale file", { file: name, dir: dirPath, reason });
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
            stats = fs2.statSync(filePath);
          } catch {
            continue;
          }
          let capturedAtMs = NaN;
          try {
            capturedAtMs = Date.parse(readJsonFile2(filePath).captured_at);
          } catch {
          }
          if (!Number.isFinite(capturedAtMs)) capturedAtMs = stats.mtimeMs;
          entries.push({ filePath, bytes: stats.size, capturedAtMs });
        }
        let evicted = 0;
        const evict = (entry, reason, detail) => {
          try {
            fs2.unlinkSync(entry.filePath);
          } catch {
            return false;
          }
          evicted += 1;
          appendLog2("shipper", "Evicted dead-lettered hook event \u2014 activity permanently lost", {
            file: path2.basename(entry.filePath),
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
          envelope = readJsonFile2(filePath);
        } catch (error) {
          return { queuedPath: null, quarantined: quarantineEnvelope(filePath, error) };
        }
        envelope.attempts = 0;
        delete envelope.retry_after;
        envelope.replayed_at = (/* @__PURE__ */ new Date()).toISOString();
        const target = path2.join(QUEUE_DIR, path2.basename(filePath));
        try {
          writeJsonFile(target, envelope);
          fs2.unlinkSync(filePath);
        } catch (error) {
          appendLog2("shipper", "Failed to replay dead-lettered hook event", {
            file: path2.basename(filePath),
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
          appendLog2("shipper", "Replayed dead-lettered hook events into the queue", { count: replayed });
        }
        return replayed;
      }
      function quarantineEnvelope(filePath, error) {
        if (!fs2.existsSync(filePath)) return false;
        try {
          readJsonFile2(filePath);
          appendLog2("shipper", "Queue file failed to process but still parses \u2014 left in place", {
            file: path2.basename(filePath),
            error: error instanceof Error ? error.message : "unknown_error"
          });
          return false;
        } catch {
        }
        const target = path2.join(QUARANTINE_DIR, path2.basename(filePath));
        let moved = false;
        try {
          ensureDir(QUARANTINE_DIR);
          fs2.renameSync(filePath, target);
          moved = true;
        } catch {
          try {
            fs2.unlinkSync(filePath);
          } catch {
          }
        }
        appendLog2("shipper", "Quarantined unreadable queue file", {
          file: path2.basename(filePath),
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
          appendLog2("hook", "Failed to wake shipper", {
            error: error instanceof Error ? error.message : "unknown_error"
          });
        }
      }
      function readPluginActivity() {
        try {
          const raw = readJsonFile2(PLUGIN_ACTIVITY_PATH);
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
        MAX_SHIP_ATTEMPTS: MAX_SHIP_ATTEMPTS2,
        DEAD_LETTER_MAX_AGE_MS,
        DEAD_LETTER_MAX_BYTES,
        DEAD_LETTER_REPLAY_LIMIT,
        appendLog: appendLog2,
        acquireShipperLock,
        callEdgeFunction: callEdgeFunction2,
        classifyGitFailure,
        deadLetterEnvelope: deadLetterEnvelope2,
        discardEnvelope: discardEnvelope2,
        enqueueHookEvent,
        ensureDir,
        firstOpaqueId,
        getStreamState: getStreamState2,
        isTrackTickProcessed,
        listDeadLetterFiles,
        listQueueFiles,
        loadAuth,
        markEnvelopeRetry: markEnvelopeRetry2,
        normalizeOpaqueId,
        pruneDeadLetter,
        quarantineEnvelope,
        readJsonFile: readJsonFile2,
        recordPluginActivity,
        releaseShipperLock,
        removeStreamState: removeStreamState2,
        replayDeadLetter,
        replayDeadLetterFile,
        resolveGitContext: resolveGitContext2,
        saveStreamState: saveStreamState2,
        shouldRetryEnvelope: shouldRetryEnvelope2,
        shouldThrottle: shouldThrottle2,
        pruneStaleStreamState,
        wakeShipper,
        writeJsonFile,
        pluginVersion
      };
    }
    module2.exports = {
      MAX_SHIP_ATTEMPTS: MAX_SHIP_ATTEMPTS2,
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
    var fs2 = require("fs");
    var os = require("os");
    var path2 = require("path");
    var MAX_LABEL_LEN = 72;
    var MAX_SOURCE_LEN = 32;
    function shellStoreCandidates(env = process.env, home = safeHomedir()) {
      const bases = [];
      const configured = typeof env.T3CODE_HOME === "string" ? env.T3CODE_HOME.trim() : "";
      if (configured) bases.push(configured);
      if (home) bases.push(path2.join(home, ".demuxx"), path2.join(home, ".t3"));
      const seen = /* @__PURE__ */ new Set();
      const stores = [];
      for (const base of bases) {
        const dbPath = path2.join(base, "userdata", "state.sqlite");
        if (seen.has(dbPath)) continue;
        seen.add(dbPath);
        stores.push({ path: dbPath, source: path2.basename(base) === ".demuxx" ? "demuxx" : "t3code" });
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
        exists = fs2.statSync(store.path).isFile();
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
        const config = JSON.parse(fs2.readFileSync(configPath, "utf8"));
        if (config && typeof config === "object") {
          if (config.track_session_titles === false || config.trackSessionTitles === false) return false;
        }
      } catch {
      }
      return true;
    }
    function defaultDaemonConfigPath() {
      const home = safeHomedir();
      return home ? path2.join(home, ".config", "devclocked", "daemon-config.json") : "";
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
    var path2 = require("path");
    var { createPluginRuntime } = require_core();
    var shellThreadNames = require_shellThreadNames();
    var runtime2 = createPluginRuntime({
      namespace: "claude-hook",
      source: "claude-plugin",
      shipperPath: path2.join(__dirname, "ship.js")
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
    function resolveStream2(hookEvent, input) {
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
    function resolveRepo2(input, stream, gitContext) {
      if (gitContext.resolution === "deferred") return { branch: null, repo_name: null };
      return {
        branch: gitContext.branch || null,
        repo_name: gitContext.repoName || null
      };
    }
    function rememberSessionModel2(stream, input) {
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
    function classifyActivity2(hookEvent, input, stream) {
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
    function buildTrackTickRequest2(hookEvent, input, stream, repo, gitContext, envelope) {
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
      const activity = classifyActivity2(hookEvent, input, stream);
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
          // A submitted prompt is the one hook that proves a human is at the keyboard;
          // ingest counts it as human time only when this flag is set (DEV-1258).
          // Boolean only: prompt text never leaves the machine.
          ...hookEvent === "UserPromptSubmit" ? { human_presence: true } : {},
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
      buildTrackTickRequest: buildTrackTickRequest2,
      setShellTitleResolver,
      shellTitleFor,
      classifyActivity: classifyActivity2,
      inferModelProvider,
      normalizedToolName,
      rememberSessionModel: rememberSessionModel2,
      tickInstant,
      resolveExecutionContext,
      resolveModel,
      resolveRepo: resolveRepo2,
      resolveStream: resolveStream2,
      sessionModel
    };
  }
});

// packages/claude-plugin/hooks/ship.js
var fs = require("fs");
var path = require("path");
var { runShipper } = require_ship();
var runtime = require_runtime();
var {
  MAX_SHIP_ATTEMPTS,
  appendLog,
  buildTrackTickRequest,
  callEdgeFunction,
  classifyActivity,
  deadLetterEnvelope,
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
  shouldThrottle
} = runtime;
function isLifecycleEvent(hookEvent) {
  return ["SessionStart", "SessionEnd"].includes(hookEvent);
}
function bypassesThrottle(hookEvent) {
  return isLifecycleEvent(hookEvent) || hookEvent === "UserPromptSubmit";
}
function isActivityTypeTransition(priorState, newActivityType) {
  return Boolean(priorState?.last_activity_type) && priorState.last_activity_type !== newActivityType;
}
var STALE_SESSION_END_MS = 20 * 6e4;
var DELAYED_ENVELOPE_MS = 2 * 6e4;
function envelopeAgeMs(envelope, nowMs = Date.now()) {
  const capturedAt = Date.parse(envelope?.captured_at);
  if (Number.isNaN(capturedAt)) return 0;
  return Math.max(0, nowMs - capturedAt);
}
var STALE_LIFECYCLE_REASONS = {
  SessionEnd: "stale_session_end",
  SessionStart: "stale_session_start"
};
function staleLifecycleReason(hookEvent, ageMs) {
  if (ageMs <= STALE_SESSION_END_MS) return null;
  return STALE_LIFECYCLE_REASONS[hookEvent] || null;
}
function isStaleSessionEnd(hookEvent, ageMs) {
  return staleLifecycleReason(hookEvent, ageMs) === "stale_session_end";
}
function initializeLifecycleState(hookEvent, stream, input) {
  if (hookEvent === "SessionStart") {
    const prior = getStreamState(stream.sessionId) || {};
    saveStreamState(stream.sessionId, {
      ...prior,
      started_at: prior.started_at || Date.now(),
      last_tick_at: prior.last_tick_at || null,
      root_stream_id: stream.rootStreamId,
      source: input.source || null
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
    discardEnvelope(filePath, envelope, "missing_hook_event_name");
    return;
  }
  const stream = resolveStream(hookEvent, input);
  const ageMs = envelopeAgeMs(envelope);
  const staleReason = staleLifecycleReason(hookEvent, ageMs);
  if (staleReason === "stale_session_end") {
    removeStreamState(stream.sessionId);
    discardEnvelope(filePath, envelope, staleReason);
    return;
  }
  if (staleReason) {
    discardEnvelope(filePath, envelope, staleReason);
    return;
  }
  initializeLifecycleState(hookEvent, stream, input);
  if (ageMs > DELAYED_ENVELOPE_MS) {
    appendLog("shipper", "Shipping delayed hook event", {
      file: path.basename(filePath),
      hook_event_name: hookEvent,
      age_ms: ageMs
    });
  }
  const throttleStateId = stream.throttleId;
  if (!bypassesThrottle(hookEvent) && shouldThrottle(throttleStateId)) {
    const priorState = getStreamState(throttleStateId);
    const newActivity = classifyActivity(hookEvent, input, stream);
    if (!isActivityTypeTransition(priorState, newActivity.activity_type)) {
      discardEnvelope(filePath, envelope, "throttled");
      return;
    }
  }
  const gitContext = resolveGitContext(input);
  const repo = resolveRepo(input, stream, gitContext);
  const payload = buildTrackTickRequest(hookEvent, input, stream, repo, gitContext, envelope);
  try {
    const response = await callEdgeFunction(apiKey, "track-tick", payload);
    if (!runtime.isTrackTickProcessed(response)) {
      discardEnvelope(filePath, envelope, "track_tick_unprocessed");
      appendLog("shipper", "Dropping hook event because track-tick processed no activity", {
        file: path.basename(filePath),
        hook_event_name: hookEvent
      });
      return { shipped: false, reachable: true };
    }
    if (!isLifecycleEvent(hookEvent)) {
      const state = getStreamState(throttleStateId) || {};
      state.last_tick_at = Date.now();
      state.last_activity_type = payload.ticks[0]?.activity_context?.ai_tool?.activity_type || state.last_activity_type;
      saveStreamState(throttleStateId, state);
    }
    runtime.recordPluginActivity({
      workspaceFingerprint: gitContext.workspaceFingerprint,
      rootStreamId: stream.rootStreamId,
      streamId: stream.streamId,
      sessionFileId: payload.ticks[0]?.activity_context?.ai_tool?.session_file_id || null,
      observedAt: payload.ticks[0]?.timestamp
    });
    if (hookEvent === "SessionEnd") {
      removeStreamState(stream.sessionId);
    }
    if (hookEvent === "SubagentStop") {
      removeStreamState(stream.throttleId);
    }
    fs.unlinkSync(filePath);
    return { shipped: true, reachable: true };
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown_error";
    if ((envelope.attempts || 0) + 1 >= MAX_SHIP_ATTEMPTS) {
      envelope.attempts = (envelope.attempts || 0) + 1;
      envelope.last_error = message;
      envelope.last_attempt_at = (/* @__PURE__ */ new Date()).toISOString();
      deadLetterEnvelope(filePath, envelope, `max_attempts:${message}`);
      return { shipped: false, reachable: false, failed: true };
    }
    markEnvelopeRetry(filePath, envelope, message);
    appendLog("shipper", "Queued hook event failed to send", {
      file: path.basename(filePath),
      hook_event_name: hookEvent,
      attempts: (envelope.attempts || 0) + 1,
      error: message
    });
    return { shipped: false, reachable: false, failed: true };
  }
}
if (require.main === module) {
  runShipper(runtime, processEnvelope);
}
module.exports = {
  DELAYED_ENVELOPE_MS,
  STALE_SESSION_END_MS,
  envelopeAgeMs,
  bypassesThrottle,
  isActivityTypeTransition,
  isLifecycleEvent,
  isStaleSessionEnd,
  processEnvelope,
  staleLifecycleReason
};
