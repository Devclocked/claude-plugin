# DevClocked for Claude Code

Claude Code-native DevClocked tracking plugin.

This plugin makes Claude Code itself trigger DevClocked tracking via its
native hooks system, so Claude Code work keeps tracking even when the
DevClocked Mac app is not open. It also detects Remote Control sessions
(a phone or browser driving a local Claude Code session) and labels the
activity accordingly.

## What it includes

- Claude Code hooks for session lifecycle, prompts, tool activity, and
  subagent streams (`SessionStart`, `UserPromptSubmit`, `PostToolUse`,
  `Stop`, `SessionEnd`, `SubagentStart`, `SubagentStop`)
- Remote Control detection via `CLAUDE_CODE_REMOTE` /
  `CLAUDE_CODE_BRIDGE_SESSION_ID`
- Git context enrichment (branch, repo, workspace fingerprint) from the
  session's working directory
- Local queue + detached background shipper for reliable, non-blocking
  event delivery

## Setup

1. Install DevClocked auth with `npx devclocked setup` or `devclocked login`
   (writes `~/.config/devclocked/cli.json`)
2. Install the managed hooks into `~/.claude/settings.json`:

```bash
npm run install-hooks
```

3. Verify setup:

```bash
npm run doctor
node ./hooks/status.js
```

4. Restart Claude Code and start coding.

The installer merges non-destructively into `~/.claude/settings.json`: it
creates the file if missing, preserves every existing key and hook entry,
and is idempotent — re-running never duplicates entries. Managed entries
are tagged with `DEVCLOCKED_CLAUDE_PLUGIN=1` so uninstall removes exactly
ours:

```bash
npm run uninstall-hooks
```

## What's tracked

Per hook event, one tick is shipped to DevClocked with:

- session/tool/file entity (file *paths* only for Edit/Write events —
  never file contents)
- activity classification (coding / reading / planning) + work signature
- git branch, repo URL, repository name, and a hashed workspace
  fingerprint
- the session model (captured at `SessionStart`)
- execution environment: `local` normally, `local_remote_control` when
  the session is driven via Remote Control (control surface is reported
  as `unknown` in that case — phone vs browser cannot be distinguished)

Runtime and token measurement is intentionally NOT shipped by this
plugin: the DevClocked desktop daemon owns precise token/runtime
measurement from Claude Code transcripts. The plugin also never sets a
`run_id`, so its stream merges with the daemon's
`claude-code:<sessionId>[:agentId]` stream instead of double-counting.

## Coverage model

- Claude Code hooks are the live keep-alive source for Claude Code
  activity
- The desktop daemon remains the precise source by watching
  `~/.claude/projects/**/*.jsonl` transcripts
- When both are present, they merge into the same stream identity — no
  double-counting

## Privacy

- Your code never leaves your machine — only timestamps and metadata are
  synced
- No prompts, AI responses, or file contents are ever collected or
  shipped
- Workspace identity is a SHA-256 fingerprint of the git root path
- All data encrypted in transit (TLS 1.3)
- [Read more](https://devclocked.com/privacy)

## Debugging

```bash
node ./hooks/status.js
node ./hooks/status.js --json
npm run doctor
npm run doctor -- --json
```

Queue: `~/.config/devclocked/claude-hook-queue/`
Dead letter: `~/.config/devclocked/claude-hook-dead-letter/` (ticks that ran out
of retries; replayed automatically once the backend is reachable again)
Logs: `~/.config/devclocked/claude-hook-logs/`

## Tests

```bash
npm test
```

## Support

- **Documentation**: [devclocked.com/docs](https://devclocked.com/docs)
- **Issues**: [GitHub Issues](https://github.com/devclocked/trackers/issues)
- **Email**: support@devclocked.com
