const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// The installer writes to $HOME/.claude/settings.json. Point HOME at a temp
// dir before requiring install.js so tests never touch the real settings.
process.env.HOME = fs.mkdtempSync(path.join(os.tmpdir(), 'devclocked-claude-install-test-'));

const {
  HOOK_EVENTS,
  MANAGED_PREFIX,
  countManagedHooks,
  install,
  settingsPath,
  stripManagedHooks,
  uninstall,
} = require('./install');

const PLUGIN_ROOT = __dirname;

function freshHome() {
  process.env.HOME = fs.mkdtempSync(path.join(os.tmpdir(), 'devclocked-claude-install-test-'));
  return process.env.HOME;
}

function readSettings() {
  return JSON.parse(fs.readFileSync(settingsPath(), 'utf-8'));
}

test('install creates settings.json with one managed hook per event', () => {
  freshHome();
  const result = install(PLUGIN_ROOT);
  assert.equal(result.installedHooks, HOOK_EVENTS.length);

  const settings = readSettings();
  for (const eventName of HOOK_EVENTS) {
    const entries = settings.hooks[eventName];
    assert.ok(Array.isArray(entries), `${eventName} entry missing`);
    assert.equal(entries.length, 1);
    assert.equal(entries[0].hooks[0].type, 'command');
    assert.ok(entries[0].hooks[0].command.includes(MANAGED_PREFIX));
    assert.ok(entries[0].hooks[0].command.includes('track.js'));
  }
  assert.equal(settings.hooks.PostToolUse[0].matcher, undefined, 'PostToolUse must match all tools');
});

test('install preserves existing settings keys and foreign hooks', () => {
  const home = freshHome();
  fs.mkdirSync(path.join(home, '.claude'), { recursive: true });
  fs.writeFileSync(
    settingsPath(),
    JSON.stringify({
      model: 'opusplan',
      permissions: { allow: ['Bash(npm run test:*)'] },
      hooks: {
        PostToolUse: [
          { matcher: 'Write', hooks: [{ type: 'command', command: 'my-other-hook.sh' }] },
        ],
        Notification: [{ hooks: [{ type: 'command', command: 'notify.sh' }] }],
      },
    })
  );

  install(PLUGIN_ROOT);
  const settings = readSettings();

  assert.equal(settings.model, 'opusplan');
  assert.deepEqual(settings.permissions, { allow: ['Bash(npm run test:*)'] });
  assert.equal(settings.hooks.Notification[0].hooks[0].command, 'notify.sh');

  const postToolUse = settings.hooks.PostToolUse;
  assert.equal(postToolUse.length, 2);
  assert.equal(postToolUse[0].hooks[0].command, 'my-other-hook.sh');
  assert.ok(postToolUse[1].hooks[0].command.includes(MANAGED_PREFIX));
});

test('install is idempotent — re-running does not duplicate entries', () => {
  freshHome();
  install(PLUGIN_ROOT);
  install(PLUGIN_ROOT);
  const third = install(PLUGIN_ROOT);

  assert.equal(third.installedHooks, HOOK_EVENTS.length);
  const settings = readSettings();
  assert.equal(countManagedHooks(settings.hooks), HOOK_EVENTS.length);
  for (const eventName of HOOK_EVENTS) {
    assert.equal(settings.hooks[eventName].length, 1, `${eventName} duplicated`);
  }
});

test('uninstall removes exactly the managed hooks', () => {
  const home = freshHome();
  fs.mkdirSync(path.join(home, '.claude'), { recursive: true });
  fs.writeFileSync(
    settingsPath(),
    JSON.stringify({
      model: 'opusplan',
      hooks: {
        Stop: [{ hooks: [{ type: 'command', command: 'keep-me.sh' }] }],
      },
    })
  );

  install(PLUGIN_ROOT);
  const result = uninstall();
  assert.equal(result.removedHooks, HOOK_EVENTS.length);

  const settings = readSettings();
  assert.equal(settings.model, 'opusplan');
  assert.deepEqual(settings.hooks, {
    Stop: [{ hooks: [{ type: 'command', command: 'keep-me.sh' }] }],
  });
});

test('uninstall drops the hooks key entirely when nothing else remains', () => {
  freshHome();
  install(PLUGIN_ROOT);
  uninstall();

  const settings = readSettings();
  assert.ok(!('hooks' in settings));
});

test('uninstall is a no-op when settings.json is missing', () => {
  freshHome();
  const result = uninstall();
  assert.equal(result.removedHooks, 0);
  assert.ok(!fs.existsSync(settingsPath()));
});

test('stripManagedHooks ignores non-managed commands mentioning devclocked', () => {
  const stripped = stripManagedHooks({
    Stop: [{ hooks: [{ type: 'command', command: 'echo devclocked' }] }],
  });
  assert.equal(stripped.Stop[0].hooks[0].command, 'echo devclocked');
});
