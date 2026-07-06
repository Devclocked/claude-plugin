#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const MANAGED_PREFIX = 'DEVCLOCKED_CLAUDE_PLUGIN=1';
const HOOK_EVENTS = [
  'SessionStart',
  'UserPromptSubmit',
  'PostToolUse',
  'Stop',
  'SessionEnd',
  'SubagentStart',
  'SubagentStop',
];

function settingsPath() {
  return path.join(process.env.HOME || '~', '.claude', 'settings.json');
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function readJson(filePath, fallback) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf-8'));
  } catch {
    return fallback;
  }
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function buildManagedHooks(pluginRoot) {
  const trackPath = path.join(pluginRoot, 'hooks', 'track.js');
  const baseCommand = `/usr/bin/env ${MANAGED_PREFIX} node ${shellQuote(trackPath)}`;

  const hooks = {};
  for (const eventName of HOOK_EVENTS) {
    hooks[eventName] = [
      {
        hooks: [
          {
            type: 'command',
            command: `${baseCommand} ${eventName}`,
          },
        ],
      },
    ];
  }
  return { hooks };
}

function isManagedHook(hook) {
  if (!hook || typeof hook !== 'object') return false;
  const command = typeof hook.command === 'string' ? hook.command : '';
  return command.includes(MANAGED_PREFIX);
}

function stripManagedHooks(hooks) {
  const source = hooks && typeof hooks === 'object' ? hooks : {};
  const nextHooks = {};

  for (const [eventName, entries] of Object.entries(source)) {
    const cleanedEntries = Array.isArray(entries)
      ? entries
          .map((entry) => {
            if (!entry || typeof entry !== 'object') return null;
            const entryHooks = Array.isArray(entry.hooks) ? entry.hooks.filter((hook) => !isManagedHook(hook)) : [];
            if (entryHooks.length === 0) return null;
            return { ...entry, hooks: entryHooks };
          })
          .filter(Boolean)
      : entries;

    if (Array.isArray(cleanedEntries) ? cleanedEntries.length > 0 : cleanedEntries) {
      nextHooks[eventName] = cleanedEntries;
    }
  }

  return nextHooks;
}

function mergeManagedHooks(existingHooks, managed) {
  const cleaned = stripManagedHooks(existingHooks);
  const merged = { ...cleaned };

  for (const [eventName, entries] of Object.entries(managed.hooks || {})) {
    const preserved = Array.isArray(merged[eventName]) ? merged[eventName] : [];
    merged[eventName] = [...preserved, ...entries];
  }

  return merged;
}

function countManagedHooks(hooks) {
  if (!hooks || typeof hooks !== 'object') return 0;

  let count = 0;
  for (const entries of Object.values(hooks)) {
    if (!Array.isArray(entries)) continue;
    for (const entry of entries) {
      if (!entry || typeof entry !== 'object' || !Array.isArray(entry.hooks)) continue;
      for (const hook of entry.hooks) {
        if (isManagedHook(hook)) count += 1;
      }
    }
  }
  return count;
}

function install(pluginRoot) {
  const filePath = settingsPath();
  const settings = readJson(filePath, {});
  const managed = buildManagedHooks(pluginRoot);
  const nextSettings = {
    ...settings,
    hooks: mergeManagedHooks(settings.hooks, managed),
  };
  writeJson(filePath, nextSettings);

  return {
    settingsPath: filePath,
    installedHooks: countManagedHooks(nextSettings.hooks),
  };
}

function uninstall() {
  const filePath = settingsPath();
  const settings = readJson(filePath, null);
  if (!settings) {
    return { settingsPath: filePath, removedHooks: 0 };
  }

  const before = countManagedHooks(settings.hooks);
  const stripped = stripManagedHooks(settings.hooks);
  const nextSettings = { ...settings };
  if (Object.keys(stripped).length > 0) {
    nextSettings.hooks = stripped;
  } else {
    delete nextSettings.hooks;
  }
  writeJson(filePath, nextSettings);

  return {
    settingsPath: filePath,
    removedHooks: before - countManagedHooks(stripped),
  };
}

function doctor(pluginRoot) {
  const filePath = settingsPath();
  const settings = readJson(filePath, {});
  const trackPath = path.join(pluginRoot, 'hooks', 'track.js');

  return {
    pluginRoot,
    settingsPath: filePath,
    settingsFilePresent: fs.existsSync(filePath),
    managedHooksInstalled: countManagedHooks(settings.hooks),
    expectedHooks: HOOK_EVENTS.length,
    trackScriptPresent: fs.existsSync(trackPath),
  };
}

function main() {
  const pluginRoot = __dirname;
  const command = process.argv[2] || 'install';

  if (command === 'install') {
    const result = install(pluginRoot);
    process.stdout.write(
      `Installed ${result.installedHooks} DevClocked Claude Code hooks into ${result.settingsPath}.\n`
    );
    return;
  }

  if (command === 'uninstall') {
    const result = uninstall();
    process.stdout.write(`Removed ${result.removedHooks} DevClocked Claude Code hooks.\n`);
    return;
  }

  if (command === 'doctor') {
    const status = doctor(pluginRoot);
    if (process.argv.includes('--json')) {
      process.stdout.write(`${JSON.stringify(status, null, 2)}\n`);
      return;
    }
    process.stdout.write(
      [
        'DevClocked Claude Code Hook Install Status',
        `plugin root: ${status.pluginRoot}`,
        `settings file: ${status.settingsFilePresent ? 'present' : 'missing'} (${status.settingsPath})`,
        `managed hooks: ${status.managedHooksInstalled}/${status.expectedHooks}`,
        `track script: ${status.trackScriptPresent ? 'present' : 'missing'}`,
      ].join('\n') + '\n'
    );
    return;
  }

  process.stderr.write(`Unknown command: ${command}\n`);
  process.exit(1);
}

if (require.main === module) {
  main();
}

module.exports = {
  HOOK_EVENTS,
  MANAGED_PREFIX,
  buildManagedHooks,
  countManagedHooks,
  doctor,
  install,
  isManagedHook,
  mergeManagedHooks,
  settingsPath,
  shellQuote,
  stripManagedHooks,
  uninstall,
};
