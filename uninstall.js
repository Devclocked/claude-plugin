#!/usr/bin/env node

const { uninstall } = require('./install');

if (require.main === module) {
  const result = uninstall();
  process.stdout.write(`Removed ${result.removedHooks} DevClocked Claude Code hooks from ${result.settingsPath}.\n`);
}

module.exports = { uninstall };
