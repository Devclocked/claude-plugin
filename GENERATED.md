# Generated — do not hand-edit

This repository is a **published mirror** of the DevClocked claude plugin.
It is generated from the monorepo by:

    node packages/plugin-build/build.mjs --surface claude --out <dir>

which bundles `packages/claude-plugin/hooks` with the shared
`packages/plugin-runtime` inlined. Edit the source in the monorepo and
re-run the build — any changes made directly here will be overwritten.

This tree's `package.json` scripts are rewritten by the build. Anything
requiring `hooks/runtime.js` cannot run here — it exists only as inlined
bytes — so only the tests that ship are wired up:

    test: node --test hooks/track.test.js install.test.js
    build: removed

Built from plugin v1.1.7, plugin-runtime 21964d787253.
