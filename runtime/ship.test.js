const test = require('node:test');
const assert = require('node:assert/strict');

const { acquireShipperLockWithWait, drainQueue } = require('./ship');

// A fake runtime backed by an in-memory "queue dir": listQueueFiles reflects
// whatever is in the set at the moment it is called, exactly like readdirSync.
function fakeRuntime(queue) {
  return {
    loadAuth: () => 'test-api-key',
    listQueueFiles: () => [...queue].sort(),
    appendLog: () => {},
  };
}

test('drainQueue re-lists after a pass and ships an envelope enqueued mid-pass (DEV-938)', async () => {
  const queue = new Set(['/q/a.json']);
  const processed = [];
  const processEnvelope = async (filePath, apiKey) => {
    assert.equal(apiKey, 'test-api-key');
    processed.push(filePath);
    queue.delete(filePath);
    // A second hook fires while a.json is shipping. Its shipper finds the lock
    // held and defers to us, so b.json is only visible on the re-list.
    if (filePath === '/q/a.json') queue.add('/q/b.json');
  };

  const result = await drainQueue(fakeRuntime(queue), processEnvelope);

  assert.deepEqual(processed, ['/q/a.json', '/q/b.json']);
  assert.deepEqual(result, { passes: 2, attempted: 2, shipped: 0, replayed: 0 });
  assert.equal(queue.size, 0);
});

test('drainQueue does not spin on a file that refuses to be removed', async () => {
  const queue = new Set(['/q/stuck.json']);
  let calls = 0;

  const result = await drainQueue(fakeRuntime(queue), async () => {
    calls += 1;
  });

  assert.equal(calls, 1);
  assert.deepEqual(result, { passes: 1, attempted: 1, shipped: 0, replayed: 0 });
});

test('drainQueue stops at maxPasses while new files keep appearing', async () => {
  const queue = new Set(['/q/0.json']);
  let next = 1;
  const processEnvelope = async (filePath) => {
    queue.delete(filePath);
    queue.add(`/q/${next++}.json`);
  };

  const result = await drainQueue(fakeRuntime(queue), processEnvelope, { maxPasses: 3 });

  assert.deepEqual(result, { passes: 3, attempted: 3, shipped: 0, replayed: 0 });
});

test('drainQueue is a no-op on an empty queue', async () => {
  const result = await drainQueue(fakeRuntime(new Set()), async () => {
    assert.fail('processEnvelope must not be called');
  });

  assert.deepEqual(result, { passes: 0, attempted: 0, shipped: 0, replayed: 0 });
});

test('acquireShipperLockWithWait returns the fd once the lock frees up on the 3rd poll (DEV-938)', async () => {
  let polls = 0;
  let sleeps = 0;
  const runtime = { acquireShipperLock: () => (++polls >= 3 ? 42 : null) };

  const fd = await acquireShipperLockWithWait(runtime, {
    timeoutMs: 5000,
    intervalMs: 100,
    sleep: async () => {
      sleeps += 1;
    },
  });

  assert.equal(fd, 42);
  assert.equal(polls, 3);
  assert.equal(sleeps, 2);
});

test('acquireShipperLockWithWait returns null once the timeout is spent waiting', async () => {
  let polls = 0;
  const sleeps = [];
  const runtime = {
    acquireShipperLock: () => {
      polls += 1;
      return null;
    },
  };

  const fd = await acquireShipperLockWithWait(runtime, {
    timeoutMs: 500,
    intervalMs: 100,
    sleep: async (ms) => {
      sleeps.push(ms);
    },
  });

  assert.equal(fd, null);
  assert.deepEqual(sleeps, [100, 100, 100, 100, 100]);
  assert.equal(polls, 6);
});

// --- DEV-936: bounded dead-letter replay inside a shipper run ----------------

// The fake above has no dead-letter helpers, so drainQueue skips replay
// entirely. This one models the store as a second in-memory set.
function fakeRuntimeWithDeadLetter(queue, deadLetter) {
  const calls = { prune: 0, replay: 0 };
  return {
    calls,
    loadAuth: () => 'test-api-key',
    listQueueFiles: () => [...queue].sort(),
    appendLog: () => {},
    pruneDeadLetter: () => {
      calls.prune += 1;
      return 0;
    },
    replayDeadLetter: () => {
      calls.replay += 1;
      const moved = [...deadLetter].sort();
      for (const filePath of moved) {
        deadLetter.delete(filePath);
        queue.add(filePath);
      }
      return moved.length;
    },
  };
}

test('an idle run replays the dead-letter store and ships each envelope exactly once (DEV-936)', async () => {
  // Empty live queue: the run doubles as a reconnect probe.
  const queue = new Set();
  const deadLetter = new Set(['/q/dead-1.json', '/q/dead-2.json']);
  const runtime = fakeRuntimeWithDeadLetter(queue, deadLetter);
  const shipped = [];

  const result = await drainQueue(runtime, async (filePath) => {
    shipped.push(filePath);
    queue.delete(filePath);
    return true;
  });

  assert.deepEqual(shipped, ['/q/dead-1.json', '/q/dead-2.json'], 'no envelope may ship twice');
  assert.equal(result.shipped, 2);
  assert.equal(result.replayed, 2);
  assert.equal(runtime.calls.replay, 1);
  assert.equal(runtime.calls.prune, 1, 'pruning runs before replay so expired entries are not replayed');
  assert.equal(queue.size, 0);
  assert.equal(deadLetter.size, 0);
});

test('a live success in the same run unlocks the dead-letter replay (DEV-936)', async () => {
  const queue = new Set(['/q/live.json']);
  const deadLetter = new Set(['/q/dead.json']);
  const runtime = fakeRuntimeWithDeadLetter(queue, deadLetter);
  const shipped = [];

  const result = await drainQueue(runtime, async (filePath) => {
    shipped.push(filePath);
    queue.delete(filePath);
    return true;
  });

  assert.deepEqual(shipped, ['/q/live.json', '/q/dead.json']);
  assert.equal(result.shipped, 2);
  assert.equal(result.replayed, 1);
  assert.equal(runtime.calls.replay, 1);
});

test('a run where nothing shipped leaves the dead-letter store alone (DEV-936)', async () => {
  // The backend is still unreachable, so replaying would only burn the parked
  // envelopes' fresh retry budget for nothing.
  const queue = new Set(['/q/live.json']);
  const deadLetter = new Set(['/q/dead.json']);
  const runtime = fakeRuntimeWithDeadLetter(queue, deadLetter);

  const result = await drainQueue(runtime, async () => false);

  assert.equal(result.shipped, 0);
  assert.equal(result.replayed, 0);
  assert.equal(runtime.calls.replay, 0);
  assert.equal(runtime.calls.prune, 1, 'the age/byte ceiling must not depend on connectivity');
  assert.equal(deadLetter.size, 1, 'nothing is lost');
});

test('replay is bounded to one pass per run so a still-down backend cannot spin the shipper (DEV-936)', async () => {
  const queue = new Set();
  const deadLetter = new Set(['/q/dead.json']);
  const runtime = fakeRuntimeWithDeadLetter(queue, deadLetter);
  let attempts = 0;

  const result = await drainQueue(runtime, async () => {
    attempts += 1;
    // Still failing: the envelope stays on disk with one spent attempt.
    return false;
  });

  assert.equal(attempts, 1, 'the replayed envelope is attempted once, then the run ends');
  assert.equal(runtime.calls.replay, 1, 'exactly one replay pass per run');
  assert.deepEqual(result, { passes: 1, attempted: 1, shipped: 0, replayed: 1 });
});

test('drainQueue honours replay: false for callers that only want the live queue (DEV-936)', async () => {
  const queue = new Set();
  const deadLetter = new Set(['/q/dead.json']);
  const runtime = fakeRuntimeWithDeadLetter(queue, deadLetter);

  const result = await drainQueue(runtime, async () => true, { replay: false });

  assert.equal(result.replayed, 0);
  assert.equal(runtime.calls.replay, 0);
  assert.equal(deadLetter.size, 1);
});
