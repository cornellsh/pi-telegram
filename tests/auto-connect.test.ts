/**
 * Regression tests for automatic Telegram connection on session start
 * Covers the refusal rules and the bounded retry, including non-happy paths
 */

import assert from "node:assert/strict";
import test from "node:test";

import {
  planTelegramAutoConnect,
  runTelegramAutoConnect,
  TELEGRAM_AUTO_CONNECT_BACKOFF_MS,
  type TelegramAutoConnectInput,
} from "../lib/auto-connect.ts";

function input(
  overrides: Partial<TelegramAutoConnectInput> = {},
): TelegramAutoConnectInput {
  return {
    enabled: true,
    mode: "tui",
    alreadyConnected: false,
    hasBotToken: true,
    alreadyAttempted: false,
    ...overrides,
  };
}

test("Auto-connect runs only for an interactive session that can connect", () => {
  assert.deepEqual(planTelegramAutoConnect(input()), { kind: "connect" });
});

test("Auto-connect refuses every case where the answer is not obvious", () => {
  const cases: [Partial<TelegramAutoConnectInput>, string][] = [
    [{ enabled: false }, "disabled"],
    [{ mode: "print" }, "non-interactive"],
    [{ mode: "json" }, "non-interactive"],
    [{ mode: "rpc" }, "non-interactive"],
    [{ hasBotToken: false }, "no-bot-token"],
    [{ alreadyConnected: true }, "already-connected"],
    [{ alreadyAttempted: true }, "already-attempted"],
  ];
  for (const [overrides, reason] of cases) {
    assert.deepEqual(
      planTelegramAutoConnect(input(overrides)),
      { kind: "skip", reason },
      JSON.stringify(overrides),
    );
  }
});

test("A disabled bridge is refused before anything else is considered", () => {
  // Every other signal says connect; `enabled` alone must still win, so
  // turning the feature off is absolute rather than advisory.
  assert.deepEqual(
    planTelegramAutoConnect(
      input({ enabled: false, hasBotToken: false, mode: "print" }),
    ),
    { kind: "skip", reason: "disabled" },
  );
});

test("Auto-connect succeeds on the first attempt without sleeping", async () => {
  let connected = false;
  const sleeps: number[] = [];
  const result = await runTelegramAutoConnect({
    connect: async () => {
      connected = true;
    },
    isConnected: () => connected,
    sleep: async (ms) => {
      sleeps.push(ms);
    },
  });
  assert.equal(result, "connected");
  assert.deepEqual(sleeps, [], "the first attempt must be immediate");
});

test("A transient failure is retried and then reports success", async () => {
  let connected = false;
  let calls = 0;
  const sleeps: number[] = [];
  const events: string[] = [];
  const result = await runTelegramAutoConnect({
    connect: async () => {
      calls += 1;
      if (calls < 3) throw new Error("connect ETIMEDOUT 149.154.166.110:443");
      connected = true;
    },
    isConnected: () => connected,
    sleep: async (ms) => {
      sleeps.push(ms);
    },
    recordRuntimeEvent: (_c, _e, details) => {
      events.push(String(details?.phase));
    },
  });
  assert.equal(result, "connected");
  assert.equal(calls, 3);
  assert.deepEqual(sleeps, [
    TELEGRAM_AUTO_CONNECT_BACKOFF_MS[1],
    TELEGRAM_AUTO_CONNECT_BACKOFF_MS[2],
  ]);
  assert.deepEqual(events, ["attempt-failed", "attempt-failed"]);
});

test("A permanent failure gives up loudly instead of retrying forever", async () => {
  let calls = 0;
  const events: string[] = [];
  const result = await runTelegramAutoConnect({
    connect: async () => {
      calls += 1;
      throw new Error("401 Unauthorized");
    },
    isConnected: () => false,
    sleep: async () => {},
    recordRuntimeEvent: (_c, _e, details) => {
      events.push(String(details?.phase));
    },
  });
  assert.equal(result, "gave-up");
  assert.equal(calls, TELEGRAM_AUTO_CONNECT_BACKOFF_MS.length);
  assert.equal(events.at(-1), "gave-up");
});

test("Auto-connect never rejects, whatever connect throws", async () => {
  // Session start must not be taken down by a bridge failure, including a
  // non-Error rejection, which would otherwise escape the Error branch.
  const result = await runTelegramAutoConnect({
    connect: async () => {
      throw "a string, not an Error";
    },
    isConnected: () => false,
    sleep: async () => {},
  });
  assert.equal(result, "gave-up");
});

test("A connect that returns without binding is not mistaken for success", async () => {
  // The manual command reports some refusals as an ordinary return, so a
  // resolved promise is not proof. Observable state is the only evidence.
  const events: string[] = [];
  const result = await runTelegramAutoConnect({
    connect: async () => {},
    isConnected: () => false,
    sleep: async () => {},
    recordRuntimeEvent: (_c, _e, details) => {
      events.push(String(details?.phase));
    },
  });
  assert.equal(result, "gave-up");
  assert.deepEqual(events, [
    "no-binding",
    "no-binding",
    "no-binding",
    "gave-up",
  ]);
});

test("A hand connect during backoff is adopted instead of replaced", async () => {
  // Reconnecting here would delete the thread the user just created.
  let connected = false;
  let calls = 0;
  const result = await runTelegramAutoConnect({
    connect: async () => {
      calls += 1;
      throw new Error("ETIMEDOUT");
    },
    isConnected: () => connected,
    sleep: async () => {
      connected = true; // the user ran /telegram-connect while we waited
    },
  });
  assert.equal(result, "connected");
  assert.equal(calls, 1, "no second connect once a binding exists");
});

test("An already-connected process performs no attempt at all", async () => {
  let calls = 0;
  const result = await runTelegramAutoConnect({
    connect: async () => {
      calls += 1;
    },
    isConnected: () => true,
    sleep: async () => {},
  });
  assert.equal(result, "already-connected");
  assert.equal(calls, 0);
});
