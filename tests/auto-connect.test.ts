/**
 * Regression tests for automatic Telegram connection on session start
 * Covers the refusal rules and the bounded retry, including non-happy paths
 */

import assert from "node:assert/strict";
import test from "node:test";

import {
  createTelegramAutoConnectSupervisor,
  planTelegramAutoConnect,
  runTelegramAutoConnect,
  TELEGRAM_AUTO_CONNECT_BACKOFF_MS,
  type TelegramAutoConnectInput,
} from "../lib/auto-connect.ts";

/**
 * Supervisor with a hand-driven clock and a connect whose outcome the test
 * controls, so every branch is reachable without timers or a network.
 */
function supervisorHarness(
  options: { connectSucceeds?: boolean; enabled?: boolean } = {},
) {
  let connected = false;
  let connects = 0;
  const phases: string[] = [];
  let fire: (() => void) | undefined;
  const deps = {
    isAutoConnectEnabled: () => options.enabled ?? true,
    hasBotToken: () => true,
    isConnected: () => connected,
    getConnectHandler: () => async () => {
      connects += 1;
      if (options.connectSucceeds !== false) connected = true;
    },
    recordRuntimeEvent: (_c: string, _e: unknown, d?: Record<string, unknown>) => {
      phases.push(String(d?.phase));
    },
    sleep: async () => {},
    backoffMs: [0] as readonly number[],
    setTimer: (fn: () => void) => {
      fire = fn;
      return { unref() {} };
    },
    clearTimer: () => {
      fire = undefined;
    },
  };
  const sup = createTelegramAutoConnectSupervisor(deps, () => ({ id: "ctx" }));
  return {
    sup,
    phases,
    connects: () => connects,
    setConnected: (v: boolean) => {
      connected = v;
    },
    isConnected: () => connected,
    armed: () => fire !== undefined,
    tick: async () => {
      const f = fire;
      fire = undefined;
      f?.();
      await new Promise((r) => setTimeout(r, 0));
    },
  };
}

test("A binding lost after startup is reconnected", async () => {
  // The case this exists for: a leader restart reclaims a live session's
  // topic, and nothing else would ever bring it back.
  const h = supervisorHarness();
  h.setConnected(true);
  h.sup.start();

  await h.sup.check();
  assert.equal(h.connects(), 0, "a healthy binding is left alone");

  h.setConnected(false);
  await h.sup.check();
  assert.equal(h.connects(), 1);
  assert.ok(h.isConnected());
  assert.ok(h.phases.includes("binding-lost"));
});

test("A manual disconnect is not undone by the supervisor", async () => {
  // Reconnecting here would argue with the operator and recreate the topic
  // they just asked to delete.
  const h = supervisorHarness();
  h.sup.start();
  h.sup.suspend("manual disconnect");
  h.setConnected(false);

  await h.sup.check();
  assert.equal(h.connects(), 0, "suspended means suspended");
  assert.ok(!h.armed(), "and the timer is cancelled, not left running");
  assert.ok(h.phases.includes("supervisor-suspended"));

  // Only an explicit start resumes it.
  h.sup.start();
  await h.sup.check();
  assert.equal(h.connects(), 1);
});

test("Reconnects do not overlap or stack timers", async () => {
  const h = supervisorHarness();
  h.sup.start();
  const armedOnce = h.armed();
  h.sup.start();
  h.sup.start();
  assert.ok(armedOnce && h.armed(), "repeat starts must not stack timers");

  h.setConnected(false);
  // Three checks dispatched together must produce one connect, not three.
  await Promise.all([h.sup.check(), h.sup.check(), h.sup.check()]);
  assert.equal(h.connects(), 1);
});

test("A failing reconnect keeps watching instead of giving up", async () => {
  // The usual cause is a leader restart, so the next check is the recovery.
  const h = supervisorHarness({ connectSucceeds: false });
  h.sup.start();
  h.setConnected(false);

  for (let i = 0; i < 3; i += 1) await h.sup.check();
  assert.equal(h.connects(), 3, "it keeps trying");
  // Reported on a widening scale, not once a minute forever.
  assert.equal(
    h.phases.filter((p) => p === "reconnect-failing").length,
    1,
    "only the first failure in the series is reported",
  );
});

test("A disabled bridge is never reconnected", async () => {
  const h = supervisorHarness({ enabled: false });
  h.sup.start();
  h.setConnected(false);
  await h.sup.check();
  assert.equal(h.connects(), 0);
});

test("The watch timer reschedules itself after each check", async () => {
  const h = supervisorHarness();
  h.sup.start();
  assert.ok(h.armed());
  h.setConnected(false);
  await h.tick();
  assert.equal(h.connects(), 1);
  assert.ok(h.armed(), "a check must arm the next one, or watching stops");
});

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
