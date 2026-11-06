/**
 * Cross-instance Telegram agent message regressions
 * Zones: multi-instance bus, inbound routing, operational delivery
 * Mirrors lib/agent-messages.ts and protects live resolution, attribution, and routing fences.
 */

import assert from "node:assert/strict";
import test from "node:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createTelegramAgentMessageRuntime } from "../lib/agent-messages.ts";
import { createTelegramBusFollowerRegistry } from "../lib/bus.ts";
import type { TelegramRoutedMessage } from "../lib/routing.ts";
import { TELEGRAM_INTERNAL_AGENT_MESSAGE } from "../lib/updates.ts";

test("Agent message runtime resolves live names and injects attributed turns", async () => {
  const registry = createTelegramBusFollowerRegistry();
  registry.register({
    instanceId: "hazel",
    connectedAtMs: 1,
    target: { chatId: 7, threadId: 99 },
    threadName: "Hazel",
  });
  const updates: Array<{
    message: TelegramRoutedMessage;
    [TELEGRAM_INTERNAL_AGENT_MESSAGE]?: true;
  }> = [];
  const runtime = createTelegramAgentMessageRuntime({
    instanceId: "isle",
    getAllowedChatId: () => 7,
    getLeaderTarget: () => ({ chatId: 7, threadId: 42 }),
    getLeaderThreadName: () => "Isle",
    followerRegistry: registry,
    getContext: () => ({ id: "ctx" }),
    handleUpdate: async (update: {
      message: TelegramRoutedMessage;
      [TELEGRAM_INTERNAL_AGENT_MESSAGE]?: true;
    }) => {
      updates.push(update);
    },
    getNowMs: () => 5_000,
  });
  assert.deepEqual(
    runtime.resolveTarget(
      { threadName: "hAzEl" },
      { chatId: 7, threadId: 42 },
    ),
    { chatId: 7, threadId: 99 },
  );
  assert.equal(
    runtime.resolveTarget(
      { threadName: "Isle" },
      { chatId: 7, threadId: 42 },
    ),
    undefined,
  );
  await runtime.route({
    sourceTarget: { chatId: 7, threadId: 42 },
    sourceThreadName: "Isle",
    message: {
      target: { chatId: 7, threadId: 99 },
      messageId: 101,
      text: "Review the release",
    },
  });
  assert.equal(updates.length, 1);
  assert.equal(updates[0]![TELEGRAM_INTERNAL_AGENT_MESSAGE], true);
  assert.equal(updates[0]!.message.message_thread_id, 99);
  assert.equal(updates[0]!.message.pi_telegram_agent_source_thread, "Isle");
  assert.equal(updates[0]!.message.text, "Review the release");
});

test("Agent message runtime rejects unknown, ambiguous, and cross-chat targets", () => {
  const registry = createTelegramBusFollowerRegistry();
  for (const instanceId of ["a", "b"]) {
    registry.register({
      instanceId,
      connectedAtMs: 1,
      target: { chatId: 7, threadId: instanceId === "a" ? 10 : 11 },
      threadName: "Hazel",
    });
  }
  const runtime = createTelegramAgentMessageRuntime({
    instanceId: "leader",
    getAllowedChatId: () => 7,
    getLeaderTarget: () => undefined,
    getLeaderThreadName: () => undefined,
    followerRegistry: registry,
    getContext: () => undefined,
    handleUpdate: async () => {},
  });
  assert.equal(runtime.resolveTarget({ threadName: "Missing" }), undefined);
  assert.equal(runtime.resolveTarget({ threadName: "Hazel" }), undefined);
  assert.equal(
    runtime.resolveTarget({ chatId: 8, threadId: 10 }),
    undefined,
  );
});

test("Agent message target resolves a live thread by its topic display name", () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-agent-display-"));
  const previous = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = dir;
  mkdirSync(join(dir, "tmp", "telegram"), { recursive: true });
  writeFileSync(join(dir, "tmp", "telegram", "thread-names.json"), JSON.stringify({ "99": "terminal1" }));
  try {
    const registry = createTelegramBusFollowerRegistry();
    registry.register({ instanceId: "hazel", threadName: "Hazel", target: { chatId: 7, threadId: 99 }, connectedAtMs: 1 });
    const runtime = createTelegramAgentMessageRuntime({
      instanceId: "isle",
      getAllowedChatId: () => 7,
      getLeaderTarget: () => ({ chatId: 7, threadId: 42 }),
      getLeaderThreadName: () => "Isle",
      followerRegistry: registry,
      getContext: () => undefined,
      handleUpdate: async () => {},
    });
    assert.deepEqual(runtime.resolveTarget({ threadName: "Terminal1" }, { chatId: 7, threadId: 42 }), { chatId: 7, threadId: 99 });
    assert.deepEqual(runtime.resolveTarget({ threadName: "hazel" }, { chatId: 7, threadId: 42 }), { chatId: 7, threadId: 99 }, "palette names still work");
  } finally {
    if (previous === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previous;
    rmSync(dir, { recursive: true, force: true });
  }
});

