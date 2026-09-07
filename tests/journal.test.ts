/**
 * Durable Telegram inbound update journal regressions
 * Zones: telegram inbound, filesystem authority, crash recovery
 * Covers schema, identity, dedupe, capacity, atomic publication, and cross-process serialization
 */

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import test from "node:test";

import { getTelegramProcessBirthIdentity } from "../lib/bus.ts";
import {
  createTelegramUpdateJournalBindingKey,
  createTelegramUpdateJournalBindingRuntime,
  createTelegramUpdateJournalBotIdentity,
  createTelegramUpdateQueueHandoffToken,
  discoverTelegramFollowerJournalPaths,
  createTelegramUpdateJournalReceiptScope,
  createTelegramUpdateJournalReceiptScopeResolver,
  createTelegramUpdateJournalRuntimeBindingResolver,
  getTelegramUpdateJournalBindingPath,
  publishTelegramUpdateJournalSegment,
  TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_BYTES,
  TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT,
  createTelegramUpdateJournalStore,
  getTelegramUpdateJournalAdmissionScopes,
  TelegramUpdateJournalError,
  type TelegramJournaledUpdate,
} from "../lib/journal.ts";
import {
  createTelegramWorkspaceAdmissionLedger,
  TelegramWorkspaceAdmissionError,
  type TelegramWorkspaceAdmissionLedger,
} from "../lib/workspace-admission.ts";

const workerPath = fileURLToPath(
  new URL("./fixtures/journal-worker.ts", import.meta.url),
);

test("Follower journal discovery is profile-exact, segment-aware, and fail-closed", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-journal-discovery-"));
  try {
    const defaultA = join(dir, "follower-inbox-aaaaaaaaaaaaaaaa.json");
    const defaultB = join(dir, "follower-inbox-bbbbbbbbbbbbbbbb.json");
    const namedC = join(dir, "follower-inbox-cccccccccccccccc.work.json");
    await writeFile(defaultA, "{}\n");
    await mkdir(`${defaultA}.segments`);
    await mkdir(`${defaultB}.segments`);
    await writeFile(namedC, "{}\n");
    await mkdir(join(dir, "follower-inbox-dddddddddddddddd.json"));
    await writeFile(join(dir, "follower-inbox-not-a-hash.json"), "{}\n");
    assert.deepEqual(discoverTelegramFollowerJournalPaths({ directory: dir }), {
      paths: [defaultA, defaultB], complete: false,
    });
    assert.deepEqual(discoverTelegramFollowerJournalPaths({ directory: dir, profileName: "work" }), {
      paths: [namedC], complete: true,
    });
    assert.deepEqual(discoverTelegramFollowerJournalPaths({
      directory: join(dir, "missing"), profileName: "work",
    }), { paths: [], complete: true });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

function isJournalError(
  error: unknown,
  code: TelegramUpdateJournalError["code"],
): boolean {
  return error instanceof TelegramUpdateJournalError && error.code === code;
}

function runJournalWorker(
  path: string,
  worker: number,
  count: number,
  mode = "append",
): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [
        "--experimental-strip-types",
        workerPath,
        path,
        String(worker),
        String(count),
        mode,
      ],
      { stdio: ["ignore", "ignore", "pipe"] },
    );
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (exitCode) => {
      if (exitCode === 0) resolve();
      else reject(new Error(`journal worker exited ${exitCode}: ${stderr}`));
    });
  });
}

async function withJournalTempDir(
  run: (input: { dir: string; path: string }) => Promise<void>,
): Promise<void> {
  const dir = await mkdtemp(join(tmpdir(), "pi-telegram-journal-"));
  try {
    await run({ dir, path: join(dir, "inbox.work.json") });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

const identity = createTelegramUpdateJournalBotIdentity({
  botToken: "123:journal-secret",
  botId: 42,
});
const queueOwnerIdentity = {
  instanceId: "journal-test-instance",
  processId: process.pid,
  processBirthId: `${process.pid}:journal-test`,
  sessionGeneration: 1,
};

function createStore(
  path: string,
  options: {
    maxEntries?: number;
    maxBytes?: number;
    nowMs?: number;
    getQueueProcessLiveness?: (
      owner: { processId: number; processBirthId: string },
    ) => "alive" | "dead" | "unverifiable";
    workspaceAdmission?: Pick<
      TelegramWorkspaceAdmissionLedger,
      "acquireAdmission" | "releaseAdmission"
    >;
    onPublicationBoundary?: (
      boundary: "before-write" | "after-write-before-rename",
      publicationPath: string,
    ) => void;
    onRecovery?: (event: {
      kind: "repaired" | "reset";
      path: string;
      revision?: number;
      quarantinePath?: string;
      reason: string;
    }) => void;
  } = {},
) {
  return createTelegramUpdateJournalStore({
    path,
    profileName: "work",
    botIdentity: identity,
    maxEntries: options.maxEntries,
    maxBytes: options.maxBytes,
    getNowMs: () => options.nowMs ?? 1_000,
    queueRuntimeIdentity: {
      instanceId: queueOwnerIdentity.instanceId,
      processId: queueOwnerIdentity.processId,
      processBirthId: queueOwnerIdentity.processBirthId,
    },
    getQueueProcessLiveness: options.getQueueProcessLiveness,
    workspaceAdmission: options.workspaceAdmission,
    onPublicationBoundary: options.onPublicationBoundary,
    onRecovery: options.onRecovery,
  });
}

test("Update journal runtime binding separates worker and process recovery identity", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-telegram-journal-binding-"));
  let profileName: string | undefined;
  let botToken: string | undefined;
  let botId: number | undefined;
  const resolveBinding = createTelegramUpdateJournalRuntimeBindingResolver({
    getProfileName: () => profileName,
    getBotToken: () => botToken,
    getBotId: () => botId,
    getJournalPath: (profile) =>
      join(directory, profile ? `inbox.${profile}.json` : "inbox.json"),
    getQueueRuntimeIdentity: () => ({
      instanceId: queueOwnerIdentity.instanceId,
      processId: queueOwnerIdentity.processId,
      processBirthId: queueOwnerIdentity.processBirthId,
    }),
  });
  try {
    assert.equal(resolveBinding(), undefined);
    botToken = "token-a";
    botId = 7;
    const first = resolveBinding()!;
    assert.equal(
      first.recoveryKey,
      createTelegramUpdateJournalBindingKey({
        path: join(directory, "inbox.json"),
        profileName: "default",
        botIdentity: createTelegramUpdateJournalBotIdentity({
          botToken: "token-a",
          botId: 7,
        }),
      }),
    );
    assert.equal(
      getTelegramUpdateJournalBindingPath(first.recoveryKey),
      join(directory, "inbox.json"),
    );
    assert.equal(getTelegramUpdateJournalBindingPath("profile-a"), undefined);
    assert.equal(first.journal.read().profile, "default");
    first.journal.appendBatch([{ update_id: 1 }]);
    assert.throws(
      () =>
        first.journal.markQueued({
          queueKind: "prompt",
          receiptId: "foreign-process",
          sourceUpdateIds: [1],
          owner: {
            ...queueOwnerIdentity,
            processId: process.pid + 1,
            processBirthId: `${process.pid + 1}:foreign`,
          },
        }),
      (error) => isJournalError(error, "conflict"),
    );

    botToken = "token-b";
    const rotated = resolveBinding()!;
    assert.notEqual(rotated.runtimeKey, first.runtimeKey);
    assert.equal(rotated.recoveryKey, first.recoveryKey);

    profileName = "work";
    const named = resolveBinding()!;
    assert.notEqual(named.runtimeKey, rotated.runtimeKey);
    assert.equal(
      named.recoveryKey,
      createTelegramUpdateJournalBindingKey({
        path: join(directory, "inbox.work.json"),
        profileName: "work",
        botIdentity: createTelegramUpdateJournalBotIdentity({
          botToken: "token-b",
          botId: 7,
        }),
      }),
    );
    assert.equal(named.journal.read().profile, "work");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Update journal binding runtime selects leader, follower, and recipient authority", async () => {
  await withJournalTempDir(async ({ dir }) => {
    let follower = false;
    const runtime = createTelegramUpdateJournalBindingRuntime({
      base: {
        getProfileName: () => "work",
        getBotToken: () => "token-a",
        getBotId: () => 7,
        getQueueRuntimeIdentity: () => ({
          instanceId: "instance-a",
          processId: 42,
          processBirthId: "42:start:1",
        }),
      },
      getLeaderJournalPath: () => join(dir, "leader.json"),
      getFollowerJournalPath: (bindingKey) =>
        join(dir, `${bindingKey}.json`),
      getActiveFollowerBindingKey: () => "active-follower",
      isFollowerRegistered: () => follower,
    });
    assert.equal(
      getTelegramUpdateJournalBindingPath(runtime.resolveActive()!.recoveryKey),
      join(dir, "leader.json"),
    );
    follower = true;
    assert.equal(
      getTelegramUpdateJournalBindingPath(runtime.resolveActive()!.recoveryKey),
      join(dir, "active-follower.json"),
    );
    const recipient = runtime.createRecipientResolver("recipient")()!;
    assert.equal(
      getTelegramUpdateJournalBindingPath(recipient.recoveryKey),
      join(dir, "recipient.json"),
    );
    recipient.journal.appendBatch([{ update_id: 1 }]);
    assert.equal(
      recipient.journal.markQueued({
        queueKind: "prompt",
        receiptId: "recipient-receipt",
        sourceUpdateIds: [1],
        owner: {
          instanceId: "recipient-instance",
          processId: 99,
          processBirthId: "99:start:1",
          sessionGeneration: 1,
        },
      }).queueOwner?.instanceId,
      "recipient-instance",
    );
    const discovered = runtime.createPathResolver(
      join(dir, "follower-inbox-0123456789abcdef.work.json"),
    )()!;
    assert.equal(
      getTelegramUpdateJournalBindingPath(discovered.recoveryKey),
      join(dir, "follower-inbox-0123456789abcdef.work.json"),
    );
    assert.equal(
      runtime.getActiveRecoveryKey(),
      runtime.resolveFollower()!.recoveryKey,
    );
  });
});

test("Update journal bot identity is deterministic without persisting tokens", () => {
  const first = createTelegramUpdateJournalBotIdentity({
    botToken: "123:abc",
    botId: 7,
  });
  const second = createTelegramUpdateJournalBotIdentity({
    botToken: "123:abc",
    botId: 7,
  });
  assert.deepEqual(first, second);
  assert.equal(first.botId, 7);
  assert.match(first.tokenSha256, /^[a-f0-9]{64}$/u);
  assert.equal(first.tokenSha256.includes("123:abc"), false);
  assert.throws(
    () => createTelegramUpdateJournalBotIdentity({ botToken: "" }),
    /configured bot token/u,
  );
  assert.throws(
    () =>
      createTelegramUpdateJournalBotIdentity({
        botToken: "123:abc",
        botId: 0,
      }),
    /safe integer/u,
  );
});

test("Update journal receipt scope is profile-bound and stable across proven token rotation", () => {
  const first = createTelegramUpdateJournalReceiptScope({
    profileName: "work",
    botIdentity: createTelegramUpdateJournalBotIdentity({
      botToken: "token-a",
      botId: 42,
    }),
  });
  const rotated = createTelegramUpdateJournalReceiptScope({
    profileName: "work",
    botIdentity: createTelegramUpdateJournalBotIdentity({
      botToken: "token-b",
      botId: 42,
    }),
  });
  const otherProfile = createTelegramUpdateJournalReceiptScope({
    profileName: "other",
    botIdentity: createTelegramUpdateJournalBotIdentity({
      botToken: "token-b",
      botId: 42,
    }),
  });
  const unknownBot = createTelegramUpdateJournalReceiptScope({
    profileName: "work",
    botIdentity: createTelegramUpdateJournalBotIdentity({
      botToken: "token-a",
    }),
  });

  assert.equal(first, rotated);
  assert.notEqual(first, otherProfile);
  assert.notEqual(first, unknownBot);
  assert.equal(first.includes("token-a"), false);
});

test("Update journal receipt scope resolver freezes same-transport bot enrichment", () => {
  let botId: number | undefined;
  let botToken = "token-a";
  const resolve = createTelegramUpdateJournalReceiptScopeResolver({
    getProfileName: () => "work",
    getBotToken: () => botToken,
    getBotId: () => botId,
  });
  const initial = resolve();
  botId = 42;
  assert.equal(resolve(), initial);
  botToken = "token-b";
  const rotated = resolve();
  assert.notEqual(rotated, initial);
  assert.equal(
    rotated,
    createTelegramUpdateJournalReceiptScope({
      profileName: "work",
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken,
        botId,
      }),
    }),
  );
});

test("Update journal derives conservative Workspace admission scopes", () => {
  assert.deepEqual(
    getTelegramUpdateJournalAdmissionScopes([
      {
        update_id: 1,
        message: { chat: { id: 100 }, message_thread_id: 10 },
      },
      {
        update_id: 2,
        callback_query: {
          message: { chat: { id: 200 }, message_thread_id: 20 },
        },
      },
    ]),
    [
      { kind: "target", target: { chatId: 100, threadId: 10 } },
      { kind: "target", target: { chatId: 200, threadId: 20 } },
    ],
  );
  assert.deepEqual(
    getTelegramUpdateJournalAdmissionScopes([
      {
        update_id: 3,
        message: { chat: { id: 100 }, message_thread_id: 10 },
      },
      { update_id: 4, edited_message: { chat: { id: 100 } } },
    ]),
    [{ kind: "chat", chatId: 100 }],
  );
  assert.deepEqual(
    getTelegramUpdateJournalAdmissionScopes([
      { update_id: 5, callback_query: { inline_message_id: "opaque" } },
    ]),
    [{ kind: "profile" }],
  );
  assert.deepEqual(
    getTelegramUpdateJournalAdmissionScopes([
      {
        update_id: 6,
        message: { chat: { id: 100 }, message_thread_id: "invalid" },
      },
    ]),
    [{ kind: "profile" }],
  );
  assert.deepEqual(getTelegramUpdateJournalAdmissionScopes([]), [
    { kind: "profile" },
  ]);
});

test("Update journal holds Workspace admission through publication and releases it", async () => {
  await withJournalTempDir(async ({ dir, path }) => {
    const admissionPath = join(dir, "workspace-admission.json");
    const admission = createTelegramWorkspaceAdmissionLedger({
      path: admissionPath,
      profileKey: "profile:journal",
      owner: {
        processId: process.pid,
        processBirthId: `${process.pid}:journal-admission-test`,
      },
      getProcessLiveness: () => "alive",
    });
    let observedLease = false;
    let observedBlockedFence = false;
    const store = createStore(path, {
      workspaceAdmission: admission,
      onPublicationBoundary(boundary) {
        if (boundary !== "after-write-before-rename") return;
        const snapshot = admission.read();
        observedLease = snapshot.leases.some(
          (lease) =>
            lease.operationKind === "journal.append" &&
            lease.scope.kind === "target" &&
            lease.scope.target.chatId === 100 &&
            lease.scope.target.threadId === 10,
        );
        observedBlockedFence =
          admission.acquireRetirementFence({
            operationId: "journal-boundary-fence",
            retirementIntentId: "journal-boundary-intent",
            bindingKey: "journal-boundary-binding",
            slot: "A",
            target: { chatId: 100, threadId: 10 },
            leaderEpoch: 1,
            retirementRequestedAtMs: 1,
          }).kind === "blocked";
      },
    });
    store.appendBatch([
      {
        update_id: 1,
        message: { chat: { id: 100 }, message_thread_id: 10 },
      },
    ]);
    assert.equal(observedLease, true);
    assert.equal(observedBlockedFence, true);
    assert.deepEqual(admission.read().leases, []);
  });
});

test("Update journal rejects fenced targets and releases admission after failure", async () => {
  await withJournalTempDir(async ({ dir, path }) => {
    const admission = createTelegramWorkspaceAdmissionLedger({
      path: join(dir, "workspace-admission.json"),
      profileKey: "profile:journal",
      owner: {
        processId: process.pid,
        processBirthId: `${process.pid}:journal-admission-test`,
      },
      getProcessLiveness: () => "alive",
    });
    const fence = admission.acquireRetirementFence({
      operationId: "active-retirement",
      retirementIntentId: "active-intent",
      bindingKey: "active-binding",
      slot: "A",
      target: { chatId: 200, threadId: 20 },
      leaderEpoch: 1,
      retirementRequestedAtMs: 1,
    });
    assert.equal(fence.kind, "acquired");
    const blocked = createStore(path, { workspaceAdmission: admission });
    assert.throws(
      () =>
        blocked.appendBatch([
          {
            update_id: 1,
            message: { chat: { id: 100 }, message_thread_id: 10 },
          },
          {
            update_id: 2,
            message: { chat: { id: 200 }, message_thread_id: 20 },
          },
        ]),
      (error) =>
        error instanceof TelegramWorkspaceAdmissionError &&
        error.code === "admission-blocked",
    );
    assert.equal(admission.read().leases.length, 0);
    assert.equal(existsSync(path), false);

    if (fence.kind === "acquired") {
      assert.equal(admission.releaseUnissuedRetirementFence(fence.fence), true);
    }
    const constrained = createStore(path, {
      workspaceAdmission: admission,
      maxEntries: 1,
    });
    assert.throws(
      () =>
        constrained.appendBatch([
          {
            update_id: 1,
            message: { chat: { id: 100 }, message_thread_id: 10 },
          },
          {
            update_id: 2,
            message: { chat: { id: 100 }, message_thread_id: 10 },
          },
        ]),
      (error) => isJournalError(error, "capacity"),
    );
    assert.deepEqual(admission.read().leases, []);
  });
});

test("Update journal cursor-only admission is profile-wide", async () => {
  await withJournalTempDir(async ({ dir, path }) => {
    const admission = createTelegramWorkspaceAdmissionLedger({
      path: join(dir, "workspace-admission.json"),
      profileKey: "profile:journal",
      owner: {
        processId: process.pid,
        processBirthId: `${process.pid}:journal-admission-test`,
      },
      getProcessLiveness: () => "alive",
    });
    const fence = admission.acquireRetirementFence({
      operationId: "other-target-retirement",
      retirementIntentId: "other-target-intent",
      bindingKey: "other-target-binding",
      slot: "B",
      target: { chatId: 999, threadId: 99 },
      leaderEpoch: 1,
      retirementRequestedAtMs: 1,
    });
    assert.equal(fence.kind, "acquired");
    const store = createStore(path, { workspaceAdmission: admission });
    assert.throws(
      () => store.appendBatch([], 1),
      (error) =>
        error instanceof TelegramWorkspaceAdmissionError &&
        error.code === "admission-blocked",
    );
    assert.equal(existsSync(path), false);
  });
});

test("Update journal appends batches, deduplicates exact replay, and removes entries", async () => {
  await withJournalTempDir(async ({ dir, path }) => {
    const store = createStore(path, { nowMs: 1_234 });
    assert.deepEqual(store.read(), {
      version: 1,
      profile: "work",
      botIdentity: identity,
      entries: [],
      exists: false,
      serializedBytes: 0,
    });

    const updates: TelegramJournaledUpdate[] = [
      { update_id: 10, message: { text: "first" } },
      { update_id: 11, callback_query: { data: "next" } },
    ];
    const appended = store.appendBatch(updates);
    assert.deepEqual(appended.addedUpdateIds, [10, 11]);
    assert.deepEqual(appended.duplicateUpdateIds, []);
    assert.equal(appended.entryCount, 2);
    assert.ok(appended.serializedBytes > 0);

    const snapshot = store.read();
    assert.equal(snapshot.exists, true);
    assert.deepEqual(
      snapshot.entries.map((entry) => ({
        updateId: entry.updateId,
        admittedAtMs: entry.admittedAtMs,
        state: entry.state,
      })),
      [
        { updateId: 10, admittedAtMs: 1_234, state: "pending" },
        { updateId: 11, admittedAtMs: 1_234, state: "pending" },
      ],
    );
    snapshot.entries[0]!.update.message = { text: "mutated snapshot" };
    assert.deepEqual(store.read().entries[0]?.update, updates[0]);

    const beforeReplay = await readFile(path, "utf8");
    const replayed = store.appendBatch([
      { message: { text: "first" }, update_id: 10 },
      { callback_query: { data: "next" }, update_id: 11 },
    ]);
    assert.deepEqual(replayed.addedUpdateIds, []);
    assert.deepEqual(replayed.duplicateUpdateIds, [10, 11]);
    assert.equal(await readFile(path, "utf8"), beforeReplay);
    assert.equal((await readFile(path, "utf8")).includes("journal-secret"), false);

    const removed = store.removeCompleted([10, 999]);
    assert.deepEqual(removed.removedUpdateIds, [10]);
    assert.deepEqual(
      store.read().entries.map((entry) => entry.updateId),
      [11],
    );
    assert.deepEqual((await readdir(dir)).sort(), [
      "inbox.work.json",
      "inbox.work.json.segments",
    ]);
    assert.deepEqual(await readdir(`${path}.segments`), [
      "0000000000000001.json",
    ]);
    if (process.platform !== "win32") {
      assert.equal((await stat(path)).mode & 0o777, 0o600);
    }
  });
});

test("Update journal publishes and retains a monotonic admission cursor with its batch", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    const appended = store.appendBatch(
      [{ update_id: 10 }, { update_id: 11 }],
      11,
    );
    assert.deepEqual(appended.addedUpdateIds, [10, 11]);
    assert.equal(store.read().acceptedThroughUpdateId, 11);

    store.removeCompleted([10, 11]);
    const settled = store.read();
    assert.deepEqual(settled.entries, []);
    assert.equal(settled.acceptedThroughUpdateId, 11);

    assert.throws(
      () => store.appendBatch([{ update_id: 12 }], 11),
      (error) => isJournalError(error, "invalid"),
    );
    assert.throws(
      () => store.appendBatch([], 10),
      (error) => isJournalError(error, "conflict"),
    );
    store.appendBatch([], 12);
    assert.equal(store.read().acceptedThroughUpdateId, 12);
  });
});

test("Update journal accepts revision-zero snapshots and publishes private atomic segments", async () => {
  await withJournalTempDir(async ({ dir, path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    assert.equal(store.read().revision, undefined);
    const segment = {
      version: 1 as const,
      revision: 1,
      previousRevision: 0,
      profile: "work",
      botIdentity: identity,
      upsertedEntries: [],
      removedUpdateIds: [1],
    };
    const published = publishTelegramUpdateJournalSegment(path, segment);
    assert.equal(published.revision, 1);
    assert.ok(published.serializedBytes > 0);
    assert.equal(
      published.path,
      join(dir, "inbox.work.json.segments", "0000000000000001.json"),
    );
    assert.equal(
      await readFile(published.path, "utf8"),
      `${JSON.stringify(segment, null, 2)}\n`,
    );
    if (process.platform !== "win32") {
      assert.equal((await stat(published.path)).mode & 0o777, 0o600);
    }
    assert.deepEqual(
      publishTelegramUpdateJournalSegment(path, segment),
      published,
    );
    const reconstructed = store.read();
    assert.equal(reconstructed.revision, 1);
    assert.deepEqual(reconstructed.entries, []);
    assert.throws(
      () =>
        publishTelegramUpdateJournalSegment(path, {
          ...segment,
          revision: 3,
          previousRevision: 2,
        }),
      /revision has a gap/u,
    );
    assert.throws(
      () =>
        publishTelegramUpdateJournalSegment(path, {
          ...segment,
          removedUpdateIds: [],
        }),
      /revision conflicts/u,
    );
  });
});

test("Update journal reconstructs ordered segments, rejects foreign identity, and resets gaps", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    publishTelegramUpdateJournalSegment(path, {
      version: 1,
      revision: 1,
      previousRevision: 0,
      profile: "work",
      botIdentity: identity,
      upsertedEntries: [
        {
          updateId: 2,
          update: { update_id: 2 },
          admittedAtMs: 2,
          state: "pending",
        },
      ],
      removedUpdateIds: [1],
    });
    publishTelegramUpdateJournalSegment(path, {
      version: 1,
      revision: 2,
      previousRevision: 1,
      profile: "work",
      botIdentity: identity,
      upsertedEntries: [],
      removedUpdateIds: [2],
    });
    assert.deepEqual(store.read().entries, []);
    assert.equal(store.read().revision, 2);

    const segmentPath = `${path}.segments/0000000000000002.json`;
    const source = JSON.parse(await readFile(segmentPath, "utf8")) as Record<
      string,
      unknown
    >;
    source.profile = "foreign";
    await writeFile(segmentPath, `${JSON.stringify(source, null, 2)}\n`);
    assert.throws(
      () => store.read(),
      (error: unknown) =>
        error instanceof TelegramUpdateJournalError &&
        error.code === "identity-mismatch",
    );

    source.profile = "work";
    source.previousRevision = 0;
    await writeFile(segmentPath, `${JSON.stringify(source, null, 2)}\n`);
    const reset = store.read();
    assert.equal(reset.revision, undefined);
    assert.deepEqual(reset.entries, []);
  });
});

test("Update journal repairs a revisionless snapshot from a later segment tail", async () => {
  await withJournalTempDir(async ({ path }) => {
    const recoveryEvents: Array<{ kind: "repaired" | "reset"; revision?: number }> = [];
    const store = createStore(path, {
      onRecovery: (event) => recoveryEvents.push(event),
    });
    store.appendBatch([{ update_id: 1 }]);
    await mkdir(`${path}.segments`);
    await writeFile(
      join(`${path}.segments`, "0000000000000002.json"),
      `${JSON.stringify(
        {
          version: 1,
          revision: 2,
          previousRevision: 1,
          profile: "work",
          botIdentity: identity,
          upsertedEntries: [],
          removedUpdateIds: [1],
        },
        null,
        2,
      )}\n`,
    );

    const repaired = store.read();
    assert.equal(repaired.revision, 2);
    assert.deepEqual(repaired.entries, []);
    assert.equal(recoveryEvents[0]?.kind, "repaired");
    assert.equal(recoveryEvents[0]?.revision, 2);
    const snapshot = JSON.parse(await readFile(path, "utf8")) as {
      revision?: number;
    };
    assert.equal(snapshot.revision, 1);
  });
});

test("Update journal quarantines and resets an uncertain orphaned segment history", async () => {
  await withJournalTempDir(async ({ path }) => {
    const recoveryEvents: Array<{
      kind: "repaired" | "reset";
      quarantinePath?: string;
      reason: string;
    }> = [];
    const store = createStore(path, {
      onRecovery: (event) => recoveryEvents.push(event),
    });
    store.appendBatch([{ update_id: 1 }]);
    publishTelegramUpdateJournalSegment(path, {
      version: 1,
      revision: 1,
      previousRevision: 0,
      profile: "work",
      botIdentity: identity,
      upsertedEntries: [],
      removedUpdateIds: [],
    });
    await rm(path);

    const reset = store.read();
    assert.equal(reset.revision, undefined);
    assert.deepEqual(reset.entries, []);
    assert.equal(recoveryEvents.length, 1);
    assert.equal(recoveryEvents[0]?.kind, "reset");
    assert.match(recoveryEvents[0]?.reason ?? "", /missing while .*segments/u);
    const quarantinePath = recoveryEvents[0]?.quarantinePath;
    assert.ok(quarantinePath);
    assert.deepEqual(await readdir(quarantinePath), [
      "inbox.work.json.segments",
    ]);
    assert.deepEqual(
      await readdir(join(quarantinePath, "inbox.work.json.segments")),
      ["0000000000000001.json"],
    );
    await assert.rejects(() => stat(`${path}.segments`), /ENOENT/u);
    assert.deepEqual(store.appendBatch([{ update_id: 2 }]).addedUpdateIds, [2]);
  });
});

test("Update journal restores orphaned segments when reset publication fails", async () => {
  await withJournalTempDir(async ({ path }) => {
    const seed = createStore(path);
    seed.appendBatch([{ update_id: 1 }]);
    publishTelegramUpdateJournalSegment(path, {
      version: 1,
      revision: 1,
      previousRevision: 0,
      profile: "work",
      botIdentity: identity,
      upsertedEntries: [],
      removedUpdateIds: [],
    });
    await rm(path);
    const recovering = createStore(path, {
      onPublicationBoundary: (boundary, publicationPath) => {
        if (boundary === "before-write" && publicationPath === path) {
          throw new Error("reset publication blocked");
        }
      },
    });

    assert.throws(() => recovering.read(), /mutation failed/u);
    await assert.rejects(() => stat(path), /ENOENT/u);
    assert.deepEqual(await readdir(`${path}.segments`), [
      "0000000000000001.json",
    ]);
    assert.deepEqual(seed.read().entries, []);
  });
});

test("Update journal repairs a cleanup-orphaned empty segment history", async () => {
  await withJournalTempDir(async ({ path }) => {
    const recoveryEvents: Array<{ kind: "repaired" | "reset"; revision?: number }> = [];
    const store = createStore(path, {
      onRecovery: (event) => recoveryEvents.push(event),
    });
    store.appendBatch([{ update_id: 1 }]);
    publishTelegramUpdateJournalSegment(path, {
      version: 1,
      revision: 1,
      previousRevision: 0,
      profile: "work",
      botIdentity: identity,
      upsertedEntries: [],
      removedUpdateIds: [1],
    });
    publishTelegramUpdateJournalSegment(path, {
      version: 1,
      revision: 2,
      previousRevision: 1,
      profile: "work",
      botIdentity: identity,
      upsertedEntries: [
        {
          updateId: 2,
          update: { update_id: 2 },
          admittedAtMs: 2,
          state: "pending",
        },
      ],
      removedUpdateIds: [],
    });
    publishTelegramUpdateJournalSegment(path, {
      version: 1,
      revision: 3,
      previousRevision: 2,
      profile: "work",
      botIdentity: identity,
      upsertedEntries: [],
      removedUpdateIds: [2],
    });
    await rm(path);

    const recovered = store.read();
    assert.equal(recovered.revision, 3);
    assert.deepEqual(recovered.entries, []);
    const snapshot = JSON.parse(await readFile(path, "utf8")) as {
      revision?: number;
      entries: unknown[];
    };
    assert.equal(snapshot.revision, 3);
    assert.deepEqual(snapshot.entries, []);
    assert.deepEqual(recoveryEvents, [{
      kind: "repaired",
      revision: 3,
      path,
      reason: "Recovered a missing snapshot from a complete empty segment history.",
    }]);
    assert.deepEqual((await readdir(`${path}.segments`)).sort(), [
      "0000000000000001.json",
      "0000000000000002.json",
      "0000000000000003.json",
    ]);
  });
});

test("Update journal recovers the admission cursor from a cleanup-orphaned empty segment chain", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }], 1);
    store.removeCompleted([1]);
    await rm(path);

    const recovered = store.read();
    assert.equal(recovered.revision, 1);
    assert.deepEqual(recovered.entries, []);
    assert.equal(recovered.acceptedThroughUpdateId, 1);
    const snapshot = JSON.parse(await readFile(path, "utf8")) as {
      acceptedThroughUpdateId?: number;
    };
    assert.equal(snapshot.acceptedThroughUpdateId, 1);
  });
});

test("Update journal compacts at the segment-count threshold without losing authority", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }], 1);
    for (
      let revision = 1;
      revision < TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT;
      revision += 1
    ) {
      const removedUpdateIds = revision % 2 === 1 ? [1] : [];
      const upsertedEntries =
        revision % 2 === 0
          ? [
              {
                updateId: 1,
                update: { update_id: 1 },
                admittedAtMs: 1,
                state: "pending" as const,
              },
            ]
          : [];
      publishTelegramUpdateJournalSegment(path, {
        version: 1,
        revision,
        previousRevision: revision - 1,
        profile: "work",
        botIdentity: identity,
        upsertedEntries,
        removedUpdateIds,
      });
    }
    assert.equal(
      (await readdir(`${path}.segments`)).length,
      TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT - 1,
    );
    const before = store.read();
    assert.equal(
      before.revision,
      TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT - 1,
    );
    assert.deepEqual(before.entries, []);
    assert.equal(before.acceptedThroughUpdateId, 1);

    store.appendBatch([{ update_id: 2 }], 2);
    const compacted = store.read();
    assert.equal(
      compacted.revision,
      TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT,
    );
    assert.deepEqual(
      compacted.entries.map((entry) => entry.updateId),
      [2],
    );
    assert.equal(compacted.acceptedThroughUpdateId, 2);
    await assert.rejects(() => readdir(`${path}.segments`), /ENOENT/u);
    const snapshot = JSON.parse(await readFile(path, "utf8")) as {
      revision?: number;
      acceptedThroughUpdateId?: number;
      entries: Array<{ updateId: number }>;
    };
    assert.equal(
      snapshot.revision,
      TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT,
    );
    assert.equal(snapshot.acceptedThroughUpdateId, 2);
    assert.deepEqual(snapshot.entries.map((entry) => entry.updateId), [2]);
  });
});

test("Update journal compacts at the segment-byte threshold", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    const payload = "x".repeat(350_000);
    let revision = 0;
    let segmentBytes = 0;
    while (segmentBytes < TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_BYTES) {
      revision += 1;
      const published = publishTelegramUpdateJournalSegment(path, {
        version: 1,
        revision,
        previousRevision: revision - 1,
        profile: "work",
        botIdentity: identity,
        upsertedEntries: [
          {
            updateId: 1,
            update: { update_id: 1, message: { text: `${revision}:${payload}` } },
            admittedAtMs: 1,
            state: "pending",
          },
        ],
        removedUpdateIds: [],
      });
      segmentBytes += published.serializedBytes;
    }
    assert.ok(
      revision < TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT,
      "byte threshold must trigger before the count threshold",
    );

    store.appendBatch([{ update_id: 2 }]);
    const compacted = store.read();
    assert.equal(compacted.revision, revision + 1);
    assert.deepEqual(
      compacted.entries.map((entry) => entry.updateId),
      [1, 2],
    );
    await assert.rejects(() => readdir(`${path}.segments`), /ENOENT/u);
  });
});

test("Update journal retains authority when compaction cleanup is interrupted", async () => {
  await withJournalTempDir(async ({ path }) => {
    await writeFile(
      path,
      `${JSON.stringify(
        {
          version: 1,
          revision: 1,
          profile: "work",
          botIdentity: identity,
          entries: [
            {
              updateId: 1,
              update: { update_id: 1 },
              admittedAtMs: 1,
              state: "pending",
            },
          ],
        },
        null,
        2,
      )}\n`,
    );
    await mkdir(`${path}.segments`);
    await mkdir(join(`${path}.segments`, "0000000000000001.json"));
    for (
      let revision = 2;
      revision <= TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT + 1;
      revision += 1
    ) {
      await writeFile(
        join(
          `${path}.segments`,
          `${String(revision).padStart(16, "0")}.json`,
        ),
        `${JSON.stringify({
          version: 1,
          revision,
          previousRevision: revision - 1,
          profile: "work",
          botIdentity: identity,
          upsertedEntries: [],
          removedUpdateIds: [],
        })}\n`,
      );
    }

    const store = createStore(path);
    store.appendBatch([{ update_id: 2 }]);
    const reconstructed = store.read();
    assert.equal(
      reconstructed.revision,
      TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT + 2,
    );
    assert.deepEqual(
      reconstructed.entries.map((entry) => entry.updateId),
      [1, 2],
    );
    assert.equal(
      (await readdir(`${path}.segments`)).length,
      1,
      "failed unlink cleanup leaves only the redundant undeletable revision",
    );
    const snapshot = JSON.parse(await readFile(path, "utf8")) as {
      revision?: number;
      entries: Array<{ updateId: number }>;
    };
    assert.equal(
      snapshot.revision,
      TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT + 2,
    );
    assert.deepEqual(snapshot.entries.map((entry) => entry.updateId), [1, 2]);
  });
});

test("Update journal compaction thresholds exclude redundant old segments", async () => {
  await withJournalTempDir(async ({ path }) => {
    await writeFile(
      path,
      `${JSON.stringify(
        {
          version: 1,
          revision: TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT - 1,
          profile: "work",
          botIdentity: identity,
          entries: [],
        },
        null,
        2,
      )}\n`,
    );
    await mkdir(`${path}.segments`);
    for (
      let revision = 1;
      revision < TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT;
      revision += 1
    ) {
      await mkdir(
        join(`${path}.segments`, `${String(revision).padStart(16, "0")}.json`),
      );
    }
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    const rawSnapshot = JSON.parse(await readFile(path, "utf8")) as {
      revision?: number;
    };
    assert.equal(
      rawSnapshot.revision,
      TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT - 1,
      "one unapplied segment must not compact because redundant cleanup remains",
    );
    assert.equal(
      store.read().revision,
      TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT,
    );
    assert.ok(
      (await readdir(`${path}.segments`)).includes(
        `${String(TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT).padStart(16, "0")}.json`,
      ),
    );
  });
});

test("Update journal bounds aggregate unapplied segment bytes", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    const payload = "x".repeat(1_000);
    for (let revision = 1; revision <= 2; revision += 1) {
      publishTelegramUpdateJournalSegment(path, {
        version: 1,
        revision,
        previousRevision: revision - 1,
        profile: "work",
        botIdentity: identity,
        upsertedEntries: [
          {
            updateId: 1,
            update: { update_id: 1, message: { text: `${revision}:${payload}` } },
            admittedAtMs: 1,
            state: "pending",
          },
        ],
        removedUpdateIds: [],
      });
    }
    const segmentSizes = await Promise.all(
      (await readdir(`${path}.segments`)).map(async (name) =>
        stat(join(`${path}.segments`, name)),
      ),
    );
    const largestSegment = Math.max(...segmentSizes.map((entry) => entry.size));
    const aggregateBytes = segmentSizes.reduce(
      (total, entry) => total + entry.size,
      0,
    );
    const maxBytes = largestSegment + 100;
    assert.ok(aggregateBytes > maxBytes);
    const constrained = createStore(path, { maxBytes });
    assert.throws(
      () => constrained.read(),
      (error: unknown) =>
        error instanceof TelegramUpdateJournalError &&
        error.code === "capacity" &&
        /unapplied-segment limit/u.test(error.message),
    );
    assert.equal(store.read().revision, 2);
  });
});

test("Update journal accepts a newer snapshot with a redundant old segment", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    publishTelegramUpdateJournalSegment(path, {
      version: 1,
      revision: 1,
      previousRevision: 0,
      profile: "work",
      botIdentity: identity,
      upsertedEntries: [],
      removedUpdateIds: [1],
    });
    await writeFile(
      path,
      `${JSON.stringify(
        {
          version: 1,
          revision: 1,
          profile: "work",
          botIdentity: identity,
          entries: [],
        },
        null,
        2,
      )}\n`,
    );

    const reconstructed = store.read();
    assert.equal(reconstructed.revision, 1);
    assert.deepEqual(reconstructed.entries, []);
    assert.deepEqual(await readdir(`${path}.segments`), [
      "0000000000000001.json",
    ]);
    store.appendBatch([{ update_id: 2 }]);
    assert.equal(store.read().revision, 2);
    assert.deepEqual(
      store.read().entries.map((entry) => entry.updateId),
      [2],
    );
  });
});

test("Update journal publication interruption preserves prior authority", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    const snapshotBefore = await readFile(path, "utf8");

    for (const boundary of [
      "before-write",
      "after-write-before-rename",
    ] as const) {
      const interrupted = createStore(path, {
        onPublicationBoundary(candidate, publicationPath) {
          if (
            candidate === boundary &&
            publicationPath.endsWith("0000000000000001.json")
          ) {
            throw new Error(`interrupted:${boundary}`);
          }
        },
      });
      assert.throws(
        () => interrupted.removeCompleted([1]),
        (error: unknown) =>
          error instanceof TelegramUpdateJournalError &&
          (error.cause as Error | undefined)?.message ===
            `interrupted:${boundary}`,
      );
      assert.equal(await readFile(path, "utf8"), snapshotBefore);
      assert.deepEqual(await readdir(`${path}.segments`), []);
      assert.deepEqual(
        store.read().entries.map((entry) => entry.updateId),
        [1],
      );
    }
  });
});

test("Update journal cursor publication interruption retains the prior batch and cursor", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }], 1);
    for (const boundary of [
      "before-write",
      "after-write-before-rename",
    ] as const) {
      const interrupted = createStore(path, {
        onPublicationBoundary(candidate, publicationPath) {
          if (
            candidate === boundary &&
            publicationPath.endsWith("0000000000000001.json")
          ) {
            throw new Error(`cursor-interrupted:${boundary}`);
          }
        },
      });
      assert.throws(
        () => interrupted.appendBatch([{ update_id: 2 }], 2),
        (error: unknown) =>
          error instanceof TelegramUpdateJournalError &&
          (error.cause as Error | undefined)?.message ===
            `cursor-interrupted:${boundary}`,
      );
      const retained = store.read();
      assert.equal(retained.acceptedThroughUpdateId, 1);
      assert.deepEqual(retained.entries.map((entry) => entry.updateId), [1]);
    }
  });
});

test("Update journal queue and completion interruption retain prior authority", async () => {
  await withJournalTempDir(async ({ path }) => {
    const owner = { ...queueOwnerIdentity };
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    let operation: "queue" | "completion" = "queue";
    const interrupted = createStore(path, {
      onPublicationBoundary(boundary, publicationPath) {
        if (
          boundary === "after-write-before-rename" &&
          /\.segments[\\/]/u.test(publicationPath)
        ) {
          throw new Error(`interrupted:${operation}`);
        }
      },
    });
    assert.throws(
      () =>
        interrupted.markQueued({
          queueKind: "prompt",
          receiptId: "publication-receipt",
          sourceUpdateIds: [1],
          owner,
        }),
      (error: unknown) =>
        error instanceof TelegramUpdateJournalError &&
        (error.cause as Error | undefined)?.message === "interrupted:queue",
    );
    assert.equal(store.read().entries[0]?.state, "pending");

    const queued = store.markQueued({
      queueKind: "prompt",
      receiptId: "publication-receipt",
      sourceUpdateIds: [1],
      owner,
    });
    operation = "completion";
    assert.throws(
      () =>
        interrupted.completeQueued([
          {
            queueKind: "prompt",
            receiptId: "publication-receipt",
            sourceUpdateIds: [1],
            queueOwner: queued.queueOwner!,
          },
        ]),
      (error: unknown) =>
        error instanceof TelegramUpdateJournalError &&
        (error.cause as Error | undefined)?.message ===
          "interrupted:completion",
    );
    assert.equal(store.read().entries[0]?.state, "queued");
    assert.deepEqual(store.read().entries[0]?.queueOwner, queued.queueOwner);
  });
});

test("Update journal capacity rejection preserves exact snapshot and segment bytes", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    store.markExecutionFailure({
      updateId: 1,
      expectedAttemptCount: 0,
      failedAtMs: 1,
      failureClass: "retry",
      summary: "short",
      disposition: "retry-wait",
      nextRetryAtMs: 2,
    });
    const snapshotBefore = await readFile(path, "utf8");
    const segmentDirectory = `${path}.segments`;
    const segmentNamesBefore = await readdir(segmentDirectory);
    const segmentsBefore = await Promise.all(
      segmentNamesBefore.map((name) =>
        readFile(join(segmentDirectory, name), "utf8"),
      ),
    );
    const currentBytes = store.read().serializedBytes;
    const constrained = createStore(path, { maxBytes: currentBytes + 1 });

    assert.throws(
      () =>
        constrained.markExecutionFailure({
          updateId: 1,
          expectedAttemptCount: 1,
          failedAtMs: 2,
          failureClass: "terminal",
          summary: "x".repeat(512),
          disposition: "failed",
          terminalReason: "capacity-boundary",
        }),
      (error: unknown) =>
        error instanceof TelegramUpdateJournalError && error.code === "capacity",
    );
    assert.equal(await readFile(path, "utf8"), snapshotBefore);
    assert.deepEqual(await readdir(segmentDirectory), segmentNamesBefore);
    assert.deepEqual(
      await Promise.all(
        segmentNamesBefore.map((name) =>
          readFile(join(segmentDirectory, name), "utf8"),
        ),
      ),
      segmentsBefore,
    );
    assert.equal(store.read().entries[0]?.state, "retry-wait");
  });
});

test("Update journal completion drain writes bounded segments without rewriting raw updates", async () => {
  await withJournalTempDir(async ({ path }) => {
    const entryCount = 2_048;
    const store = createStore(path);
    store.appendBatch(
      Array.from({ length: entryCount }, (_, index) => ({
        update_id: index + 1,
        message: { text: `retained-raw-update-${index + 1}` },
      })),
    );
    const snapshotBefore = await readFile(path, "utf8");
    const snapshotBytes = Buffer.byteLength(snapshotBefore);
    for (let offset = 0; offset < entryCount; offset += 64) {
      store.removeCompleted(
        Array.from({ length: 64 }, (_, index) => offset + index + 1),
      );
    }
    assert.equal(await readFile(path, "utf8"), snapshotBefore);
    assert.deepEqual(store.read().entries, []);
    const segmentNames = await readdir(`${path}.segments`);
    assert.equal(segmentNames.length, entryCount / 64);
    const segmentBytes = (
      await Promise.all(
        segmentNames.map((name) => stat(join(`${path}.segments`, name))),
      )
    ).reduce((total, entry) => total + entry.size, 0);
    assert.ok(segmentBytes < snapshotBytes);
  });
});

test("Update journal rejects conflicting and unordered batches atomically", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 10, message: { text: "first" } }]);

    assert.throws(
      () =>
        store.appendBatch([
          { update_id: 10, message: { text: "changed" } },
          { update_id: 11, message: { text: "new" } },
        ]),
      (error) => isJournalError(error, "conflict"),
    );
    assert.deepEqual(
      store.read().entries.map((entry) => entry.updateId),
      [10],
    );

    assert.throws(
      () =>
        store.appendBatch([
          { update_id: 12 },
          { update_id: 11 },
        ]),
      (error) => isJournalError(error, "invalid"),
    );
    assert.deepEqual(
      store.read().entries.map((entry) => entry.updateId),
      [10],
    );
  });
});

test("Update journal capacity rejects a whole batch without partial publication", async () => {
  await withJournalTempDir(async ({ path }) => {
    const entryBounded = createStore(path, { maxEntries: 2 });
    entryBounded.appendBatch([{ update_id: 1 }]);
    assert.throws(
      () =>
        entryBounded.appendBatch([{ update_id: 2 }, { update_id: 3 }]),
      (error) => isJournalError(error, "capacity"),
    );
    assert.deepEqual(
      entryBounded.read().entries.map((entry) => entry.updateId),
      [1],
    );
  });

  await withJournalTempDir(async ({ path }) => {
    const byteBounded = createStore(path, { maxBytes: 600 });
    assert.throws(
      () =>
        byteBounded.appendBatch([
          { update_id: 1, message: { text: "x".repeat(2_000) } },
        ]),
      (error) => isJournalError(error, "capacity"),
    );
    assert.equal(byteBounded.read().exists, false);
    await writeFile(path, "x".repeat(601), "utf8");
    assert.throws(
      () => byteBounded.read(),
      (error) => isJournalError(error, "capacity"),
    );
  });
});

test("Update journal validates queued receipt schema", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    const queuedEntry = {
      updateId: 5,
      update: { update_id: 5, message: { text: "queued" } },
      admittedAtMs: 1,
      state: "queued",
      queueKind: "prompt",
      queueReceiptId: "receipt-5",
    };
    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "work",
        botIdentity: identity,
        entries: [queuedEntry],
      }),
      "utf8",
    );
    assert.throws(
      () => store.read(),
      (error) => isJournalError(error, "invalid"),
    );

    const queueOwner = {
      ...queueOwnerIdentity,
      acquisitionId: "acquisition-5",
      acquiredAtMs: 2,
    };
    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "work",
        botIdentity: identity,
        entries: [{ ...queuedEntry, queueOwner }],
      }),
      "utf8",
    );
    assert.deepEqual(store.read().entries[0]?.queueOwner, queueOwner);

    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "work",
        botIdentity: identity,
        entries: [
          {
            ...queuedEntry,
            queueOwner: { ...queueOwner, sessionGeneration: 0 },
          },
        ],
      }),
      "utf8",
    );
    assert.throws(
      () => store.read(),
      (error) => isJournalError(error, "invalid"),
    );

    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "work",
        botIdentity: identity,
        entries: [{ ...queuedEntry, queueReceiptId: undefined }],
      }),
      "utf8",
    );
    assert.throws(
      () => store.read(),
      (error) => isJournalError(error, "invalid"),
    );

    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "work",
        botIdentity: identity,
        entries: [
          { ...queuedEntry, queueOwner },
          {
            ...queuedEntry,
            updateId: 6,
            update: { update_id: 6 },
            queueOwner: {
              ...queueOwner,
              acquisitionId: "other-acquisition",
            },
          },
        ],
      }),
      "utf8",
    );
    assert.throws(
      () => store.read(),
      (error) => isJournalError(error, "invalid"),
    );
  });
});

test("Update journal persists retry and terminal execution failure transitions", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }, { update_id: 2 }, { update_id: 3 }]);
    const firstFailure = store.markExecutionFailure({
      updateId: 1,
      expectedAttemptCount: 0,
      failedAtMs: 100,
      failureClass: "handler-timeout",
      summary: "Handler timed out.",
      disposition: "retry-wait",
      nextRetryAtMs: 200,
    });
    assert.deepEqual(firstFailure.entry, {
      updateId: 1,
      update: { update_id: 1 },
      admittedAtMs: firstFailure.entry.admittedAtMs,
      state: "retry-wait",
      failure: {
        attemptCount: 1,
        failedAtMs: 100,
        failureClass: "handler-timeout",
        summary: "Handler timed out.",
      },
      nextRetryAtMs: 200,
    });
    const beforeStaleAttempt = await readFile(path, "utf8");
    assert.throws(
      () =>
        store.markExecutionFailure({
          updateId: 1,
          expectedAttemptCount: 0,
          failedAtMs: 150,
          failureClass: "stale-attempt",
          summary: "Stale writer.",
          disposition: "failed",
          terminalReason: "terminal:stale-attempt",
        }),
      (error) => isJournalError(error, "conflict"),
    );
    assert.equal(await readFile(path, "utf8"), beforeStaleAttempt);

    const terminal = store.markExecutionFailure({
      updateId: 1,
      expectedAttemptCount: 1,
      failedAtMs: 200,
      failureClass: "handler-timeout",
      summary: "Handler timed out again.",
      disposition: "failed",
      terminalReason: "retry-exhausted:handler-timeout",
    });
    assert.match(
      terminal.entry.terminalFailureId ?? "",
      /^failure-[a-f0-9]{32}$/u,
    );
    assert.deepEqual(terminal.entry, {
      updateId: 1,
      update: { update_id: 1 },
      admittedAtMs: terminal.entry.admittedAtMs,
      state: "failed",
      failure: {
        attemptCount: 2,
        failedAtMs: 200,
        failureClass: "handler-timeout",
        summary: "Handler timed out again.",
      },
      terminalAtMs: 200,
      terminalReason: "retry-exhausted:handler-timeout",
      terminalFailureId: terminal.entry.terminalFailureId,
    });
    assert.throws(
      () =>
        store.markQueued({
          queueKind: "prompt",
          receiptId: "failed-receipt",
          sourceUpdateIds: [1],
          owner: queueOwnerIdentity,
        }),
      (error) => isJournalError(error, "conflict"),
    );

    store.markExecutionFailure({
      updateId: 2,
      expectedAttemptCount: 0,
      failedAtMs: 300,
      failureClass: "temporary",
      summary: "Temporary failure.",
      disposition: "retry-wait",
      nextRetryAtMs: 400,
    });
    const queuedControl = store.markQueued({
      queueKind: "control",
      receiptId: "control-2",
      sourceUpdateIds: [2],
      owner: queueOwnerIdentity,
    });
    assert.deepEqual(store.read().entries[1], {
      updateId: 2,
      update: { update_id: 2 },
      admittedAtMs: store.read().entries[1]!.admittedAtMs,
      state: "queued",
      queueKind: "control",
      queueReceiptId: "control-2",
      queueOwner: queuedControl.queueOwner,
    });
  });
});

test("Update journal commits exact retry and discard dispositions before terminal authority leaves", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 10 }, { update_id: 11 }]);
    const retryFailure = store.markExecutionFailure({
      updateId: 10,
      expectedAttemptCount: 0,
      failedAtMs: 100,
      failureClass: "retry-target",
      summary: "Retry this update.",
      disposition: "failed",
      terminalReason: "terminal:retry-target",
    }).entry;
    const discardFailure = store.markExecutionFailure({
      updateId: 11,
      expectedAttemptCount: 0,
      failedAtMs: 200,
      failureClass: "discard-target",
      summary: "Discard this update.",
      disposition: "failed",
      terminalReason: "terminal:discard-target",
    }).entry;
    assert.throws(
      () => store.removeCompleted([10]),
      (error) => isJournalError(error, "conflict"),
    );

    const retry = store.applyOperatorDisposition({
      action: "retry",
      updateId: 10,
      failureId: retryFailure.terminalFailureId!,
    });
    assert.equal(retry.duplicate, false);
    assert.deepEqual(store.read().entries[0], {
      updateId: 10,
      update: { update_id: 10 },
      admittedAtMs: retryFailure.admittedAtMs,
      state: "retry-wait",
      failure: retryFailure.failure,
      nextRetryAtMs: 1_000,
    });
    assert.deepEqual(retry.disposition, {
      failureId: retryFailure.terminalFailureId,
      updateId: 10,
      action: "retry",
      committedAtMs: 1_000,
      attemptCount: 1,
      failureClass: "retry-target",
      terminalAtMs: 100,
      terminalReason: "terminal:retry-target",
    });
    const afterRetry = await readFile(path, "utf8");
    assert.equal(
      store.applyOperatorDisposition({
        action: "retry",
        updateId: 10,
        failureId: retryFailure.terminalFailureId!,
      }).duplicate,
      true,
    );
    assert.equal(await readFile(path, "utf8"), afterRetry);
    assert.throws(
      () =>
        store.applyOperatorDisposition({
          action: "discard",
          updateId: 10,
          failureId: retryFailure.terminalFailureId!,
        }),
      (error) => isJournalError(error, "conflict"),
    );
    assert.throws(
      () =>
        store.applyOperatorDisposition({
          action: "retry",
          updateId: 11,
          failureId: "failure-stale",
        }),
      (error) => isJournalError(error, "conflict"),
    );

    const discard = store.applyOperatorDisposition({
      action: "discard",
      updateId: 11,
      failureId: discardFailure.terminalFailureId!,
    });
    assert.equal(discard.duplicate, false);
    assert.deepEqual(
      store.read().entries.map((entry) => entry.updateId),
      [10],
    );
    assert.deepEqual(
      store.read().operatorDispositions?.map((entry) => ({
        updateId: entry.updateId,
        action: entry.action,
        failureId: entry.failureId,
      })),
      [
        {
          updateId: 10,
          action: "retry",
          failureId: retryFailure.terminalFailureId,
        },
        {
          updateId: 11,
          action: "discard",
          failureId: discardFailure.terminalFailureId,
        },
      ],
    );
    assert.equal(
      store.applyOperatorDisposition({
        action: "discard",
        updateId: 11,
        failureId: discardFailure.terminalFailureId!,
      }).duplicate,
      true,
    );
    assert.deepEqual(store.appendBatch([{ update_id: 11 }]), {
      addedUpdateIds: [],
      duplicateUpdateIds: [11],
      entryCount: 1,
      serializedBytes: store.read().serializedBytes,
    });
  });
});

test("Update journal disposition publication failure preserves terminal authority", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    const failed = store.markExecutionFailure({
      updateId: 1,
      expectedAttemptCount: 0,
      failedAtMs: 100,
      failureClass: "capacity-target",
      summary: "Keep this terminal authority.",
      disposition: "failed",
      terminalReason: "terminal:capacity-target",
    }).entry;
    const before = await readFile(path, "utf8");
    const constrained = createStore(path, {
      maxBytes: Buffer.byteLength(before) + 1,
    });
    assert.throws(
      () =>
        constrained.applyOperatorDisposition({
          action: "retry",
          updateId: 1,
          failureId: failed.terminalFailureId!,
        }),
      (error) => isJournalError(error, "capacity"),
    );
    assert.equal(await readFile(path, "utf8"), before);
    assert.equal(store.read().entries[0]?.state, "failed");
    assert.equal(store.read().operatorDispositions, undefined);
  });
});

test("Update journal derives a stable identity for pre-disposition terminal evidence", async () => {
  await withJournalTempDir(async ({ path }) => {
    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "work",
        botIdentity: identity,
        entries: [
          {
            updateId: 5,
            update: { update_id: 5 },
            admittedAtMs: 1,
            state: "failed",
            failure: {
              attemptCount: 2,
              failedAtMs: 10,
              failureClass: "legacy-terminal",
              summary: "Legacy terminal evidence.",
            },
            terminalAtMs: 10,
            terminalReason: "terminal:legacy-terminal",
          },
        ],
      }),
      "utf8",
    );
    const store = createStore(path);
    const firstFailureId = store.read().entries[0]?.terminalFailureId;
    assert.match(firstFailureId ?? "", /^failure-[a-f0-9]{32}$/u);
    assert.equal(
      store.read().entries[0]?.terminalFailureId,
      firstFailureId,
    );
    store.applyOperatorDisposition({
      action: "retry",
      updateId: 5,
      failureId: firstFailureId!,
    });
    assert.equal(store.read().entries[0]?.state, "retry-wait");
    assert.equal(
      store.read().operatorDispositions?.[0]?.failureId,
      firstFailureId,
    );
  });
});

test("Update journal rejects malformed persistent failure metadata", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    const base = {
      updateId: 5,
      update: { update_id: 5 },
      admittedAtMs: 1,
      state: "retry-wait",
      failure: {
        attemptCount: 1,
        failedAtMs: 10,
        failureClass: "temporary",
        summary: "Temporary failure.",
      },
      nextRetryAtMs: 20,
    };
    for (const entry of [
      { ...base, nextRetryAtMs: undefined },
      {
        ...base,
        failure: { ...base.failure, attemptCount: 0 },
      },
      {
        ...base,
        failure: { ...base.failure, summary: "x".repeat(513) },
      },
      {
        ...base,
        state: "failed",
        nextRetryAtMs: undefined,
        terminalAtMs: 10,
      },
    ]) {
      await writeFile(
        path,
        JSON.stringify({
          version: 1,
          profile: "work",
          botIdentity: identity,
          entries: [entry],
        }),
        "utf8",
      );
      assert.throws(
        () => store.read(),
        (error) => isJournalError(error, "invalid"),
      );
    }
  });
});

test("Update journal persists queue ownership and fences exact receipt completion", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }, { update_id: 2 }]);
    const queued = store.markQueued({
      queueKind: "prompt",
      receiptId: "prompt-1",
      sourceUpdateIds: [1, 2],
      owner: queueOwnerIdentity,
    });
    assert.deepEqual(queued.queuedUpdateIds, [1, 2]);
    assert.deepEqual(queued.duplicateUpdateIds, []);
    assert.deepEqual(
      {
        ...queued.queueOwner,
        acquisitionId: undefined,
      },
      {
        ...queueOwnerIdentity,
        acquisitionId: undefined,
        acquiredAtMs: 1_000,
      },
    );
    assert.match(queued.queueOwner?.acquisitionId ?? "", /^[a-f0-9-]{36}$/u);
    assert.deepEqual(
      store.read().entries.map((entry) => ({
        updateId: entry.updateId,
        state: entry.state,
        queueKind: entry.queueKind,
        queueReceiptId: entry.queueReceiptId,
        queueOwner: entry.queueOwner,
      })),
      [
        {
          updateId: 1,
          state: "queued",
          queueKind: "prompt",
          queueReceiptId: "prompt-1",
          queueOwner: queued.queueOwner,
        },
        {
          updateId: 2,
          state: "queued",
          queueKind: "prompt",
          queueReceiptId: "prompt-1",
          queueOwner: queued.queueOwner,
        },
      ],
    );

    const beforeReplay = await readFile(path, "utf8");
    const replayed = store.markQueued({
      queueKind: "prompt",
      receiptId: "prompt-1",
      sourceUpdateIds: [2, 1],
      owner: queueOwnerIdentity,
    });
    assert.deepEqual(replayed.queuedUpdateIds, []);
    assert.deepEqual(replayed.duplicateUpdateIds, [2, 1]);
    assert.deepEqual(replayed.queueOwner, queued.queueOwner);
    assert.equal(await readFile(path, "utf8"), beforeReplay);
    assert.throws(
      () =>
        store.markQueued({
          queueKind: "prompt",
          receiptId: "prompt-1",
          sourceUpdateIds: [1, 2],
          owner: {
            ...queueOwnerIdentity,
            instanceId: "foreign-instance",
          },
        }),
      (error) => isJournalError(error, "conflict"),
    );
    assert.equal(await readFile(path, "utf8"), beforeReplay);
    assert.throws(
      () =>
        store.markQueued({
          queueKind: "prompt",
          receiptId: "prompt-1",
          sourceUpdateIds: [1, 2],
          owner: {
            ...queueOwnerIdentity,
            processBirthId: `${process.pid}:reused-pid`,
          },
        }),
      (error) => isJournalError(error, "conflict"),
    );
    assert.equal(await readFile(path, "utf8"), beforeReplay);

    assert.throws(
      () =>
        store.markQueued({
          queueKind: "prompt",
          receiptId: "prompt-1",
          sourceUpdateIds: [1],
          owner: queueOwnerIdentity,
        }),
      (error) => isJournalError(error, "conflict"),
    );
    assert.throws(
      () => store.removeCompleted([1, 2]),
      (error) => isJournalError(error, "conflict"),
    );
    assert.throws(
      () =>
        store.completeQueued([
          {
            queueKind: "prompt",
            receiptId: "prompt-1",
            sourceUpdateIds: [1, 2],
            queueOwner: {
              ...queued.queueOwner!,
              acquisitionId: "stale-acquisition",
            },
          },
        ]),
      (error) => isJournalError(error, "conflict"),
    );
    assert.equal(await readFile(path, "utf8"), beforeReplay);

    const completed = store.completeQueued([
      {
        queueKind: "prompt",
        receiptId: "prompt-1",
        sourceUpdateIds: [2, 1],
        queueOwner: queued.queueOwner!,
      },
    ]);
    assert.deepEqual(completed.removedUpdateIds, [1, 2]);
    assert.deepEqual(store.read().entries, []);
  });
});

test("Update journal live handoff retains donor authority until exact recipient acceptance", async () => {
  await withJournalTempDir(async ({ path }) => {
    const donorStore = createStore(path, { nowMs: 2_000 });
    donorStore.appendBatch([{ update_id: 1 }, { update_id: 2 }]);
    const receipt = {
      queueKind: "prompt" as const,
      receiptId: "live-handoff-receipt",
      sourceUpdateIds: [1, 2],
    };
    const donorOwner = donorStore.markQueued({
      ...receipt,
      owner: queueOwnerIdentity,
    }).queueOwner!;
    const recipientIdentity = {
      instanceId: "recipient-instance",
      processId: process.pid + 1,
      processBirthId: `${process.pid + 1}:recipient`,
      sessionGeneration: 4,
    };
    const handoffToken = createTelegramUpdateQueueHandoffToken();
    const handoff = {
      ...receipt,
      expectedOwner: donorOwner,
      recipientOwner: recipientIdentity,
      handoffToken,
    };

    const offered = donorStore.offerQueuedHandoff(handoff);
    assert.equal(offered.duplicate, false);
    assert.match(offered.handoff.handoffId, /^handoff-[a-f0-9]{32}$/u);
    assert.deepEqual(offered.handoff.recipientOwner, recipientIdentity);
    assert.deepEqual(
      donorStore.read().entries.map((entry) => ({
        updateId: entry.updateId,
        queueOwner: entry.queueOwner,
        queueHandoff: entry.queueHandoff,
      })),
      [1, 2].map((updateId) => ({
        updateId,
        queueOwner: donorOwner,
        queueHandoff: offered.handoff,
      })),
    );
    assert.equal(donorStore.offerQueuedHandoff(handoff).duplicate, true);
    assert.throws(
      () =>
        donorStore.discardQueued({
          ...receipt,
          expectedOwner: donorOwner,
        }),
      (error) => isJournalError(error, "conflict"),
    );
    assert.throws(
      () =>
        donorStore.recoverDeadQueueOwner({
          ...receipt,
          deadOwner: donorOwner,
          recoveryOwner: recipientIdentity,
        }),
      (error) => isJournalError(error, "conflict"),
    );
    assert.throws(
      () =>
        donorStore.completeQueued([
          { ...receipt, queueOwner: donorOwner },
        ]),
      (error) => isJournalError(error, "conflict"),
    );
    assert.throws(
      () =>
        donorStore.acceptQueuedHandoff({
          ...handoff,
          handoffToken: createTelegramUpdateQueueHandoffToken(),
        }),
      (error) => isJournalError(error, "conflict"),
    );

    const recipientStore = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: identity,
      getNowMs: () => 3_000,
      queueRuntimeIdentity: {
        instanceId: recipientIdentity.instanceId,
        processId: recipientIdentity.processId,
        processBirthId: recipientIdentity.processBirthId,
      },
    });
    const accepted = recipientStore.acceptQueuedHandoff(handoff);
    assert.equal(accepted.duplicate, false);
    assert.notEqual(accepted.queueOwner.acquisitionId, donorOwner.acquisitionId);
    assert.deepEqual(
      {
        ...accepted.queueOwner,
        acquisitionId: undefined,
      },
      {
        ...recipientIdentity,
        acquisitionId: undefined,
        acquiredAtMs: 3_000,
        handoffId: offered.handoff.handoffId,
      },
    );
    assert.equal(recipientStore.acceptQueuedHandoff(handoff).duplicate, true);
    assert.throws(
      () =>
        recipientStore.acceptQueuedHandoff({
          ...handoff,
          handoffToken: createTelegramUpdateQueueHandoffToken(),
        }),
      (error) => isJournalError(error, "conflict"),
    );
    assert.throws(
      () =>
        donorStore.completeQueued([
          { ...receipt, queueOwner: donorOwner },
        ]),
      (error) => isJournalError(error, "conflict"),
    );
    const completed = recipientStore.completeQueued([
      { ...receipt, queueOwner: accepted.queueOwner },
    ]);
    assert.deepEqual(completed.removedUpdateIds, [1, 2]);
    assert.deepEqual(recipientStore.read().entries, []);
  });
});

test("Update journal donor can cancel only its exact unaccepted handoff", async () => {
  await withJournalTempDir(async ({ path }) => {
    const donorStore = createStore(path, { nowMs: 2_000 });
    donorStore.appendBatch([{ update_id: 1 }]);
    const receipt = {
      queueKind: "control" as const,
      receiptId: "cancel-handoff-receipt",
      sourceUpdateIds: [1],
    };
    const donorOwner = donorStore.markQueued({
      ...receipt,
      owner: queueOwnerIdentity,
    }).queueOwner!;
    const recipientOwner = {
      instanceId: "recipient-instance",
      processId: process.pid + 1,
      processBirthId: `${process.pid + 1}:recipient`,
      sessionGeneration: 1,
    };
    const handoff = {
      ...receipt,
      expectedOwner: donorOwner,
      recipientOwner,
      handoffToken: createTelegramUpdateQueueHandoffToken(),
    };
    donorStore.offerQueuedHandoff(handoff);
    assert.throws(
      () =>
        donorStore.cancelQueuedHandoff({
          ...handoff,
          handoffToken: createTelegramUpdateQueueHandoffToken(),
        }),
      (error) => isJournalError(error, "conflict"),
    );
    const cancelled = donorStore.cancelQueuedHandoff(handoff);
    assert.deepEqual(cancelled.cancelledUpdateIds, [1]);
    assert.throws(
      () => donorStore.cancelQueuedHandoff(handoff),
      (error) => isJournalError(error, "conflict"),
    );
    assert.equal(donorStore.read().entries[0]?.queueHandoff, undefined);
    assert.deepEqual(
      donorStore.completeQueued([{ ...receipt, queueOwner: donorOwner }])
        .removedUpdateIds,
      [1],
    );
  });
});

test("Update journal handoff metadata capacity failure preserves queue owner", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path, { nowMs: 2_000 });
    store.appendBatch([{ update_id: 1 }]);
    const receipt = {
      queueKind: "prompt" as const,
      receiptId: "handoff-capacity",
      sourceUpdateIds: [1],
    };
    const owner = store.markQueued({
      ...receipt,
      owner: queueOwnerIdentity,
    }).queueOwner!;
    const before = await readFile(path, "utf8");
    const constrained = createStore(path, {
      maxBytes: Buffer.byteLength(before) + 1,
      nowMs: 2_000,
    });
    assert.throws(
      () =>
        constrained.offerQueuedHandoff({
          ...receipt,
          expectedOwner: owner,
          recipientOwner: {
            instanceId: "recipient-instance",
            processId: process.pid + 1,
            processBirthId: `${process.pid + 1}:recipient`,
            sessionGeneration: 2,
          },
          handoffToken: createTelegramUpdateQueueHandoffToken(),
        }),
      (error) => isJournalError(error, "capacity"),
    );
    assert.equal(await readFile(path, "utf8"), before);
    const entry = store.read().entries[0];
    assert.equal(entry?.state, "queued");
    assert.deepEqual(entry?.queueOwner, owner);
    assert.equal(entry?.queueHandoff, undefined);
  });
});

test("Update journal explicit queue discard fences stale or foreign owners", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path, { nowMs: 2_000 });
    store.appendBatch([{ update_id: 1 }, { update_id: 2 }]);
    const promptOwner = store.markQueued({
      queueKind: "prompt",
      receiptId: "prompt-receipt",
      sourceUpdateIds: [1],
      owner: queueOwnerIdentity,
    }).queueOwner!;
    assert.throws(
      () =>
        store.discardQueued({
          queueKind: "prompt",
          receiptId: "prompt-receipt",
          sourceUpdateIds: [1],
          expectedOwner: {
            ...promptOwner,
            acquisitionId: "stale-acquisition",
          },
        }),
      (error) => isJournalError(error, "conflict"),
    );
    assert.throws(
      () =>
        store.discardQueued({
          queueKind: "prompt",
          receiptId: "prompt-receipt",
          sourceUpdateIds: [1],
          expectedOwner: {
            ...promptOwner,
            instanceId: "foreign-runtime",
          },
        }),
      (error) => isJournalError(error, "conflict"),
    );

    const discardOwner = store.markQueued({
      queueKind: "control",
      receiptId: "discard-receipt",
      sourceUpdateIds: [2],
      owner: queueOwnerIdentity,
    }).queueOwner!;
    const discarded = store.discardQueued({
      queueKind: "control",
      receiptId: "discard-receipt",
      sourceUpdateIds: [2],
      expectedOwner: discardOwner,
    });
    assert.deepEqual(discarded.previousOwner, discardOwner);
    assert.deepEqual(discarded.removedUpdateIds, [2]);
    assert.deepEqual(
      store.read().entries.map((entry) => entry.updateId),
      [1],
    );
  });
});

test("Update journal dead-owner cleanup requires exact negative liveness proof", async () => {
  await withJournalTempDir(async ({ path }) => {
    let ownerLiveness: "alive" | "dead" | "unverifiable" = "alive";
    const store = createStore(path, {
      nowMs: 3_000,
      getQueueProcessLiveness(owner) {
        assert.equal(owner.processId, 444);
        assert.equal(owner.processBirthId, "444:start:dead-owner");
        return ownerLiveness;
      },
    });
    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "work",
        botIdentity: identity,
        entries: [
          {
            updateId: 9,
            update: { update_id: 9 },
            admittedAtMs: 1,
            state: "queued",
            queueKind: "prompt",
            queueReceiptId: "dead-owner-receipt",
            queueOwner: {
              instanceId: "dead-owner-instance",
              processId: 444,
              processBirthId: "444:start:dead-owner",
              sessionGeneration: 1,
              acquisitionId: "dead-owner-acquisition",
              acquiredAtMs: 2,
            },
          },
        ],
      }),
      "utf8",
    );
    const deadOwner = store.read().entries[0]!.queueOwner!;
    const recoveryOwner = {
      ...queueOwnerIdentity,
      sessionGeneration: 5,
    };
    const retained = store.recoverDeadQueueOwner({
      queueKind: "prompt",
      receiptId: "dead-owner-receipt",
      sourceUpdateIds: [9],
      deadOwner,
      recoveryOwner,
    });
    assert.equal(retained.status, "owner-alive");
    assert.deepEqual(store.read().entries[0]?.queueOwner, deadOwner);

    ownerLiveness = "unverifiable";
    const unverifiable = store.recoverDeadQueueOwner({
      queueKind: "prompt",
      receiptId: "dead-owner-receipt",
      sourceUpdateIds: [9],
      deadOwner,
      recoveryOwner,
    });
    assert.equal(unverifiable.status, "owner-unverifiable");
    assert.deepEqual(store.read().entries[0]?.queueOwner, deadOwner);

    ownerLiveness = "dead";
    const recovered = store.recoverDeadQueueOwner({
      queueKind: "prompt",
      receiptId: "dead-owner-receipt",
      sourceUpdateIds: [9],
      deadOwner,
      recoveryOwner,
    });
    assert.equal(recovered.status, "recovered");
    assert.deepEqual(recovered.recoveredUpdateIds, [9]);
    assert.deepEqual(store.read().entries, []);
    assert.throws(
      () =>
        store.recoverDeadQueueOwner({
          queueKind: "prompt",
          receiptId: "dead-owner-receipt",
          sourceUpdateIds: [9],
          deadOwner,
          recoveryOwner,
        }),
      (error) => isJournalError(error, "conflict"),
    );
  });

  await withJournalTempDir(async ({ path }) => {
    const currentBirthOwner = {
      instanceId: "current-birth-instance",
      processId: process.pid,
      processBirthId: getTelegramProcessBirthIdentity(
        process.pid,
        "current-test-process",
      ),
      sessionGeneration: 1,
      acquisitionId: "unknown-birth-acquisition",
      acquiredAtMs: 1,
    };
    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "work",
        botIdentity: identity,
        entries: [
          {
            updateId: 1,
            update: { update_id: 1 },
            admittedAtMs: 1,
            state: "queued",
            queueKind: "prompt",
            queueReceiptId: "current-live-process",
            queueOwner: currentBirthOwner,
          },
        ],
      }),
      "utf8",
    );
    const defaultProofStore = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: identity,
      getNowMs: () => 4_000,
      queueRuntimeIdentity: {
        instanceId: queueOwnerIdentity.instanceId,
        processId: queueOwnerIdentity.processId,
        processBirthId: queueOwnerIdentity.processBirthId,
      },
    });
    const retained = defaultProofStore.recoverDeadQueueOwner({
      queueKind: "prompt",
      receiptId: "current-live-process",
      sourceUpdateIds: [1],
      deadOwner: currentBirthOwner,
      recoveryOwner: {
        ...queueOwnerIdentity,
        sessionGeneration: 2,
      },
    });
    assert.equal(
      retained.status,
      process.platform === "win32" ? "owner-unverifiable" : "owner-alive",
    );
    assert.deepEqual(
      defaultProofStore.read().entries[0]?.queueOwner,
      currentBirthOwner,
    );
  });
});

test("Update journal failure metadata capacity error preserves prior authority", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path, { maxBytes: 500 });
    store.appendBatch([{ update_id: 1 }]);
    assert.throws(
      () =>
        store.markExecutionFailure({
          updateId: 1,
          expectedAttemptCount: 0,
          failedAtMs: 100,
          failureClass: "large-diagnostic",
          summary: "x".repeat(400),
          disposition: "failed",
          terminalReason: "terminal:large-diagnostic",
        }),
      (error) => isJournalError(error, "capacity"),
    );
    assert.equal(store.read().entries[0]?.state, "pending");
  });
});

test("Update journal retry metadata capacity error preserves prior authority", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    const before = await readFile(path, "utf8");
    const constrained = createStore(path, {
      maxBytes: Buffer.byteLength(before) + 1,
    });
    assert.throws(
      () =>
        constrained.markExecutionFailure({
          updateId: 1,
          expectedAttemptCount: 0,
          failedAtMs: 100,
          failureClass: "retry-capacity",
          summary: "Retry metadata must publish atomically.",
          disposition: "retry-wait",
          nextRetryAtMs: 200,
        }),
      (error) => isJournalError(error, "capacity"),
    );
    assert.equal(await readFile(path, "utf8"), before);
    assert.equal(store.read().entries[0]?.state, "pending");
  });
});

test("Update journal queue receipt capacity failure preserves pending authority", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path, { maxBytes: 2_000 });
    store.appendBatch([{ update_id: 1 }]);
    assert.throws(
      () =>
        store.markQueued({
          queueKind: "prompt",
          receiptId: "r".repeat(2_000),
          sourceUpdateIds: [1],
          owner: queueOwnerIdentity,
        }),
      (error) => isJournalError(error, "capacity"),
    );
    const entry = store.read().entries[0];
    assert.equal(entry?.state, "pending");
    assert.equal(entry?.queueReceiptId, undefined);
  });
});

test("Update journal queue-owner metadata capacity failure preserves pending authority", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    store.appendBatch([{ update_id: 1 }]);
    const before = await readFile(path, "utf8");
    const constrained = createStore(path, {
      maxBytes: Buffer.byteLength(before) + 1,
    });
    assert.throws(
      () =>
        constrained.markQueued({
          queueKind: "prompt",
          receiptId: "owner-capacity",
          sourceUpdateIds: [1],
          owner: queueOwnerIdentity,
        }),
      (error) => isJournalError(error, "capacity"),
    );
    assert.equal(await readFile(path, "utf8"), before);
    const entry = store.read().entries[0];
    assert.equal(entry?.state, "pending");
    assert.equal(entry?.queueOwner, undefined);
  });
});

test("Update journal fails closed on malformed schema and identity mismatch", async () => {
  await withJournalTempDir(async ({ path }) => {
    const store = createStore(path);
    const malformedSource = "{broken";
    await writeFile(path, malformedSource, "utf8");
    assert.throws(
      () => store.read(),
      (error) => isJournalError(error, "invalid"),
    );
    assert.equal(await readFile(path, "utf8"), malformedSource);

    await writeFile(
      path,
      JSON.stringify({
        version: 2,
        profile: "work",
        botIdentity: identity,
        entries: [],
      }),
      "utf8",
    );
    assert.throws(
      () => store.read(),
      (error) => isJournalError(error, "unsupported-version"),
    );

    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "other",
        botIdentity: identity,
        entries: [],
      }),
      "utf8",
    );
    const rebound = store.read();
    assert.equal(rebound.profile, "work");
    assert.deepEqual(rebound.botIdentity, identity);

    await writeFile(
      path,
      JSON.stringify({
        version: 1,
        profile: "work",
        botIdentity: identity,
        entries: [
          {
            updateId: 5,
            update: { update_id: 6 },
            admittedAtMs: 1,
            state: "pending",
          },
        ],
      }),
      "utf8",
    );
    assert.throws(
      () => store.read(),
      (error) => isJournalError(error, "invalid"),
    );
  });
});

test("Update journal permits token rotation only when bot identity remains provable", async () => {
  await withJournalTempDir(async ({ path }) => {
    const original = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken: "token-a",
        botId: 42,
      }),
    });
    original.appendBatch([{ update_id: 1 }]);

    const rotated = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken: "token-b",
        botId: 42,
      }),
    });
    assert.equal(rotated.read().entries.length, 1);
    rotated.appendBatch([{ update_id: 1 }]);
    assert.equal(
      rotated.read().botIdentity.tokenSha256,
      createTelegramUpdateJournalBotIdentity({
        botToken: "token-b",
        botId: 42,
      }).tokenSha256,
    );

    const conflictingBot = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken: "token-b",
        botId: 99,
      }),
    });
    assert.throws(
      () => conflictingBot.read(),
      (error) => isJournalError(error, "identity-mismatch"),
    );
  });

  await withJournalTempDir(async ({ path }) => {
    const unknownBot = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken: "token-a",
      }),
    });
    unknownBot.appendBatch([{ update_id: 1 }]);
    const unprovableRotation = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken: "token-b",
      }),
    });
    assert.throws(
      () => unprovableRotation.read(),
      (error) => isJournalError(error, "identity-mismatch"),
    );
  });
});

test("Update journal atomically rebinds a fully drained journal", async () => {
  await withJournalTempDir(async ({ path }) => {
    const original = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken: "token-a",
        botId: 42,
      }),
    });
    original.appendBatch([{ update_id: 1 }], 1);
    original.removeCompleted([1]);
    assert.equal(original.read().entries.length, 0);
    assert.equal(original.read().acceptedThroughUpdateId, 1);

    const reboundIdentity = createTelegramUpdateJournalBotIdentity({
      botToken: "token-b",
      botId: 99,
    });
    const rebound = createTelegramUpdateJournalStore({
      path,
      profileName: "other",
      botIdentity: reboundIdentity,
    });
    const snapshot = rebound.read();
    assert.equal(snapshot.profile, "other");
    assert.deepEqual(snapshot.botIdentity, reboundIdentity);
    assert.deepEqual(snapshot.entries, []);
    assert.equal(snapshot.acceptedThroughUpdateId, undefined);
    rebound.appendBatch([{ update_id: 2 }]);
    assert.deepEqual(
      rebound.read().entries.map((entry) => entry.updateId),
      [2],
    );
    assert.throws(
      () => original.read(),
      (error: unknown) =>
        error instanceof TelegramUpdateJournalError &&
        error.code === "identity-mismatch",
    );
  });
});

test("Update journal serializes concurrent rebind and old-identity append", async () => {
  await withJournalTempDir(async ({ path }) => {
    const original = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken: "123:journal-worker",
        botId: 77,
      }),
    });
    original.appendBatch([{ update_id: 1 }]);
    original.removeCompleted([1]);

    const results = await Promise.allSettled([
      runJournalWorker(path, 1, 1, "append"),
      runJournalWorker(path, 2, 1, "rebind"),
    ]);
    assert.equal(
      results.filter((result) => result.status === "fulfilled").length,
      1,
    );
    assert.equal(
      results.filter((result) => result.status === "rejected").length,
      1,
    );

    const source = JSON.parse(await readFile(path, "utf8")) as {
      profile: string;
      botIdentity: { botId?: number };
    };
    const winner = createTelegramUpdateJournalStore({
      path,
      profileName: source.profile,
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken:
          source.profile === "rebound"
            ? "123:journal-rebound"
            : "123:journal-worker",
        botId: source.botIdentity.botId,
      }),
    });
    const snapshot = winner.read();
    assert.equal(snapshot.entries.length, 1);
    assert.equal(snapshot.entries[0]?.updateId, source.profile === "rebound" ? 20_000 : 10_000);
  });
});

test("Update journal transaction serializes concurrent process appenders", async () => {
  await withJournalTempDir(async ({ path }) => {
    const workers = 4;
    const updatesPerWorker = 12;
    await Promise.all(
      Array.from({ length: workers }, (_value, worker) =>
        runJournalWorker(path, worker + 1, updatesPerWorker),
      ),
    );

    const store = createTelegramUpdateJournalStore({
      path,
      profileName: "work",
      botIdentity: createTelegramUpdateJournalBotIdentity({
        botToken: "123:journal-worker",
        botId: 77,
      }),
    });
    const ids = store.read().entries.map((entry) => entry.updateId);
    assert.equal(ids.length, workers * updatesPerWorker);
    assert.deepEqual(ids, [...ids].sort((left, right) => left - right));
    assert.equal(new Set(ids).size, ids.length);
  });
});
