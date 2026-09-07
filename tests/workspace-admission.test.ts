/**
 * Durable Telegram Workspace admission ledger regressions
 * Zones: telegram workspace identity, filesystem authority, process recovery
 * Covers cross-process exclusion, stale leases, exact recovery, and deletion permits
 */

import assert from "node:assert/strict";
import {
  existsSync,
  mkdtempSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { getTelegramProcessBirthIdentity } from "../lib/bus.ts";
import {
  createTelegramWorkspaceAdmissionLedger,
  createTelegramWorkspaceAdmissionProfileKey,
  createTelegramWorkspaceAdmissionRuntimeBinding,
  runWithTelegramWorkspaceAdmissionsAsync,
  TelegramWorkspaceAdmissionError,
  type TelegramWorkspaceAdmissionLedger,
  type TelegramWorkspaceAdmissionOwner,
  type TelegramWorkspaceRetirementFence,
} from "../lib/workspace-admission.ts";
import { resolveTelegramWorkspaceAdmissionPath } from "../lib/paths.ts";
import { runNodeEval } from "./fixtures/node-eval.ts";

const profileKey = "profile:test";
const target = { chatId: 100, threadId: 10 };
const owner: TelegramWorkspaceAdmissionOwner = {
  processId: 101,
  processBirthId: "101:start:owner",
};

function createTempPath(): { dir: string; path: string } {
  const dir = mkdtempSync(join(tmpdir(), "pi-telegram-workspace-admission-"));
  return { dir, path: join(dir, "workspace-admission.json") };
}

function createLedger(input: {
  path: string;
  owner?: TelegramWorkspaceAdmissionOwner;
  getNowMs?: () => number;
  getProcessLiveness?: () => "alive" | "dead" | "unverifiable";
  publishRename?: typeof renameSync;
}): TelegramWorkspaceAdmissionLedger {
  return createTelegramWorkspaceAdmissionLedger({
    path: input.path,
    profileKey,
    owner: input.owner ?? owner,
    getNowMs: input.getNowMs,
    getProcessLiveness: input.getProcessLiveness ?? (() => "alive"),
    publishRename: input.publishRename,
  });
}

function acquireFence(
  ledger: TelegramWorkspaceAdmissionLedger,
  operationId = "retirement-one",
): TelegramWorkspaceRetirementFence {
  const result = ledger.acquireRetirementFence({
    operationId,
    retirementIntentId: "intent-one",
    bindingKey: "binding-one",
    slot: "A",
    target,
    leaderEpoch: "epoch-one",
    retirementRequestedAtMs: 900,
  });
  assert.equal(result.kind, "acquired");
  return result.fence;
}

function isAdmissionError(
  error: unknown,
  code: TelegramWorkspaceAdmissionError["code"],
): boolean {
  return error instanceof TelegramWorkspaceAdmissionError && error.code === code;
}

test("Workspace admission runtime binds profile paths to stable bot identity", () => {
  const temp = createTempPath();
  try {
    assert.equal(
      resolveTelegramWorkspaceAdmissionPath(temp.dir),
      join(temp.dir, "tmp", "telegram", "workspace-admission.json"),
    );
    assert.equal(
      resolveTelegramWorkspaceAdmissionPath(temp.dir, "work"),
      join(temp.dir, "tmp", "telegram", "workspace-admission.work.json"),
    );
    const firstProfileKey = createTelegramWorkspaceAdmissionProfileKey({
      profileName: "work",
      botToken: "secret-a",
    });
    assert.notEqual(
      firstProfileKey,
      createTelegramWorkspaceAdmissionProfileKey({
        profileName: "work",
        botToken: "secret-b",
      }),
    );
    assert.equal(firstProfileKey.includes("secret"), false);

    let activeProfile: string | undefined;
    let botToken = "secret-default";
    const runtime = createTelegramWorkspaceAdmissionRuntimeBinding({
      getProfileName: () => activeProfile,
      getBotToken: () => botToken,
      getPath: (profileName) =>
        resolveTelegramWorkspaceAdmissionPath(temp.dir, profileName),
      owner,
      getProcessLiveness: () => "alive",
    });
    const initial = runtime.resolve()!;
    const initialProfileKey = initial.getProfileKey();
    const lease = initial.acquireAdmission({
      operationId: "runtime-binding-lease",
      operationKind: "journal.append",
      scope: { kind: "profile" },
    });
    assert.equal(lease.kind, "acquired");

    activeProfile = "work";
    botToken = "secret-work";
    const work = runtime.resolve()!;
    assert.notEqual(work.getProfileKey(), initialProfileKey);
    assert.deepEqual(work.read().leases, []);

    activeProfile = undefined;
    botToken = "secret-default";
    assert.equal(runtime.resolve()?.getProfileKey(), initialProfileKey);
    botToken = "rotated-default";
    const conflicting = runtime.resolve()!;
    assert.notEqual(conflicting.getProfileKey(), initialProfileKey);
    assert.throws(
      () => conflicting.read(),
      (error) => isAdmissionError(error, "invalid-state"),
    );
    botToken = "secret-default";
    const original = runtime.resolve()!;
    if (lease.kind === "acquired") {
      assert.equal(original.releaseAdmission(lease.lease), true);
    }
    botToken = "rotated-default";
    const rebound = runtime.resolve()!;
    assert.deepEqual(rebound.read().leases, []);
    assert.equal(
      rebound.acquireAdmission({
        operationId: "rotated-lease",
        operationKind: "journal.append",
        scope: { kind: "profile" },
      }).kind,
      "acquired",
    );
  } finally {
    rmSync(temp.dir, { recursive: true, force: true });
  }
});

test("Exact, chat-wide, and profile-wide leases fence only their declared scope", () => {
  const exactTemp = createTempPath();
  const chatTemp = createTempPath();
  const profileTemp = createTempPath();
  try {
    const exactLedger = createLedger({ path: exactTemp.path });
    assert.equal(
      exactLedger.acquireAdmission({
        operationId: "exact-lease",
        operationKind: "api.sendMessage",
        scope: { kind: "target", target },
      }).kind,
      "acquired",
    );
    assert.equal(
      exactLedger.acquireRetirementFence({
        operationId: "same-target-fence",
        retirementIntentId: "intent-one",
        bindingKey: "binding-one",
        slot: "A",
        target,
        leaderEpoch: 1,
        retirementRequestedAtMs: 1,
      }).kind,
      "blocked",
    );
    assert.equal(
      exactLedger.acquireRetirementFence({
        operationId: "other-target-fence",
        retirementIntentId: "intent-two",
        bindingKey: "binding-two",
        slot: "B",
        target: { chatId: 100, threadId: 11 },
        leaderEpoch: 1,
        retirementRequestedAtMs: 1,
      }).kind,
      "acquired",
    );
    assert.equal(
      exactLedger.acquireAdmission({
        operationId: "disjoint-target-lease",
        operationKind: "journal.append",
        scope: { kind: "target", target: { chatId: 100, threadId: 12 } },
      }).kind,
      "acquired",
    );
    assert.deepEqual(
      exactLedger.acquireAdmission({
        operationId: "fenced-target-lease",
        operationKind: "api.sendMessage",
        scope: { kind: "target", target: { chatId: 100, threadId: 11 } },
      }),
      { kind: "blocked", reason: "retirement-fenced" },
    );
    assert.deepEqual(
      exactLedger.acquireAdmission({
        operationId: "fenced-chat-lease",
        operationKind: "api.editMessageText",
        scope: { kind: "chat", chatId: 100 },
      }),
      { kind: "blocked", reason: "retirement-fenced" },
    );
    assert.deepEqual(
      exactLedger.acquireRetirementFence({
        operationId: "second-active-fence",
        retirementIntentId: "intent-three",
        bindingKey: "binding-three",
        slot: "C",
        target: { chatId: 200, threadId: 20 },
        leaderEpoch: 1,
        retirementRequestedAtMs: 1,
      }),
      { kind: "blocked", reason: "retirement-active" },
    );

    const chatLedger = createLedger({ path: chatTemp.path });
    chatLedger.acquireAdmission({
      operationId: "chat-lease",
      operationKind: "api.deleteMessage",
      scope: { kind: "chat", chatId: 100 },
    });
    assert.equal(
      chatLedger.acquireRetirementFence({
        operationId: "chat-fence",
        retirementIntentId: "intent-chat",
        bindingKey: "binding-chat",
        slot: "C",
        target,
        leaderEpoch: 1,
        retirementRequestedAtMs: 1,
      }).kind,
      "blocked",
    );

    const profileLedger = createLedger({ path: profileTemp.path });
    profileLedger.acquireAdmission({
      operationId: "profile-lease",
      operationKind: "journal.undecodable",
      scope: { kind: "profile" },
    });
    assert.equal(
      profileLedger.acquireRetirementFence({
        operationId: "profile-fence",
        retirementIntentId: "intent-profile",
        bindingKey: "binding-profile",
        slot: "D",
        target: { chatId: 999, threadId: 44 },
        leaderEpoch: 1,
        retirementRequestedAtMs: 1,
      }).kind,
      "blocked",
    );
  } finally {
    rmSync(exactTemp.dir, { recursive: true, force: true });
    rmSync(chatTemp.dir, { recursive: true, force: true });
    rmSync(profileTemp.dir, { recursive: true, force: true });
  }
});

test("Admission release and acquisition retries are exact and idempotent", () => {
  const temp = createTempPath();
  try {
    const ledger = createLedger({ path: temp.path, getNowMs: () => 123 });
    const first = ledger.acquireAdmission({
      operationId: "stable-operation",
      operationKind: "journal.append",
      scope: { kind: "target", target },
    });
    assert.equal(first.kind, "acquired");
    assert.equal(first.resumed, false);
    const retried = ledger.acquireAdmission({
      operationId: "stable-operation",
      operationKind: "journal.append",
      scope: { kind: "target", target },
    });
    assert.equal(retried.kind, "acquired");
    assert.equal(retried.resumed, true);
    assert.deepEqual(retried.lease, first.lease);
    assert.throws(
      () =>
        ledger.acquireAdmission({
          operationId: "stable-operation",
          operationKind: "api.sendMessage",
          scope: { kind: "target", target },
        }),
      (error) => isAdmissionError(error, "authority-changed"),
    );
    assert.equal(ledger.releaseAdmission(first.lease), true);
    assert.equal(ledger.releaseAdmission(first.lease), false);
  } finally {
    rmSync(temp.dir, { recursive: true, force: true });
  }
});

test("Concurrent admitted operations cannot share one live operation identity", async () => {
  const temp = createTempPath();
  let releaseOperation: (() => void) | undefined;
  try {
    const ledger = createLedger({ path: temp.path });
    const held = new Promise<void>((resolve) => {
      releaseOperation = resolve;
    });
    let operationCalls = 0;
    const first = runWithTelegramWorkspaceAdmissionsAsync({
      ledger,
      operationId: "shared-live-operation",
      operationKind: "api.sendMessage",
      scopes: [{ kind: "target", target }],
      async operation() {
        operationCalls += 1;
        await held;
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(ledger.read().leases.length, 1);

    await assert.rejects(
      () => runWithTelegramWorkspaceAdmissionsAsync({
        ledger,
        operationId: "shared-live-operation",
        operationKind: "api.sendMessage",
        scopes: [{ kind: "target", target }],
        async operation() {
          operationCalls += 1;
        },
      }),
      (error) => isAdmissionError(error, "invalid-state"),
    );
    assert.equal(operationCalls, 1);
    assert.equal(ledger.read().leases.length, 1);
    assert.deepEqual(
      ledger.acquireRetirementFence({
        operationId: "duplicate-helper-fence",
        retirementIntentId: "duplicate-helper-intent",
        bindingKey: "duplicate-helper-binding",
        slot: "A",
        target,
        leaderEpoch: 1,
        retirementRequestedAtMs: 1,
      }),
      { kind: "blocked", reason: "admission-active" },
    );
    if (!releaseOperation) throw new Error("Operation release was not captured.");
    releaseOperation();
    await first;
    assert.deepEqual(ledger.read().leases, []);
    await runWithTelegramWorkspaceAdmissionsAsync({
      ledger,
      operationId: "shared-live-operation",
      operationKind: "api.sendMessage",
      scopes: [{ kind: "target", target }],
      async operation() {
        operationCalls += 1;
      },
    });
    assert.equal(operationCalls, 2);
    assert.deepEqual(ledger.read().leases, []);
  } finally {
    releaseOperation?.();
    rmSync(temp.dir, { recursive: true, force: true });
  }
});

test("Foreign processes cannot release leases or advance destructive phases", () => {
  const leaseTemp = createTempPath();
  const fenceTemp = createTempPath();
  try {
    const leaseLedger = createLedger({ path: leaseTemp.path });
    const leaseResult = leaseLedger.acquireAdmission({
      operationId: "owned-lease",
      operationKind: "api.sendMessage",
      scope: { kind: "target", target },
    });
    assert.equal(leaseResult.kind, "acquired");
    const foreignOwner = {
      processId: 404,
      processBirthId: "404:start:foreign",
    };
    const foreignLeaseLedger = createLedger({
      path: leaseTemp.path,
      owner: foreignOwner,
    });
    assert.throws(
      () => foreignLeaseLedger.releaseAdmission(leaseResult.lease),
      (error) => isAdmissionError(error, "authority-changed"),
    );
    assert.equal(leaseLedger.read().leases.length, 1);

    const fenceLedger = createLedger({ path: fenceTemp.path });
    const fenced = acquireFence(fenceLedger);
    const foreignFenceLedger = createLedger({
      path: fenceTemp.path,
      owner: foreignOwner,
    });
    assert.throws(
      () => foreignFenceLedger.issueDeletionPermit(fenced),
      (error) => isAdmissionError(error, "authority-changed"),
    );
    assert.equal(fenceLedger.read().fence?.phase, "fenced");
  } finally {
    rmSync(leaseTemp.dir, { recursive: true, force: true });
    rmSync(fenceTemp.dir, { recursive: true, force: true });
  }
});

test("Only proven-dead leases are pruned before retirement admission", () => {
  const unverifiableTemp = createTempPath();
  const deadTemp = createTempPath();
  try {
    const staleOwner = {
      processId: 202,
      processBirthId: "202:start:former",
    };
    const createStaleLease = (path: string) => {
      createLedger({ path, owner: staleOwner }).acquireAdmission({
        operationId: "stale-lease",
        operationKind: "journal.append",
        scope: { kind: "target", target },
      });
    };
    createStaleLease(unverifiableTemp.path);
    const unverifiable = createLedger({
      path: unverifiableTemp.path,
      getProcessLiveness: () => "unverifiable",
    });
    assert.equal(
      unverifiable.acquireRetirementFence({
        operationId: "blocked-fence",
        retirementIntentId: "intent-blocked",
        bindingKey: "binding-blocked",
        slot: "A",
        target,
        leaderEpoch: 1,
        retirementRequestedAtMs: 1,
      }).kind,
      "blocked",
    );
    assert.equal(unverifiable.read().leases.length, 1);

    createStaleLease(deadTemp.path);
    const dead = createLedger({
      path: deadTemp.path,
      getProcessLiveness: () => "dead",
    });
    assert.equal(
      dead.acquireRetirementFence({
        operationId: "recovered-fence",
        retirementIntentId: "intent-recovered",
        bindingKey: "binding-recovered",
        slot: "A",
        target,
        leaderEpoch: 1,
        retirementRequestedAtMs: 1,
      }).kind,
      "acquired",
    );
    assert.equal(dead.read().leases.length, 0);
  } finally {
    rmSync(unverifiableTemp.dir, { recursive: true, force: true });
    rmSync(deadTemp.dir, { recursive: true, force: true });
  }
});

test("Malformed, foreign-profile, and unverifiable ledger state fails closed", () => {
  const malformed = createTempPath();
  const foreign = createTempPath();
  try {
    writeFileSync(malformed.path, "{not-json\n", { mode: 0o600 });
    assert.throws(
      () => createLedger({ path: malformed.path }).read(),
      (error) => isAdmissionError(error, "invalid-state"),
    );
    writeFileSync(
      foreign.path,
      `${JSON.stringify({
        version: 1,
        profileKey: "profile:other",
        leases: [{
          operationId: "foreign-lease",
          operationKind: "journal.append",
          profileKey: "profile:other",
          scope: { kind: "profile" },
          owner: { processId: 505, processBirthId: "505:start:foreign" },
          acquiredAtMs: 1,
        }],
      })}\n`,
      { mode: 0o600 },
    );
    assert.throws(
      () => createLedger({ path: foreign.path }).acquireAdmission({
        operationId: "must-not-overwrite",
        operationKind: "journal.append",
        scope: { kind: "profile" },
      }),
      (error) => isAdmissionError(error, "invalid-state"),
    );
  } finally {
    rmSync(malformed.dir, { recursive: true, force: true });
    rmSync(foreign.dir, { recursive: true, force: true });
  }
});

test("Deletion phase grants one permit and retains issued fences until commit", () => {
  const temp = createTempPath();
  let nowMs = 1_000;
  try {
    const ledger = createLedger({ path: temp.path, getNowMs: () => nowMs++ });
    const fenced = acquireFence(ledger);
    assert.deepEqual(ledger.listReservedSlots(), ["A"]);
    const issued = ledger.issueDeletionPermit(fenced);
    assert.equal(issued.kind, "issued");
    assert.deepEqual(issued.permit.target, target);
    assert.equal(issued.permit.slot, "A");
    const retried = ledger.issueDeletionPermit(fenced);
    assert.equal(retried.kind, "already-issued");
    assert.equal(retried.fence.phase, "deletion-issued");
    assert.throws(
      () => ledger.releaseUnissuedRetirementFence(retried.fence),
      (error) => isAdmissionError(error, "authority-changed"),
    );
    const ready = ledger.confirmRetirementAbsence(retried.fence);
    assert.equal(ready.phase, "commit-ready");
    assert.deepEqual(ledger.listReservedSlots(), ["A"]);
    assert.equal(ledger.completeRetirementFence(ready), true);
    assert.deepEqual(ledger.listReservedSlots(), []);
    assert.equal(ledger.completeRetirementFence(ready), false);
  } finally {
    rmSync(temp.dir, { recursive: true, force: true });
  }
});

test("Confirmed already-absence can commit without issuing a deletion permit", () => {
  const temp = createTempPath();
  try {
    const ledger = createLedger({ path: temp.path });
    const fenced = acquireFence(ledger);
    const ready = ledger.confirmRetirementAbsence(fenced);
    assert.equal(ready.phase, "commit-ready");
    assert.equal("deletionIssuedAtMs" in ready, false);
    assert.equal(ledger.completeRetirementFence(ready), true);
  } finally {
    rmSync(temp.dir, { recursive: true, force: true });
  }
});

test("Successor adoption changes only owner and epoch while preserving phase", () => {
  const temp = createTempPath();
  try {
    const ledger = createLedger({ path: temp.path, getNowMs: () => 333 });
    const fenced = acquireFence(ledger);
    const issued = ledger.issueDeletionPermit(fenced);
    assert.equal(issued.kind, "issued");
    const successor = {
      processId: 303,
      processBirthId: "303:start:successor",
    };
    const successorLedger = createLedger({ path: temp.path, owner: successor });
    const adopted = successorLedger.adoptRetirementFence(issued.fence, {
      owner: successor,
      leaderEpoch: "epoch-two",
    });
    assert.equal(adopted.phase, "deletion-issued");
    assert.equal(adopted.acquiredAtMs, issued.fence.acquiredAtMs);
    assert.equal(adopted.retirementRequestedAtMs, 900);
    assert.deepEqual(adopted.owner, successor);
    assert.equal(adopted.leaderEpoch, "epoch-two");
    assert.deepEqual(
      successorLedger.adoptRetirementFence(issued.fence, {
        owner: successor,
        leaderEpoch: "epoch-two",
      }),
      adopted,
    );
    assert.throws(
      () => successorLedger.adoptRetirementFence({ ...issued.fence, retirementIntentId: "other" }, {
        owner: successor,
        leaderEpoch: "epoch-three",
      }),
      (error) => isAdmissionError(error, "authority-changed"),
    );
  } finally {
    rmSync(temp.dir, { recursive: true, force: true });
  }
});

test("Ambiguous publication recovers acquisition but never reissues deletion", () => {
  const leaseTemp = createTempPath();
  const fenceTemp = createTempPath();
  try {
    let leaseRenameCount = 0;
    const ambiguousLease = createLedger({
      path: leaseTemp.path,
      getNowMs: () => 55,
      publishRename(sourcePath, destinationPath) {
        leaseRenameCount += 1;
        renameSync(sourcePath, destinationPath);
        throw new Error("lost publication acknowledgement");
      },
    });
    assert.throws(
      () => ambiguousLease.acquireAdmission({
        operationId: "ambiguous-lease",
        operationKind: "journal.append",
        scope: { kind: "target", target },
      }),
      (error) => isAdmissionError(error, "publication-unknown"),
    );
    assert.equal(leaseRenameCount, 1);
    const recoveredLease = createLedger({
      path: leaseTemp.path,
      getNowMs: () => 99,
    }).acquireAdmission({
      operationId: "ambiguous-lease",
      operationKind: "journal.append",
      scope: { kind: "target", target },
    });
    assert.equal(recoveredLease.kind, "acquired");
    assert.equal(recoveredLease.resumed, true);
    assert.equal(recoveredLease.lease.acquiredAtMs, 55);

    let fenceRenameCount = 0;
    const ambiguousFenceLedger = createLedger({
      path: fenceTemp.path,
      getNowMs: () => 77,
      publishRename(sourcePath, destinationPath) {
        fenceRenameCount += 1;
        renameSync(sourcePath, destinationPath);
        if (fenceRenameCount === 2) {
          throw new Error("lost deletion-phase acknowledgement");
        }
      },
    });
    const fenced = acquireFence(ambiguousFenceLedger);
    assert.throws(
      () => ambiguousFenceLedger.issueDeletionPermit(fenced),
      (error) => isAdmissionError(error, "publication-unknown"),
    );
    const recoveredFence = createLedger({ path: fenceTemp.path });
    const noSecondPermit = recoveredFence.issueDeletionPermit(fenced);
    assert.equal(noSecondPermit.kind, "already-issued");
    assert.equal(noSecondPermit.fence.phase, "deletion-issued");
  } finally {
    rmSync(leaseTemp.dir, { recursive: true, force: true });
    rmSync(fenceTemp.dir, { recursive: true, force: true });
  }
});

interface RaceResult {
  action: "admission" | "fence";
  kind: "acquired" | "blocked";
  reason?: string;
}

function runRaceParticipant(input: {
  path: string;
  readyPath: string;
  startPath: string;
  action: "admission" | "fence";
  operationId: string;
}): Promise<RaceResult> {
  const moduleUrl = new URL("../lib/workspace-admission.ts", import.meta.url).href;
  const busUrl = new URL("../lib/bus.ts", import.meta.url).href;
  const source = `
    import { existsSync, writeFileSync } from "node:fs";
    import { createTelegramWorkspaceAdmissionLedger } from ${JSON.stringify(moduleUrl)};
    import { getTelegramProcessBirthIdentity } from ${JSON.stringify(busUrl)};
    const sleep = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
    const owner = { processId: process.pid, processBirthId: getTelegramProcessBirthIdentity(process.pid, Date.now()) };
    const ledger = createTelegramWorkspaceAdmissionLedger({ path: process.env.LEDGER_PATH, profileKey: ${JSON.stringify(profileKey)}, owner });
    writeFileSync(process.env.READY_PATH, "ready");
    while (!existsSync(process.env.START_PATH)) sleep(2);
    const action = process.env.ACTION;
    const result = action === "admission"
      ? ledger.acquireAdmission({ operationId: process.env.OPERATION_ID, operationKind: "journal.append", scope: { kind: "target", target: ${JSON.stringify(target)} } })
      : ledger.acquireRetirementFence({ operationId: process.env.OPERATION_ID, retirementIntentId: "intent-race", bindingKey: "binding-race", slot: "A", target: ${JSON.stringify(target)}, leaderEpoch: "epoch-race", retirementRequestedAtMs: 1 });
    process.stdout.write(JSON.stringify({ action, kind: result.kind, reason: result.reason }));
    if (result.kind === "acquired") sleep(250);
  `;
  return runNodeEval(source, {
    env: {
      LEDGER_PATH: input.path,
      READY_PATH: input.readyPath,
      START_PATH: input.startPath,
      ACTION: input.action,
      OPERATION_ID: input.operationId,
    },
  }).then(({ code, stdout, stderr }) => {
    assert.equal(code, 0, stderr);
    return JSON.parse(stdout) as RaceResult;
  });
}

async function waitForFiles(paths: string[]): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    if (paths.every((path) => existsSync(path))) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail("Timed out waiting for race participants");
}

test("Cross-process admission and retirement contenders serialize without overlap", async () => {
  const temp = createTempPath();
  const readyAdmission = join(temp.dir, "ready-admission");
  const readyFence = join(temp.dir, "ready-fence");
  const startPath = join(temp.dir, "start");
  try {
    const admission = runRaceParticipant({
      path: temp.path,
      readyPath: readyAdmission,
      startPath,
      action: "admission",
      operationId: "race-admission",
    });
    const fence = runRaceParticipant({
      path: temp.path,
      readyPath: readyFence,
      startPath,
      action: "fence",
      operationId: "race-fence",
    });
    await waitForFiles([readyAdmission, readyFence]);
    writeFileSync(startPath, "start");
    const results = await Promise.all([admission, fence]);
    assert.equal(results.filter((result) => result.kind === "acquired").length, 1);
    assert.equal(results.filter((result) => result.kind === "blocked").length, 1);
    assert.match(
      results.find((result) => result.kind === "blocked")?.reason ?? "",
      /^(admission-active|retirement-fenced)$/u,
    );
  } finally {
    rmSync(temp.dir, { recursive: true, force: true });
  }
});

test("A crashed process admission is reclaimed only after process-birth death proof", async () => {
  const temp = createTempPath();
  try {
    const moduleUrl = new URL("../lib/workspace-admission.ts", import.meta.url).href;
    const busUrl = new URL("../lib/bus.ts", import.meta.url).href;
    const source = `
      import { createTelegramWorkspaceAdmissionLedger } from ${JSON.stringify(moduleUrl)};
      import { getTelegramProcessBirthIdentity } from ${JSON.stringify(busUrl)};
      const owner = { processId: process.pid, processBirthId: getTelegramProcessBirthIdentity(process.pid, Date.now()) };
      const ledger = createTelegramWorkspaceAdmissionLedger({ path: process.env.LEDGER_PATH, profileKey: ${JSON.stringify(profileKey)}, owner });
      const result = ledger.acquireAdmission({ operationId: "crashed-admission", operationKind: "api.sendMessage", scope: { kind: "target", target: ${JSON.stringify(target)} } });
      process.stdout.write(JSON.stringify(result));
    `;
    const child = await runNodeEval(source, { env: { LEDGER_PATH: temp.path } });
    assert.equal(child.code, 0, child.stderr);
    assert.equal((JSON.parse(child.stdout) as { kind: string }).kind, "acquired");

    const currentOwner = {
      processId: process.pid,
      processBirthId: getTelegramProcessBirthIdentity(process.pid, Date.now()),
    };
    const recovered = createTelegramWorkspaceAdmissionLedger({
      path: temp.path,
      profileKey,
      owner: currentOwner,
    });
    const result = recovered.acquireRetirementFence({
      operationId: "post-crash-fence",
      retirementIntentId: "post-crash-intent",
      bindingKey: "post-crash-binding",
      slot: "A",
      target,
      leaderEpoch: 1,
      retirementRequestedAtMs: 1,
    });
    assert.equal(result.kind, "acquired");
    assert.equal(recovered.read().leases.length, 0);
  } finally {
    rmSync(temp.dir, { recursive: true, force: true });
  }
});
