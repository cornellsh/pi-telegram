/**
 * Durable Telegram Workspace admission and retirement fencing
 * Zones: telegram workspace identity, filesystem authority, process recovery
 * Owns profile-scoped admission leases, destructive fences, and deletion permits
 * Excludes journal/API operations, retirement policy, and binding persistence
 */

import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
  type PathLike,
} from "node:fs";
import { dirname } from "node:path";

import {
  getTelegramProcessLiveness,
  type TelegramProcessLiveness,
} from "./bus.ts";
import {
  renameTelegramPathWithRetry,
  withTelegramFileTransaction,
} from "./locks.ts";
import type { TelegramTarget } from "./target.ts";

const TELEGRAM_WORKSPACE_ADMISSION_VERSION = 1;
const TELEGRAM_WORKSPACE_ADMISSION_MAX_LEASES = 4096;
const TELEGRAM_WORKSPACE_ADMISSION_MAX_TEXT = 512;
const activeTelegramWorkspaceAdmissionOperationIds = new Set<string>();

export interface TelegramWorkspaceAdmissionOwner {
  processId: number;
  processBirthId: string;
}

export type TelegramWorkspaceAdmissionScope =
  | { kind: "target"; target: TelegramTarget & { threadId: number } }
  | { kind: "chat"; chatId: number }
  | { kind: "profile" };

export interface TelegramWorkspaceAdmissionLease {
  operationId: string;
  operationKind: string;
  profileKey: string;
  scope: TelegramWorkspaceAdmissionScope;
  owner: TelegramWorkspaceAdmissionOwner;
  acquiredAtMs: number;
}

interface TelegramWorkspaceRetirementFenceBase {
  operationId: string;
  retirementIntentId: string;
  profileKey: string;
  bindingKey: string;
  slot: string;
  target: TelegramTarget & { threadId: number };
  leaderEpoch: number | string;
  retirementRequestedAtMs: number;
  owner: TelegramWorkspaceAdmissionOwner;
  acquiredAtMs: number;
}

export type TelegramWorkspaceRetirementFence =
  | (TelegramWorkspaceRetirementFenceBase & { phase: "fenced" })
  | (TelegramWorkspaceRetirementFenceBase & {
      phase: "deletion-issued";
      deletionIssuedAtMs: number;
    })
  | (TelegramWorkspaceRetirementFenceBase & {
      phase: "commit-ready";
      absenceConfirmedAtMs: number;
      deletionIssuedAtMs?: number;
    });

export interface TelegramWorkspaceDeletionPermit {
  readonly operationId: string;
  readonly retirementIntentId: string;
  readonly profileKey: string;
  readonly bindingKey: string;
  readonly slot: string;
  readonly target: TelegramTarget & { threadId: number };
  readonly leaderEpoch: number | string;
  readonly issuedAtMs: number;
}

interface TelegramWorkspaceAdmissionState {
  version: 1;
  profileKey: string;
  leases: TelegramWorkspaceAdmissionLease[];
  fence?: TelegramWorkspaceRetirementFence;
}

export type TelegramWorkspaceAdmissionBlockReason =
  | "retirement-fenced"
  | "admission-active"
  | "retirement-active";

export type TelegramWorkspaceAdmissionAcquireResult =
  | { kind: "acquired"; lease: TelegramWorkspaceAdmissionLease; resumed: boolean }
  | { kind: "blocked"; reason: "retirement-fenced" };

export type TelegramWorkspaceRetirementFenceAcquireResult =
  | {
      kind: "acquired";
      fence: TelegramWorkspaceRetirementFence;
      resumed: boolean;
    }
  | {
      kind: "blocked";
      reason: "admission-active" | "retirement-active";
    };

export type TelegramWorkspaceDeletionPermitResult =
  | {
      kind: "issued";
      fence: TelegramWorkspaceRetirementFence;
      permit: TelegramWorkspaceDeletionPermit;
    }
  | { kind: "already-issued"; fence: TelegramWorkspaceRetirementFence };

export interface TelegramWorkspaceAdmissionLedgerSnapshot {
  profileKey: string;
  leases: TelegramWorkspaceAdmissionLease[];
  fence?: TelegramWorkspaceRetirementFence;
}

export type TelegramWorkspaceAdmissionPublicationBoundary =
  | "before-write"
  | "after-write-before-rename";

export interface TelegramWorkspaceAdmissionLedgerOptions {
  path: string;
  profileKey: string;
  owner: TelegramWorkspaceAdmissionOwner;
  getNowMs?: () => number;
  getProcessLiveness?: (
    owner: TelegramWorkspaceAdmissionOwner,
  ) => TelegramProcessLiveness;
  publishRename?: (sourcePath: PathLike, destinationPath: PathLike) => void;
  onPublicationBoundary?: (
    boundary: TelegramWorkspaceAdmissionPublicationBoundary,
    path: string,
  ) => void;
}

export class TelegramWorkspaceAdmissionError extends Error {
  readonly code:
    | "invalid-input"
    | "invalid-state"
    | "state-unavailable"
    | "authority-changed"
    | "admission-blocked"
    | "publication-unknown";

  constructor(
    code: TelegramWorkspaceAdmissionError["code"],
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "TelegramWorkspaceAdmissionError";
    this.code = code;
  }
}

export interface TelegramWorkspaceAdmissionLedger {
  getProfileKey: () => string;
  getOwner: () => TelegramWorkspaceAdmissionOwner;
  listReservedSlots: () => string[];
  read: () => TelegramWorkspaceAdmissionLedgerSnapshot;
  acquireAdmission: (input: {
    operationId: string;
    operationKind: string;
    scope: TelegramWorkspaceAdmissionScope;
  }) => TelegramWorkspaceAdmissionAcquireResult;
  releaseAdmission: (expected: TelegramWorkspaceAdmissionLease) => boolean;
  acquireRetirementFence: (input: {
    operationId: string;
    retirementIntentId: string;
    bindingKey: string;
    slot: string;
    target: TelegramTarget & { threadId: number };
    leaderEpoch: number | string;
    retirementRequestedAtMs: number;
  }) => TelegramWorkspaceRetirementFenceAcquireResult;
  adoptRetirementFence: (
    expected: TelegramWorkspaceRetirementFence,
    replacement: {
      owner: TelegramWorkspaceAdmissionOwner;
      leaderEpoch: number | string;
    },
  ) => TelegramWorkspaceRetirementFence;
  issueDeletionPermit: (
    expected: TelegramWorkspaceRetirementFence,
  ) => TelegramWorkspaceDeletionPermitResult;
  confirmRetirementAbsence: (
    expected: TelegramWorkspaceRetirementFence,
  ) => TelegramWorkspaceRetirementFence;
  releaseUnissuedRetirementFence: (
    expected: TelegramWorkspaceRetirementFence,
  ) => boolean;
  completeRetirementFence: (
    expected: TelegramWorkspaceRetirementFence,
  ) => boolean;
}

function invalidInput(message: string): never {
  throw new TelegramWorkspaceAdmissionError("invalid-input", message);
}

function invalidState(message: string, cause?: unknown): never {
  throw new TelegramWorkspaceAdmissionError("invalid-state", message, {
    cause,
  });
}

function authorityChanged(message: string): never {
  throw new TelegramWorkspaceAdmissionError("authority-changed", message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isSafeTimestamp(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function isBoundedText(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= TELEGRAM_WORKSPACE_ADMISSION_MAX_TEXT
  );
}

function isLeaderEpoch(value: unknown): value is number | string {
  return (
    (typeof value === "number" && Number.isSafeInteger(value)) ||
    isBoundedText(value)
  );
}

function normalizeTarget(
  value: unknown,
): (TelegramTarget & { threadId: number }) | undefined {
  if (!isRecord(value)) return undefined;
  if (
    !Number.isSafeInteger(value.chatId) ||
    value.chatId === 0 ||
    !Number.isSafeInteger(value.threadId) ||
    (value.threadId as number) <= 0
  ) {
    return undefined;
  }
  return { chatId: value.chatId as number, threadId: value.threadId as number };
}

function normalizeOwner(
  value: unknown,
): TelegramWorkspaceAdmissionOwner | undefined {
  if (!isRecord(value)) return undefined;
  if (
    !Number.isSafeInteger(value.processId) ||
    (value.processId as number) <= 0 ||
    !isBoundedText(value.processBirthId) ||
    !value.processBirthId.startsWith(`${value.processId}:`)
  ) {
    return undefined;
  }
  return {
    processId: value.processId as number,
    processBirthId: value.processBirthId,
  };
}

function normalizeScope(
  value: unknown,
): TelegramWorkspaceAdmissionScope | undefined {
  if (!isRecord(value)) return undefined;
  if (value.kind === "profile") return { kind: "profile" };
  if (value.kind === "chat") {
    if (!Number.isSafeInteger(value.chatId) || value.chatId === 0) return undefined;
    return { kind: "chat", chatId: value.chatId as number };
  }
  if (value.kind === "target") {
    const target = normalizeTarget(value.target);
    return target ? { kind: "target", target } : undefined;
  }
  return undefined;
}

function normalizeLease(
  value: unknown,
  profileKey: string,
): TelegramWorkspaceAdmissionLease | undefined {
  if (!isRecord(value)) return undefined;
  const scope = normalizeScope(value.scope);
  const owner = normalizeOwner(value.owner);
  if (
    !isBoundedText(value.operationId) ||
    !isBoundedText(value.operationKind) ||
    value.profileKey !== profileKey ||
    !scope ||
    !owner ||
    !isSafeTimestamp(value.acquiredAtMs)
  ) {
    return undefined;
  }
  return {
    operationId: value.operationId,
    operationKind: value.operationKind,
    profileKey,
    scope,
    owner,
    acquiredAtMs: value.acquiredAtMs,
  };
}

function normalizeFence(
  value: unknown,
  profileKey: string,
): TelegramWorkspaceRetirementFence | undefined {
  if (!isRecord(value)) return undefined;
  const target = normalizeTarget(value.target);
  const owner = normalizeOwner(value.owner);
  if (
    !isBoundedText(value.operationId) ||
    !isBoundedText(value.retirementIntentId) ||
    value.profileKey !== profileKey ||
    !isBoundedText(value.bindingKey) ||
    typeof value.slot !== "string" ||
    !/^[A-Z]$/u.test(value.slot) ||
    !target ||
    !isLeaderEpoch(value.leaderEpoch) ||
    !isSafeTimestamp(value.retirementRequestedAtMs) ||
    !owner ||
    !isSafeTimestamp(value.acquiredAtMs)
  ) {
    return undefined;
  }
  const base: TelegramWorkspaceRetirementFenceBase = {
    operationId: value.operationId,
    retirementIntentId: value.retirementIntentId,
    profileKey,
    bindingKey: value.bindingKey,
    slot: value.slot,
    target,
    leaderEpoch: value.leaderEpoch,
    retirementRequestedAtMs: value.retirementRequestedAtMs,
    owner,
    acquiredAtMs: value.acquiredAtMs,
  };
  if (value.phase === "fenced") return { ...base, phase: "fenced" };
  if (
    value.phase === "deletion-issued" &&
    isSafeTimestamp(value.deletionIssuedAtMs)
  ) {
    return {
      ...base,
      phase: "deletion-issued",
      deletionIssuedAtMs: value.deletionIssuedAtMs,
    };
  }
  if (
    value.phase === "commit-ready" &&
    isSafeTimestamp(value.absenceConfirmedAtMs) &&
    (value.deletionIssuedAtMs === undefined ||
      isSafeTimestamp(value.deletionIssuedAtMs))
  ) {
    return {
      ...base,
      phase: "commit-ready",
      absenceConfirmedAtMs: value.absenceConfirmedAtMs,
      ...(value.deletionIssuedAtMs === undefined
        ? {}
        : { deletionIssuedAtMs: value.deletionIssuedAtMs }),
    };
  }
  return undefined;
}

function cloneScope(
  scope: TelegramWorkspaceAdmissionScope,
): TelegramWorkspaceAdmissionScope {
  return scope.kind === "target"
    ? { kind: "target", target: { ...scope.target } }
    : { ...scope };
}

function cloneLease(
  lease: TelegramWorkspaceAdmissionLease,
): TelegramWorkspaceAdmissionLease {
  return { ...lease, scope: cloneScope(lease.scope), owner: { ...lease.owner } };
}

function cloneFence(
  fence: TelegramWorkspaceRetirementFence,
): TelegramWorkspaceRetirementFence {
  return { ...fence, target: { ...fence.target }, owner: { ...fence.owner } };
}

function readState(
  path: string,
  profileKey: string,
): TelegramWorkspaceAdmissionState {
  let raw: string;
  try {
    raw = readFileSync(path, "utf8");
  } catch (error) {
    if ((error as { code?: unknown })?.code === "ENOENT") {
      return {
        version: TELEGRAM_WORKSPACE_ADMISSION_VERSION,
        profileKey,
        leases: [],
      };
    }
    throw new TelegramWorkspaceAdmissionError(
      "state-unavailable",
      "Telegram Workspace admission state is unavailable.",
      { cause: error },
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    invalidState("Telegram Workspace admission state is malformed.", error);
  }
  if (!isRecord(parsed)) {
    invalidState("Telegram Workspace admission state is invalid.");
  }
  if (
    parsed.version !== TELEGRAM_WORKSPACE_ADMISSION_VERSION ||
    !Array.isArray(parsed.leases) ||
    parsed.leases.length > TELEGRAM_WORKSPACE_ADMISSION_MAX_LEASES
  ) {
    invalidState("Telegram Workspace admission state authority is invalid.");
  }
  if (parsed.profileKey !== profileKey) {
    if (parsed.leases.length === 0 && parsed.fence === undefined) {
      return {
        version: TELEGRAM_WORKSPACE_ADMISSION_VERSION,
        profileKey,
        leases: [],
      };
    }
    invalidState("Telegram Workspace admission profile authority changed.");
  }
  const leases = parsed.leases.map((value) => normalizeLease(value, profileKey));
  if (leases.some((lease) => lease === undefined)) {
    invalidState("Telegram Workspace admission lease is invalid.");
  }
  const normalizedLeases = leases as TelegramWorkspaceAdmissionLease[];
  if (new Set(normalizedLeases.map((lease) => lease.operationId)).size !== normalizedLeases.length) {
    invalidState("Telegram Workspace admission lease identity is duplicated.");
  }
  const fence =
    parsed.fence === undefined ? undefined : normalizeFence(parsed.fence, profileKey);
  if (parsed.fence !== undefined && !fence) {
    invalidState("Telegram Workspace retirement fence is invalid.");
  }
  return {
    version: TELEGRAM_WORKSPACE_ADMISSION_VERSION,
    profileKey,
    leases: normalizedLeases,
    ...(fence ? { fence } : {}),
  };
}

function writeState(
  path: string,
  state: TelegramWorkspaceAdmissionState,
  options: Pick<
    TelegramWorkspaceAdmissionLedgerOptions,
    "publishRename" | "onPublicationBoundary"
  >,
): void {
  const tempPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  try {
    options.onPublicationBoundary?.("before-write", path);
    writeFileSync(tempPath, `${JSON.stringify(state, null, 2)}\n`, {
      encoding: "utf8",
      mode: 0o600,
    });
    chmodSync(tempPath, 0o600);
    options.onPublicationBoundary?.("after-write-before-rename", path);
    try {
      if (
        !renameTelegramPathWithRetry(tempPath, path, {
          ...(options.publishRename ? { rename: options.publishRename } : {}),
        })
      ) {
        throw new Error("Temporary admission state disappeared before publication.");
      }
      chmodSync(path, 0o600);
    } catch (error) {
      throw new TelegramWorkspaceAdmissionError(
        "publication-unknown",
        "Telegram Workspace admission publication outcome is unknown.",
        { cause: error },
      );
    }
  } finally {
    try {
      unlinkSync(tempPath);
    } catch {
      // A successful atomic rename consumes the temporary path.
    }
  }
}

function areTargetsEqual(
  left: TelegramTarget & { threadId: number },
  right: TelegramTarget & { threadId: number },
): boolean {
  return left.chatId === right.chatId && left.threadId === right.threadId;
}

function scopesConflict(
  scope: TelegramWorkspaceAdmissionScope,
  target: TelegramTarget & { threadId: number },
): boolean {
  if (scope.kind === "profile") return true;
  if (scope.kind === "chat") return scope.chatId === target.chatId;
  return areTargetsEqual(scope.target, target);
}

function areOwnersEqual(
  left: TelegramWorkspaceAdmissionOwner,
  right: TelegramWorkspaceAdmissionOwner,
): boolean {
  return (
    left.processId === right.processId &&
    left.processBirthId === right.processBirthId
  );
}

function areScopesEqual(
  left: TelegramWorkspaceAdmissionScope,
  right: TelegramWorkspaceAdmissionScope,
): boolean {
  if (left.kind !== right.kind) return false;
  if (left.kind === "profile" && right.kind === "profile") return true;
  if (left.kind === "chat" && right.kind === "chat") {
    return left.chatId === right.chatId;
  }
  return (
    left.kind === "target" &&
    right.kind === "target" &&
    areTargetsEqual(left.target, right.target)
  );
}

function areLeasesEqual(
  left: TelegramWorkspaceAdmissionLease,
  right: TelegramWorkspaceAdmissionLease,
): boolean {
  return (
    left.operationId === right.operationId &&
    left.operationKind === right.operationKind &&
    left.profileKey === right.profileKey &&
    areScopesEqual(left.scope, right.scope) &&
    areOwnersEqual(left.owner, right.owner) &&
    left.acquiredAtMs === right.acquiredAtMs
  );
}

function areFenceBasesEqual(
  left: TelegramWorkspaceRetirementFence,
  right: TelegramWorkspaceRetirementFence,
): boolean {
  return (
    left.operationId === right.operationId &&
    left.retirementIntentId === right.retirementIntentId &&
    left.profileKey === right.profileKey &&
    left.bindingKey === right.bindingKey &&
    left.slot === right.slot &&
    areTargetsEqual(left.target, right.target) &&
    left.leaderEpoch === right.leaderEpoch &&
    left.retirementRequestedAtMs === right.retirementRequestedAtMs &&
    areOwnersEqual(left.owner, right.owner) &&
    left.acquiredAtMs === right.acquiredAtMs
  );
}

function areFencesEqual(
  left: TelegramWorkspaceRetirementFence,
  right: TelegramWorkspaceRetirementFence,
): boolean {
  if (!areFenceBasesEqual(left, right) || left.phase !== right.phase) return false;
  if (left.phase === "fenced" && right.phase === "fenced") return true;
  if (
    left.phase === "deletion-issued" &&
    right.phase === "deletion-issued"
  ) {
    return left.deletionIssuedAtMs === right.deletionIssuedAtMs;
  }
  return (
    left.phase === "commit-ready" &&
    right.phase === "commit-ready" &&
    left.absenceConfirmedAtMs === right.absenceConfirmedAtMs &&
    left.deletionIssuedAtMs === right.deletionIssuedAtMs
  );
}

function validateOptions(options: TelegramWorkspaceAdmissionLedgerOptions): void {
  if (!isBoundedText(options.path)) {
    invalidInput("Telegram Workspace admission path is required.");
  }
  if (!isBoundedText(options.profileKey)) {
    invalidInput("Telegram Workspace admission profile authority is required.");
  }
  if (!normalizeOwner(options.owner)) {
    invalidInput("Telegram Workspace admission owner is invalid.");
  }
}

function validateOperationId(operationId: string): void {
  if (!isBoundedText(operationId)) {
    invalidInput("Telegram Workspace admission operation identity is invalid.");
  }
}

function validateScope(scope: TelegramWorkspaceAdmissionScope): void {
  if (!normalizeScope(scope)) {
    invalidInput("Telegram Workspace admission scope is invalid.");
  }
}

function validateExpectedLease(
  lease: TelegramWorkspaceAdmissionLease,
  profileKey: string,
): void {
  if (!normalizeLease(lease, profileKey)) {
    invalidInput("Telegram Workspace admission lease authority is invalid.");
  }
}

function validateExpectedFence(
  fence: TelegramWorkspaceRetirementFence,
  profileKey: string,
): void {
  if (!normalizeFence(fence, profileKey)) {
    invalidInput("Telegram Workspace retirement fence authority is invalid.");
  }
}

export function createTelegramWorkspaceAdmissionOperationId(): string {
  return randomUUID();
}

export function createTelegramWorkspaceAdmissionProfileKey(input: {
  profileName?: string;
  botToken: string;
}): string {
  const profileName = (input.profileName ?? "default").trim();
  if (!isBoundedText(profileName) || !isBoundedText(input.botToken)) {
    invalidInput("Telegram Workspace admission bot/profile identity is invalid.");
  }
  return JSON.stringify({
    version: TELEGRAM_WORKSPACE_ADMISSION_VERSION,
    profile: profileName,
    bot: {
      tokenSha256: createHash("sha256").update(input.botToken).digest("hex"),
    },
  });
}

export interface TelegramWorkspaceAdmissionRuntimeBinding {
  resolve: () => TelegramWorkspaceAdmissionLedger | undefined;
}

export function createTelegramWorkspaceAdmissionRuntimeBinding(input: {
  getProfileName: () => string | undefined;
  getBotToken: () => string | undefined;
  getPath: (profileName?: string) => string;
  owner: TelegramWorkspaceAdmissionOwner;
  getNowMs?: () => number;
  getProcessLiveness?: (
    owner: TelegramWorkspaceAdmissionOwner,
  ) => TelegramProcessLiveness;
}): TelegramWorkspaceAdmissionRuntimeBinding {
  return {
    resolve() {
      const botToken = input.getBotToken();
      if (!botToken) return undefined;
      const configuredProfileName = input.getProfileName();
      const profileKey = createTelegramWorkspaceAdmissionProfileKey({
        profileName: configuredProfileName ?? "default",
        botToken,
      });
      return createTelegramWorkspaceAdmissionLedger({
        path: input.getPath(configuredProfileName),
        profileKey,
        owner: input.owner,
        getNowMs: input.getNowMs,
        getProcessLiveness: input.getProcessLiveness,
      });
    },
  };
}

function getAdmissionScopeKey(scope: TelegramWorkspaceAdmissionScope): string {
  if (scope.kind === "profile") return "profile";
  if (scope.kind === "chat") return `chat:${scope.chatId}`;
  return `target:${scope.target.chatId}:${scope.target.threadId}`;
}

function reserveTelegramWorkspaceAdmissionOperationId(
  operationId: string,
): () => void {
  if (activeTelegramWorkspaceAdmissionOperationIds.has(operationId)) {
    invalidState("Telegram Workspace admission operation is already active.");
  }
  activeTelegramWorkspaceAdmissionOperationIds.add(operationId);
  return () => {
    activeTelegramWorkspaceAdmissionOperationIds.delete(operationId);
  };
}

export function runWithTelegramWorkspaceAdmissions<T>(input: {
  ledger: Pick<
    TelegramWorkspaceAdmissionLedger,
    "acquireAdmission" | "releaseAdmission"
  >;
  operationId: string;
  operationKind: string;
  scopes: readonly TelegramWorkspaceAdmissionScope[];
  operation: () => T;
}): T {
  validateOperationId(input.operationId);
  if (!isBoundedText(input.operationKind) || input.scopes.length === 0) {
    invalidInput("Telegram Workspace admitted operation is invalid.");
  }
  const uniqueScopes = new Map<string, TelegramWorkspaceAdmissionScope>();
  for (const scope of input.scopes) {
    validateScope(scope);
    uniqueScopes.set(getAdmissionScopeKey(scope), cloneScope(scope));
  }
  const scopes = Array.from(uniqueScopes.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([, scope]) => scope);
  const releaseActiveOperation =
    reserveTelegramWorkspaceAdmissionOperationId(input.operationId);
  const acquired: TelegramWorkspaceAdmissionLease[] = [];
  let outcome: { ok: true; value: T } | { ok: false; error: unknown };
  try {
    for (const scope of scopes) {
      const scopeOperationId = `admission:${createHash("sha256")
        .update(input.operationId)
        .update("\0")
        .update(getAdmissionScopeKey(scope))
        .digest("hex")}`;
      const result = input.ledger.acquireAdmission({
        operationId: scopeOperationId,
        operationKind: input.operationKind,
        scope,
      });
      if (result.kind === "blocked") {
        throw new TelegramWorkspaceAdmissionError(
          "admission-blocked",
          "Telegram Workspace operation is blocked by retirement.",
        );
      }
      acquired.push(result.lease);
    }
    outcome = { ok: true, value: input.operation() };
  } catch (error) {
    outcome = { ok: false, error };
  }
  const releaseErrors: unknown[] = [];
  for (const lease of acquired.reverse()) {
    try {
      input.ledger.releaseAdmission(lease);
    } catch (error) {
      releaseErrors.push(error);
    }
  }
  releaseActiveOperation();
  if (releaseErrors.length > 0) {
    throw new AggregateError(
      outcome.ok ? releaseErrors : [outcome.error, ...releaseErrors],
      "Telegram Workspace admission release failed.",
    );
  }
  if (!outcome.ok) throw outcome.error;
  return outcome.value;
}

export async function runWithTelegramWorkspaceAdmissionsAsync<T>(input: {
  ledger: Pick<
    TelegramWorkspaceAdmissionLedger,
    "acquireAdmission" | "releaseAdmission"
  >;
  operationId: string;
  operationKind: string;
  scopes: readonly TelegramWorkspaceAdmissionScope[];
  operation: () => Promise<T>;
  onReleaseError?: (error: unknown) => void;
}): Promise<T> {
  validateOperationId(input.operationId);
  if (!isBoundedText(input.operationKind) || input.scopes.length === 0) {
    invalidInput("Telegram Workspace admitted operation is invalid.");
  }
  const uniqueScopes = new Map<string, TelegramWorkspaceAdmissionScope>();
  for (const scope of input.scopes) {
    validateScope(scope);
    uniqueScopes.set(getAdmissionScopeKey(scope), cloneScope(scope));
  }
  const scopes = Array.from(uniqueScopes.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([, scope]) => scope);
  const releaseActiveOperation =
    reserveTelegramWorkspaceAdmissionOperationId(input.operationId);
  const acquired: TelegramWorkspaceAdmissionLease[] = [];
  const releaseAll = (): void => {
    for (const lease of acquired.reverse()) {
      try {
        if (!input.ledger.releaseAdmission(lease)) {
          throw new TelegramWorkspaceAdmissionError(
            "authority-changed",
            "Telegram Workspace admission lease disappeared before release.",
          );
        }
      } catch (error) {
        try {
          input.onReleaseError?.(error);
        } catch {
          // Diagnostics cannot change the settled admitted operation.
        }
      }
    }
  };
  try {
    for (const scope of scopes) {
      const scopeOperationId = `admission:${createHash("sha256")
        .update(input.operationId)
        .update("\0")
        .update(getAdmissionScopeKey(scope))
        .digest("hex")}`;
      const result = input.ledger.acquireAdmission({
        operationId: scopeOperationId,
        operationKind: input.operationKind,
        scope,
      });
      if (result.kind === "blocked") {
        throw new TelegramWorkspaceAdmissionError(
          "admission-blocked",
          "Telegram Workspace operation is blocked by retirement.",
        );
      }
      acquired.push(result.lease);
    }
  } catch (error) {
    releaseAll();
    releaseActiveOperation();
    throw error;
  }
  try {
    return await input.operation();
  } finally {
    releaseAll();
    releaseActiveOperation();
  }
}

export function createTelegramWorkspaceAdmissionLedger(
  options: TelegramWorkspaceAdmissionLedgerOptions,
): TelegramWorkspaceAdmissionLedger {
  validateOptions(options);
  const getNowMs = options.getNowMs ?? Date.now;
  const getProcessLiveness =
    options.getProcessLiveness ??
    ((owner: TelegramWorkspaceAdmissionOwner) =>
      getTelegramProcessLiveness(owner));
  const transactionPath = `${options.path}.transaction`;

  function transact<T>(
    operation: (state: TelegramWorkspaceAdmissionState) => {
      result: T;
      changed: boolean;
    },
  ): T {
    return withTelegramFileTransaction(transactionPath, () => {
      const state = readState(options.path, options.profileKey);
      const retainedLeases = state.leases.filter(
        (lease) => getProcessLiveness(lease.owner) !== "dead",
      );
      const pruned = retainedLeases.length !== state.leases.length;
      if (pruned) state.leases = retainedLeases;
      const outcome = operation(state);
      if (pruned || outcome.changed) writeState(options.path, state, options);
      return outcome.result;
    });
  }

  function read(): TelegramWorkspaceAdmissionLedgerSnapshot {
    const state = readState(options.path, options.profileKey);
    return {
      profileKey: state.profileKey,
      leases: state.leases.map(cloneLease),
      ...(state.fence ? { fence: cloneFence(state.fence) } : {}),
    };
  }

  function acquireAdmission(input: {
    operationId: string;
    operationKind: string;
    scope: TelegramWorkspaceAdmissionScope;
  }): TelegramWorkspaceAdmissionAcquireResult {
    validateOperationId(input.operationId);
    if (!isBoundedText(input.operationKind)) {
      invalidInput("Telegram Workspace admission operation kind is invalid.");
    }
    validateScope(input.scope);
    return transact<TelegramWorkspaceAdmissionAcquireResult>((state) => {
      const existing = state.leases.find(
        (lease) => lease.operationId === input.operationId,
      );
      if (existing) {
        const retried: TelegramWorkspaceAdmissionLease = {
          operationId: input.operationId,
          operationKind: input.operationKind,
          profileKey: options.profileKey,
          scope: cloneScope(input.scope),
          owner: { ...options.owner },
          acquiredAtMs: existing.acquiredAtMs,
        };
        if (!areLeasesEqual(existing, retried)) {
          authorityChanged("Telegram Workspace admission operation identity changed.");
        }
        return {
          result: { kind: "acquired", lease: cloneLease(existing), resumed: true },
          changed: false,
        };
      }
      if (state.fence && scopesConflict(input.scope, state.fence.target)) {
        return {
          result: { kind: "blocked", reason: "retirement-fenced" },
          changed: false,
        };
      }
      if (state.leases.length >= TELEGRAM_WORKSPACE_ADMISSION_MAX_LEASES) {
        invalidState("Telegram Workspace admission lease capacity is exhausted.");
      }
      const lease: TelegramWorkspaceAdmissionLease = {
        operationId: input.operationId,
        operationKind: input.operationKind,
        profileKey: options.profileKey,
        scope: cloneScope(input.scope),
        owner: { ...options.owner },
        acquiredAtMs: getNowMs(),
      };
      state.leases.push(lease);
      return {
        result: { kind: "acquired", lease: cloneLease(lease), resumed: false },
        changed: true,
      };
    });
  }

  function releaseAdmission(expected: TelegramWorkspaceAdmissionLease): boolean {
    validateExpectedLease(expected, options.profileKey);
    if (!areOwnersEqual(expected.owner, options.owner)) {
      authorityChanged("Telegram Workspace admission release requires lease owner authority.");
    }
    return transact((state) => {
      const index = state.leases.findIndex(
        (lease) => lease.operationId === expected.operationId,
      );
      if (index < 0) return { result: false, changed: false };
      const current = state.leases[index];
      if (!areLeasesEqual(current, expected)) {
        authorityChanged("Telegram Workspace admission lease authority changed.");
      }
      state.leases.splice(index, 1);
      return { result: true, changed: true };
    });
  }

  function acquireRetirementFence(input: {
    operationId: string;
    retirementIntentId: string;
    bindingKey: string;
    slot: string;
    target: TelegramTarget & { threadId: number };
    leaderEpoch: number | string;
    retirementRequestedAtMs: number;
  }): TelegramWorkspaceRetirementFenceAcquireResult {
    validateOperationId(input.operationId);
    const candidate = normalizeFence(
      {
        ...input,
        profileKey: options.profileKey,
        owner: options.owner,
        acquiredAtMs: getNowMs(),
        phase: "fenced",
      },
      options.profileKey,
    );
    if (!candidate) {
      invalidInput("Telegram Workspace retirement fence input is invalid.");
    }
    return transact<TelegramWorkspaceRetirementFenceAcquireResult>((state) => {
      if (state.fence) {
        if (
          state.fence.phase === "fenced" &&
          areFenceBasesEqual(state.fence, {
            ...candidate,
            acquiredAtMs: state.fence.acquiredAtMs,
          })
        ) {
          return {
            result: {
              kind: "acquired",
              fence: cloneFence(state.fence),
              resumed: true,
            },
            changed: false,
          };
        }
        if (state.fence.operationId === candidate.operationId) {
          authorityChanged("Telegram Workspace retirement fence authority changed.");
        }
        return {
          result: { kind: "blocked", reason: "retirement-active" },
          changed: false,
        };
      }
      if (state.leases.some((lease) => scopesConflict(lease.scope, candidate.target))) {
        return {
          result: { kind: "blocked", reason: "admission-active" },
          changed: false,
        };
      }
      state.fence = candidate;
      return {
        result: { kind: "acquired", fence: cloneFence(candidate), resumed: false },
        changed: true,
      };
    });
  }

  function adoptRetirementFence(
    expected: TelegramWorkspaceRetirementFence,
    replacement: {
      owner: TelegramWorkspaceAdmissionOwner;
      leaderEpoch: number | string;
    },
  ): TelegramWorkspaceRetirementFence {
    validateExpectedFence(expected, options.profileKey);
    if (!normalizeOwner(replacement.owner) || !isLeaderEpoch(replacement.leaderEpoch)) {
      invalidInput("Telegram Workspace retirement successor authority is invalid.");
    }
    if (!areOwnersEqual(replacement.owner, options.owner)) {
      authorityChanged("Telegram Workspace retirement adoption requires successor owner authority.");
    }
    const adopted = {
      ...cloneFence(expected),
      owner: { ...replacement.owner },
      leaderEpoch: replacement.leaderEpoch,
    } satisfies TelegramWorkspaceRetirementFence;
    return transact((state) => {
      if (!state.fence) {
        authorityChanged("Telegram Workspace retirement fence is absent.");
      }
      if (areFencesEqual(state.fence, adopted)) {
        return { result: cloneFence(state.fence), changed: false };
      }
      if (!areFencesEqual(state.fence, expected)) {
        authorityChanged("Telegram Workspace retirement fence authority changed.");
      }
      state.fence = adopted;
      return { result: cloneFence(adopted), changed: true };
    });
  }

  function issueDeletionPermit(
    expected: TelegramWorkspaceRetirementFence,
  ): TelegramWorkspaceDeletionPermitResult {
    validateExpectedFence(expected, options.profileKey);
    if (!areOwnersEqual(expected.owner, options.owner)) {
      authorityChanged("Telegram Workspace deletion permit requires fence owner authority.");
    }
    return transact<TelegramWorkspaceDeletionPermitResult>((state) => {
      if (!state.fence) {
        authorityChanged("Telegram Workspace retirement fence is absent.");
      }
      if (
        state.fence.operationId === expected.operationId &&
        areFenceBasesEqual(state.fence, expected) &&
        state.fence.phase !== "fenced"
      ) {
        return {
          result: { kind: "already-issued", fence: cloneFence(state.fence) },
          changed: false,
        };
      }
      if (!areFencesEqual(state.fence, expected)) {
        authorityChanged("Telegram Workspace retirement fence authority changed.");
      }
      if (state.fence.phase !== "fenced") {
        return {
          result: { kind: "already-issued", fence: cloneFence(state.fence) },
          changed: false,
        };
      }
      const deletionIssuedAtMs = getNowMs();
      const issued: TelegramWorkspaceRetirementFence = {
        ...state.fence,
        phase: "deletion-issued",
        deletionIssuedAtMs,
      };
      state.fence = issued;
      return {
        result: {
          kind: "issued",
          fence: cloneFence(issued),
          permit: {
            operationId: issued.operationId,
            retirementIntentId: issued.retirementIntentId,
            profileKey: issued.profileKey,
            bindingKey: issued.bindingKey,
            slot: issued.slot,
            target: { ...issued.target },
            leaderEpoch: issued.leaderEpoch,
            issuedAtMs: deletionIssuedAtMs,
          },
        },
        changed: true,
      };
    });
  }

  function confirmRetirementAbsence(
    expected: TelegramWorkspaceRetirementFence,
  ): TelegramWorkspaceRetirementFence {
    validateExpectedFence(expected, options.profileKey);
    if (!areOwnersEqual(expected.owner, options.owner)) {
      authorityChanged("Telegram Workspace absence confirmation requires fence owner authority.");
    }
    return transact((state) => {
      if (!state.fence) {
        authorityChanged("Telegram Workspace retirement fence is absent.");
      }
      if (
        state.fence.operationId === expected.operationId &&
        areFenceBasesEqual(state.fence, expected) &&
        state.fence.phase === "commit-ready"
      ) {
        return { result: cloneFence(state.fence), changed: false };
      }
      if (!areFencesEqual(state.fence, expected)) {
        authorityChanged("Telegram Workspace retirement fence authority changed.");
      }
      const ready: TelegramWorkspaceRetirementFence = {
        ...state.fence,
        phase: "commit-ready",
        absenceConfirmedAtMs: getNowMs(),
        ...(state.fence.phase === "deletion-issued"
          ? { deletionIssuedAtMs: state.fence.deletionIssuedAtMs }
          : {}),
      };
      state.fence = ready;
      return { result: cloneFence(ready), changed: true };
    });
  }

  function releaseUnissuedRetirementFence(
    expected: TelegramWorkspaceRetirementFence,
  ): boolean {
    validateExpectedFence(expected, options.profileKey);
    if (!areOwnersEqual(expected.owner, options.owner)) {
      authorityChanged("Telegram Workspace fence release requires owner authority.");
    }
    return transact((state) => {
      if (!state.fence) return { result: false, changed: false };
      if (!areFencesEqual(state.fence, expected)) {
        authorityChanged("Telegram Workspace retirement fence authority changed.");
      }
      if (state.fence.phase !== "fenced") {
        authorityChanged("Issued Telegram Workspace retirement fence cannot be released.");
      }
      delete state.fence;
      return { result: true, changed: true };
    });
  }

  function completeRetirementFence(
    expected: TelegramWorkspaceRetirementFence,
  ): boolean {
    validateExpectedFence(expected, options.profileKey);
    if (!areOwnersEqual(expected.owner, options.owner)) {
      authorityChanged("Telegram Workspace fence completion requires owner authority.");
    }
    return transact((state) => {
      if (!state.fence) return { result: false, changed: false };
      if (!areFencesEqual(state.fence, expected)) {
        authorityChanged("Telegram Workspace retirement fence authority changed.");
      }
      if (state.fence.phase !== "commit-ready") {
        authorityChanged("Telegram Workspace retirement commit is not ready.");
      }
      delete state.fence;
      return { result: true, changed: true };
    });
  }

  return {
    getProfileKey: () => options.profileKey,
    getOwner: () => ({ ...options.owner }),
    listReservedSlots: () => {
      const fence = read().fence;
      return fence ? [fence.slot] : [];
    },
    read,
    acquireAdmission,
    releaseAdmission,
    acquireRetirementFence,
    adoptRetirementFence,
    issueDeletionPermit,
    confirmRetirementAbsence,
    releaseUnissuedRetirementFence,
    completeRetirementFence,
  };
}
