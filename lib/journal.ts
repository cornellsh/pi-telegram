/**
 * Telegram durable inbound update journal
 * Zones: telegram inbound, filesystem authority, crash recovery
 * Owns profile/bot-scoped raw updates, schema validation, deduplication,
 * bounded atomic publication, durable queue-receipt/failure state, and compaction.
 * It does not own polling, update execution, queue admission, or follower routing.
 */

import { createHash, randomBytes, randomUUID } from "node:crypto";
import {
  chmodSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmdirSync,
  statSync,
  unlinkSync,
  writeFileSync,
  type Dirent,
} from "node:fs";
import { basename, dirname, join } from "node:path";
import { isDeepStrictEqual } from "node:util";

import {
  renameTelegramPathWithRetry,
  withTelegramFileTransaction,
} from "./locks.ts";
import {
  getTelegramProcessLiveness,
  type TelegramProcessLiveness,
} from "./bus.ts";
import {
  getTelegramProfilePathSuffix,
  TELEGRAM_DEFAULT_PROFILE_NAME,
} from "./paths.ts";
import {
  runWithTelegramWorkspaceAdmissions,
  type TelegramWorkspaceAdmissionLedger,
  type TelegramWorkspaceAdmissionScope,
} from "./workspace-admission.ts";

export const TELEGRAM_UPDATE_JOURNAL_VERSION = 1 as const;
export const TELEGRAM_UPDATE_JOURNAL_MAX_ENTRIES = 10_000;
export const TELEGRAM_UPDATE_JOURNAL_MAX_BYTES = 32 * 1024 * 1024;
export const TELEGRAM_UPDATE_JOURNAL_FAILURE_ID_MAX_LENGTH = 128;
export const TELEGRAM_UPDATE_JOURNAL_QUEUE_OWNER_ID_MAX_LENGTH = 256;
export const TELEGRAM_UPDATE_JOURNAL_QUEUE_HANDOFF_ID_MAX_LENGTH = 128;
export const TELEGRAM_UPDATE_JOURNAL_QUEUE_HANDOFF_TOKEN_MIN_LENGTH = 32;
export const TELEGRAM_UPDATE_JOURNAL_QUEUE_HANDOFF_TOKEN_MAX_LENGTH = 256;
export const TELEGRAM_UPDATE_JOURNAL_FAILURE_CLASS_MAX_LENGTH = 128;
export const TELEGRAM_UPDATE_JOURNAL_FAILURE_SUMMARY_MAX_LENGTH = 512;

export interface TelegramFollowerJournalDiscovery {
  paths: string[];
  complete: boolean;
}

function escapeTelegramJournalPathPattern(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

/** Read-only discovery for canonical follower journal snapshots and segment roots. */
export function discoverTelegramFollowerJournalPaths(input: {
  directory: string;
  profileName?: string;
}): TelegramFollowerJournalDiscovery {
  const suffix = escapeTelegramJournalPathPattern(
    getTelegramProfilePathSuffix(input.profileName),
  );
  const pattern = new RegExp(
    `^(follower-inbox-[a-f0-9]{16}${suffix}\\.json)(?:\\.segments)?$`,
    "u",
  );
  let entries: Dirent[];
  try {
    entries = readdirSync(input.directory, { withFileTypes: true });
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT"
      ? { paths: [], complete: true }
      : { paths: [], complete: false };
  }
  const paths = new Set<string>();
  let complete = true;
  for (const entry of entries) {
    const match = pattern.exec(entry.name);
    if (!match) continue;
    const candidatePath = join(input.directory, entry.name);
    const expectsDirectory = entry.name.endsWith(".segments");
    try {
      const stat = statSync(candidatePath);
      if (expectsDirectory ? !stat.isDirectory() : !stat.isFile()) {
        complete = false;
        continue;
      }
      paths.add(join(input.directory, match[1]!));
    } catch {
      complete = false;
    }
  }
  return { paths: Array.from(paths).sort(), complete };
}
export const TELEGRAM_UPDATE_JOURNAL_TERMINAL_REASON_MAX_LENGTH = 256;
export const TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT = 256;
export const TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_BYTES = 4 * 1024 * 1024;

export type TelegramUpdateJournalErrorCode =
  | "capacity"
  | "conflict"
  | "identity-mismatch"
  | "invalid"
  | "io"
  | "unsupported-version";

export class TelegramUpdateJournalError extends Error {
  readonly code: TelegramUpdateJournalErrorCode;
  readonly path: string;

  constructor(
    code: TelegramUpdateJournalErrorCode,
    path: string,
    message: string,
    options: ErrorOptions = {},
  ) {
    super(message, options);
    this.name = "TelegramUpdateJournalError";
    this.code = code;
    this.path = path;
  }
}

export interface TelegramUpdateJournalBotIdentity {
  botId?: number;
  tokenSha256: string;
}

export interface TelegramUpdateJournalInput {
  update_id: number;
}

export type TelegramJournaledUpdate = TelegramUpdateJournalInput &
  Record<string, unknown>;

const TELEGRAM_UPDATE_CHAT_CARRIERS = new Set([
  "message",
  "edited_message",
  "channel_post",
  "edited_channel_post",
  "business_message",
  "edited_business_message",
  "message_reaction",
  "message_reaction_count",
  "my_chat_member",
  "chat_member",
  "chat_join_request",
  "chat_boost",
  "removed_chat_boost",
  "deleted_business_messages",
]);

function getUpdateCarrierScope(
  value: unknown,
): TelegramWorkspaceAdmissionScope | undefined {
  if (!isRecord(value) || !isRecord(value.chat)) return undefined;
  const chatId = value.chat.id;
  if (!Number.isSafeInteger(chatId) || chatId === 0) return undefined;
  const threadId = value.message_thread_id;
  if (threadId === undefined) return { kind: "chat", chatId: chatId as number };
  if (!Number.isSafeInteger(threadId) || (threadId as number) <= 0) {
    return undefined;
  }
  return {
    kind: "target",
    target: { chatId: chatId as number, threadId: threadId as number },
  };
}

function getUpdateAdmissionScope(
  update: TelegramUpdateJournalInput & Record<string, unknown>,
): TelegramWorkspaceAdmissionScope {
  const payloadKeys = Object.keys(update).filter((key) => key !== "update_id");
  if (payloadKeys.length !== 1) return { kind: "profile" };
  const payloadKey = payloadKeys[0];
  if (TELEGRAM_UPDATE_CHAT_CARRIERS.has(payloadKey)) {
    return getUpdateCarrierScope(update[payloadKey]) ?? { kind: "profile" };
  }
  if (payloadKey === "callback_query") {
    const query = update.callback_query;
    return isRecord(query)
      ? getUpdateCarrierScope(query.message) ?? { kind: "profile" }
      : { kind: "profile" };
  }
  return { kind: "profile" };
}

function getWorkspaceAdmissionScopeKey(
  scope: TelegramWorkspaceAdmissionScope,
): string {
  if (scope.kind === "profile") return "profile";
  if (scope.kind === "chat") return `chat:${scope.chatId}`;
  return `target:${scope.target.chatId}:${scope.target.threadId}`;
}

export function getTelegramUpdateJournalAdmissionScopes(
  updates: readonly (TelegramUpdateJournalInput & Record<string, unknown>)[],
): TelegramWorkspaceAdmissionScope[] {
  if (updates.length === 0) return [{ kind: "profile" }];
  const scopes = updates.map(getUpdateAdmissionScope);
  if (scopes.some((scope) => scope.kind === "profile")) {
    return [{ kind: "profile" }];
  }
  const chatIds = new Set(
    scopes
      .filter((scope): scope is { kind: "chat"; chatId: number } => scope.kind === "chat")
      .map((scope) => scope.chatId),
  );
  const unique = new Map<string, TelegramWorkspaceAdmissionScope>();
  for (const scope of scopes) {
    if (scope.kind === "target" && chatIds.has(scope.target.chatId)) continue;
    unique.set(getWorkspaceAdmissionScopeKey(scope), scope);
  }
  return Array.from(unique.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([, scope]) => scope);
}

export type TelegramUpdateJournalEntryState =
  | "pending"
  | "retry-wait"
  | "queued"
  | "failed";
export type TelegramUpdateJournalQueueKind = "prompt" | "control";

export interface TelegramUpdateJournalQueueProcessIdentity {
  processId: number;
  processBirthId: string;
}

export interface TelegramUpdateJournalQueueRuntimeIdentity
  extends TelegramUpdateJournalQueueProcessIdentity {
  instanceId: string;
}

export interface TelegramUpdateJournalQueueOwnerIdentity
  extends TelegramUpdateJournalQueueRuntimeIdentity {
  sessionGeneration: number;
}

export interface TelegramUpdateJournalQueueOwner
  extends TelegramUpdateJournalQueueOwnerIdentity {
  acquisitionId: string;
  acquiredAtMs: number;
  handoffId?: string;
}

export interface TelegramUpdateJournalQueueHandoff {
  handoffId: string;
  offeredAtMs: number;
  recipientOwner: TelegramUpdateJournalQueueOwnerIdentity;
}

export interface TelegramUpdateJournalFailure {
  attemptCount: number;
  failedAtMs: number;
  failureClass: string;
  summary: string;
}

export type TelegramUpdateJournalOperatorDispositionAction =
  | "retry"
  | "discard";

export interface TelegramUpdateJournalOperatorDisposition {
  failureId: string;
  updateId: number;
  action: TelegramUpdateJournalOperatorDispositionAction;
  committedAtMs: number;
  attemptCount: number;
  failureClass: string;
  terminalAtMs: number;
  terminalReason: string;
}

export interface TelegramUpdateJournalEntry {
  updateId: number;
  update: TelegramJournaledUpdate;
  admittedAtMs: number;
  state: TelegramUpdateJournalEntryState;
  queueKind?: TelegramUpdateJournalQueueKind;
  queueReceiptId?: string;
  queueOwner?: TelegramUpdateJournalQueueOwner;
  queueHandoff?: TelegramUpdateJournalQueueHandoff;
  failure?: TelegramUpdateJournalFailure;
  nextRetryAtMs?: number;
  terminalAtMs?: number;
  terminalReason?: string;
  terminalFailureId?: string;
}

export interface TelegramUpdateJournalFile {
  version: typeof TELEGRAM_UPDATE_JOURNAL_VERSION;
  revision?: number;
  acceptedThroughUpdateId?: number;
  profile: string;
  botIdentity: TelegramUpdateJournalBotIdentity;
  entries: TelegramUpdateJournalEntry[];
  operatorDispositions?: TelegramUpdateJournalOperatorDisposition[];
}

export interface TelegramUpdateJournalSnapshot
  extends TelegramUpdateJournalFile {
  exists: boolean;
  serializedBytes: number;
}

export interface TelegramUpdateJournalAppendResult {
  addedUpdateIds: number[];
  duplicateUpdateIds: number[];
  entryCount: number;
  serializedBytes: number;
}

export interface TelegramUpdateJournalRemoveResult {
  removedUpdateIds: number[];
  entryCount: number;
  serializedBytes: number;
}

export interface TelegramUpdateJournalQueueReceipt {
  queueKind: TelegramUpdateJournalQueueKind;
  receiptId: string;
  sourceUpdateIds: readonly number[];
  owner: TelegramUpdateJournalQueueOwnerIdentity;
}

export interface TelegramUpdateJournalQueueResult {
  queuedUpdateIds: number[];
  duplicateUpdateIds: number[];
  queueOwner?: TelegramUpdateJournalQueueOwner;
  entryCount: number;
  serializedBytes: number;
}

export interface TelegramUpdateJournalQueuedCompletion {
  queueKind: TelegramUpdateJournalQueueKind;
  receiptId: string;
  sourceUpdateIds: readonly number[];
  queueOwner: TelegramUpdateJournalQueueOwner;
}

export interface TelegramUpdateJournalQueueHandoffInput {
  queueKind: TelegramUpdateJournalQueueKind;
  receiptId: string;
  sourceUpdateIds: readonly number[];
  expectedOwner: TelegramUpdateJournalQueueOwner;
  recipientOwner: TelegramUpdateJournalQueueOwnerIdentity;
  handoffToken: string;
}

export interface TelegramUpdateJournalQueueHandoffOfferResult {
  handoff: TelegramUpdateJournalQueueHandoff;
  previousOwner: TelegramUpdateJournalQueueOwner;
  offeredUpdateIds: number[];
  duplicate: boolean;
  entryCount: number;
  serializedBytes: number;
}

export interface TelegramUpdateJournalQueueHandoffAcceptResult {
  handoffId: string;
  previousOwner?: TelegramUpdateJournalQueueOwner;
  queueOwner: TelegramUpdateJournalQueueOwner;
  acceptedUpdateIds: number[];
  duplicate: boolean;
  entryCount: number;
  serializedBytes: number;
}

export interface TelegramUpdateJournalQueueHandoffCancelResult {
  handoffId: string;
  previousOwner: TelegramUpdateJournalQueueOwner;
  cancelledUpdateIds: number[];
  entryCount: number;
  serializedBytes: number;
}

export interface TelegramUpdateJournalQueueDiscardInput {
  queueKind: TelegramUpdateJournalQueueKind;
  receiptId: string;
  sourceUpdateIds: readonly number[];
  expectedOwner: TelegramUpdateJournalQueueOwner;
}

export interface TelegramUpdateJournalQueueDiscardResult {
  previousOwner: TelegramUpdateJournalQueueOwner;
  removedUpdateIds: number[];
  entryCount: number;
  serializedBytes: number;
}

export interface TelegramUpdateJournalDeadQueueOwnerRecoveryInput {
  queueKind: TelegramUpdateJournalQueueKind;
  receiptId: string;
  sourceUpdateIds: readonly number[];
  deadOwner: TelegramUpdateJournalQueueOwner;
  recoveryOwner: TelegramUpdateJournalQueueOwnerIdentity;
}

export type TelegramUpdateJournalDeadQueueOwnerRecoveryResult =
  | {
      status: "owner-alive" | "owner-unverifiable";
      previousOwner: TelegramUpdateJournalQueueOwner;
      recoveredUpdateIds: [];
      entryCount: number;
      serializedBytes: number;
    }
  | {
      status: "recovered";
      previousOwner: TelegramUpdateJournalQueueOwner;
      recoveredUpdateIds: number[];
      entryCount: number;
      serializedBytes: number;
    };

export interface TelegramUpdateJournalFailureInput {
  updateId: number;
  expectedAttemptCount: number;
  failedAtMs: number;
  failureClass: string;
  summary: string;
  disposition: "retry-wait" | "failed";
  nextRetryAtMs?: number;
  terminalReason?: string;
}

export interface TelegramUpdateJournalFailureResult {
  entry: TelegramUpdateJournalEntry;
  entryCount: number;
  serializedBytes: number;
}

export interface TelegramUpdateJournalOperatorDispositionInput {
  updateId: number;
  failureId: string;
  action: TelegramUpdateJournalOperatorDispositionAction;
}

export interface TelegramUpdateJournalOperatorDispositionResult {
  disposition: TelegramUpdateJournalOperatorDisposition;
  duplicate: boolean;
  entryCount: number;
  serializedBytes: number;
}

export interface TelegramUpdateJournalStore {
  read(): TelegramUpdateJournalSnapshot;
  appendBatch<TUpdate extends TelegramUpdateJournalInput>(
    updates: readonly TUpdate[],
    acceptedThroughUpdateId?: number,
  ): TelegramUpdateJournalAppendResult;
  markQueued(
    receipt: TelegramUpdateJournalQueueReceipt,
  ): TelegramUpdateJournalQueueResult;
  markExecutionFailure(
    input: TelegramUpdateJournalFailureInput,
  ): TelegramUpdateJournalFailureResult;
  applyOperatorDisposition(
    input: TelegramUpdateJournalOperatorDispositionInput,
  ): TelegramUpdateJournalOperatorDispositionResult;
  offerQueuedHandoff(
    input: TelegramUpdateJournalQueueHandoffInput,
  ): TelegramUpdateJournalQueueHandoffOfferResult;
  acceptQueuedHandoff(
    input: TelegramUpdateJournalQueueHandoffInput,
  ): TelegramUpdateJournalQueueHandoffAcceptResult;
  cancelQueuedHandoff(
    input: TelegramUpdateJournalQueueHandoffInput,
  ): TelegramUpdateJournalQueueHandoffCancelResult;
  completeQueued(
    receipts: readonly TelegramUpdateJournalQueuedCompletion[],
  ): TelegramUpdateJournalRemoveResult;
  discardQueued(
    input: TelegramUpdateJournalQueueDiscardInput,
  ): TelegramUpdateJournalQueueDiscardResult;
  recoverDeadQueueOwner(
    input: TelegramUpdateJournalDeadQueueOwnerRecoveryInput,
  ): TelegramUpdateJournalDeadQueueOwnerRecoveryResult;
  removeCompleted(
    updateIds: readonly number[],
  ): TelegramUpdateJournalRemoveResult;
}

export type TelegramUpdateJournalPublicationBoundary =
  | "before-write"
  | "after-write-before-rename";

export interface TelegramUpdateJournalRecoveryEvent {
  kind: "repaired" | "reset";
  path: string;
  revision?: number;
  quarantinePath?: string;
  reason: string;
}

export interface TelegramUpdateJournalStoreOptions {
  path: string;
  profileName?: string;
  botIdentity: TelegramUpdateJournalBotIdentity;
  maxEntries?: number;
  maxBytes?: number;
  getNowMs?: () => number;
  onRecovery?: (event: TelegramUpdateJournalRecoveryEvent) => void;
  queueRuntimeIdentity?: TelegramUpdateJournalQueueRuntimeIdentity;
  getQueueProcessLiveness?: (
    owner: TelegramUpdateJournalQueueProcessIdentity,
  ) => TelegramProcessLiveness;
  workspaceAdmission?: Pick<
    TelegramWorkspaceAdmissionLedger,
    "acquireAdmission" | "releaseAdmission"
  >;
  onPublicationBoundary?: (
    boundary: TelegramUpdateJournalPublicationBoundary,
    publicationPath: string,
  ) => void;
}

interface ReadTelegramUpdateJournalResult {
  file: TelegramUpdateJournalFile;
  exists: boolean;
  serializedBytes: number;
}

export interface TelegramUpdateJournalSegment {
  version: typeof TELEGRAM_UPDATE_JOURNAL_VERSION;
  revision: number;
  previousRevision: number;
  acceptedThroughUpdateId?: number;
  profile: string;
  botIdentity: TelegramUpdateJournalBotIdentity;
  upsertedEntries: TelegramUpdateJournalEntry[];
  removedUpdateIds: number[];
  operatorDispositions?: TelegramUpdateJournalOperatorDisposition[];
}

export interface TelegramUpdateJournalSegmentPublicationResult {
  path: string;
  revision: number;
  serializedBytes: number;
}

function createJournalError(
  code: TelegramUpdateJournalErrorCode,
  path: string,
  detail: string,
  cause?: unknown,
): TelegramUpdateJournalError {
  return new TelegramUpdateJournalError(
    code,
    path,
    `Telegram update journal ${detail}: ${path}`,
    cause === undefined ? undefined : { cause },
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function hasOnlyKeys(
  value: Record<string, unknown>,
  allowedKeys: readonly string[],
): boolean {
  const allowed = new Set(allowedKeys);
  return Object.keys(value).every((key) => allowed.has(key));
}

function isSafeNonNegativeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function isSafePositiveInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) > 0;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isBoundedString(
  value: unknown,
  maxLength: number,
): value is string {
  return isNonEmptyString(value) && value.length <= maxLength;
}

function validateBotIdentity(
  value: unknown,
  path: string,
): TelegramUpdateJournalBotIdentity {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, ["botId", "tokenSha256"]) ||
    !isNonEmptyString(value.tokenSha256) ||
    !/^[a-f0-9]{64}$/u.test(value.tokenSha256) ||
    (value.botId !== undefined && !isSafePositiveInteger(value.botId))
  ) {
    throw createJournalError("invalid", path, "has invalid bot identity");
  }
  return {
    ...(value.botId !== undefined ? { botId: value.botId } : {}),
    tokenSha256: value.tokenSha256,
  };
}

function validateJournaledUpdate(
  value: unknown,
  path: string,
): TelegramJournaledUpdate {
  if (!isRecord(value) || !isSafeNonNegativeInteger(value.update_id)) {
    throw createJournalError(
      "invalid",
      path,
      "contains an update without a safe integer update_id",
    );
  }
  return value as TelegramJournaledUpdate;
}

function normalizeIncomingJournaledUpdate(
  value: unknown,
  path: string,
): TelegramJournaledUpdate {
  let normalized: unknown;
  try {
    const serialized = JSON.stringify(value);
    if (serialized === undefined) {
      throw new Error("Update is not JSON serializable.");
    }
    normalized = JSON.parse(serialized) as unknown;
  } catch (error) {
    throw createJournalError(
      "invalid",
      path,
      "received a non-JSON update",
      error,
    );
  }
  return validateJournaledUpdate(normalized, path);
}

function validateJournalQueueOwnerIdentity(
  value: unknown,
  path: string,
): TelegramUpdateJournalQueueOwnerIdentity {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "instanceId",
      "processId",
      "processBirthId",
      "sessionGeneration",
    ]) ||
    !isBoundedString(
      value.instanceId,
      TELEGRAM_UPDATE_JOURNAL_QUEUE_OWNER_ID_MAX_LENGTH,
    ) ||
    !isSafePositiveInteger(value.processId) ||
    !isBoundedString(
      value.processBirthId,
      TELEGRAM_UPDATE_JOURNAL_QUEUE_OWNER_ID_MAX_LENGTH,
    ) ||
    !isSafePositiveInteger(value.sessionGeneration)
  ) {
    throw createJournalError(
      "invalid",
      path,
      "contains invalid queue owner identity",
    );
  }
  return {
    instanceId: value.instanceId,
    processId: value.processId,
    processBirthId: value.processBirthId,
    sessionGeneration: value.sessionGeneration,
  };
}

function validateJournalQueueOwner(
  value: unknown,
  path: string,
): TelegramUpdateJournalQueueOwner {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "instanceId",
      "processId",
      "processBirthId",
      "sessionGeneration",
      "acquisitionId",
      "acquiredAtMs",
      "handoffId",
    ]) ||
    !isBoundedString(
      value.acquisitionId,
      TELEGRAM_UPDATE_JOURNAL_QUEUE_OWNER_ID_MAX_LENGTH,
    ) ||
    !isSafeNonNegativeInteger(value.acquiredAtMs) ||
    (value.handoffId !== undefined &&
      !isBoundedString(
        value.handoffId,
        TELEGRAM_UPDATE_JOURNAL_QUEUE_HANDOFF_ID_MAX_LENGTH,
      ))
  ) {
    throw createJournalError(
      "invalid",
      path,
      "contains invalid queue receipt acquisition",
    );
  }
  const identity = validateJournalQueueOwnerIdentity(
    {
      instanceId: value.instanceId,
      processId: value.processId,
      processBirthId: value.processBirthId,
      sessionGeneration: value.sessionGeneration,
    },
    path,
  );
  return {
    ...identity,
    acquisitionId: value.acquisitionId,
    acquiredAtMs: value.acquiredAtMs,
    ...(typeof value.handoffId === "string"
      ? { handoffId: value.handoffId }
      : {}),
  };
}

export function parseTelegramUpdateJournalQueueOwner(
  value: unknown,
): TelegramUpdateJournalQueueOwner | undefined {
  try {
    return validateJournalQueueOwner(value, "Telegram queue handoff acknowledgement");
  } catch {
    return undefined;
  }
}

export function isTelegramUpdateJournalQueueOwnerProcess(
  owner: TelegramUpdateJournalQueueOwner,
  identity: TelegramUpdateJournalQueueOwnerIdentity,
): boolean {
  return (
    owner.instanceId === identity.instanceId &&
    owner.processId === identity.processId &&
    owner.processBirthId === identity.processBirthId
  );
}

export function areTelegramUpdateJournalQueueOwnersEqual(
  left: TelegramUpdateJournalQueueOwner,
  right: TelegramUpdateJournalQueueOwner,
): boolean {
  return (
    isTelegramUpdateJournalQueueOwnerProcess(left, right) &&
    left.sessionGeneration === right.sessionGeneration &&
    left.acquisitionId === right.acquisitionId &&
    left.acquiredAtMs === right.acquiredAtMs &&
    left.handoffId === right.handoffId
  );
}

function cloneJournalQueueOwner(
  owner: TelegramUpdateJournalQueueOwner,
): TelegramUpdateJournalQueueOwner {
  return { ...owner };
}

function createTelegramUpdateQueueHandoffId(input: {
  handoffToken: string;
  queueKind: TelegramUpdateJournalQueueKind;
  receiptId: string;
  sourceUpdateIds: readonly number[];
  expectedOwner: TelegramUpdateJournalQueueOwner;
  recipientOwner: TelegramUpdateJournalQueueOwnerIdentity;
}): string {
  return `handoff-${createHash("sha256")
    .update(
      JSON.stringify({
        version: 1,
        token: input.handoffToken,
        queueKind: input.queueKind,
        receiptId: input.receiptId,
        sourceUpdateIds: [...input.sourceUpdateIds].sort((a, b) => a - b),
        donorAcquisitionId: input.expectedOwner.acquisitionId,
        recipientOwner: input.recipientOwner,
      }),
    )
    .digest("hex")
    .slice(0, 32)}`;
}

function validateJournalQueueHandoff(
  value: unknown,
  path: string,
): TelegramUpdateJournalQueueHandoff {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, ["handoffId", "offeredAtMs", "recipientOwner"]) ||
    !isBoundedString(
      value.handoffId,
      TELEGRAM_UPDATE_JOURNAL_QUEUE_HANDOFF_ID_MAX_LENGTH,
    ) ||
    !isSafeNonNegativeInteger(value.offeredAtMs)
  ) {
    throw createJournalError(
      "invalid",
      path,
      "contains invalid queue handoff metadata",
    );
  }
  return {
    handoffId: value.handoffId,
    offeredAtMs: value.offeredAtMs,
    recipientOwner: validateJournalQueueOwnerIdentity(
      value.recipientOwner,
      path,
    ),
  };
}

function cloneJournalQueueHandoff(
  handoff: TelegramUpdateJournalQueueHandoff,
): TelegramUpdateJournalQueueHandoff {
  return {
    ...handoff,
    recipientOwner: { ...handoff.recipientOwner },
  };
}

function validateJournalFailure(
  value: unknown,
  path: string,
): TelegramUpdateJournalFailure {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "attemptCount",
      "failedAtMs",
      "failureClass",
      "summary",
    ]) ||
    !isSafePositiveInteger(value.attemptCount) ||
    !isSafeNonNegativeInteger(value.failedAtMs) ||
    !isBoundedString(
      value.failureClass,
      TELEGRAM_UPDATE_JOURNAL_FAILURE_CLASS_MAX_LENGTH,
    ) ||
    !isBoundedString(
      value.summary,
      TELEGRAM_UPDATE_JOURNAL_FAILURE_SUMMARY_MAX_LENGTH,
    )
  ) {
    throw createJournalError(
      "invalid",
      path,
      "contains invalid execution failure metadata",
    );
  }
  return {
    attemptCount: value.attemptCount,
    failedAtMs: value.failedAtMs,
    failureClass: value.failureClass,
    summary: value.summary,
  };
}

function createTelegramUpdateTerminalFailureId(input: {
  updateId: number;
  attemptCount: number;
  failedAtMs: number;
  failureClass: string;
  terminalAtMs: number;
  terminalReason: string;
}): string {
  return `failure-${createHash("sha256")
    .update(JSON.stringify(input))
    .digest("hex")
    .slice(0, 32)}`;
}

function validateJournalOperatorDisposition(
  value: unknown,
  path: string,
): TelegramUpdateJournalOperatorDisposition {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "failureId",
      "updateId",
      "action",
      "committedAtMs",
      "attemptCount",
      "failureClass",
      "terminalAtMs",
      "terminalReason",
    ]) ||
    !isBoundedString(
      value.failureId,
      TELEGRAM_UPDATE_JOURNAL_FAILURE_ID_MAX_LENGTH,
    ) ||
    !isSafeNonNegativeInteger(value.updateId) ||
    (value.action !== "retry" && value.action !== "discard") ||
    !isSafeNonNegativeInteger(value.committedAtMs) ||
    !isSafePositiveInteger(value.attemptCount) ||
    !isBoundedString(
      value.failureClass,
      TELEGRAM_UPDATE_JOURNAL_FAILURE_CLASS_MAX_LENGTH,
    ) ||
    !isSafeNonNegativeInteger(value.terminalAtMs) ||
    value.committedAtMs < value.terminalAtMs ||
    !isBoundedString(
      value.terminalReason,
      TELEGRAM_UPDATE_JOURNAL_TERMINAL_REASON_MAX_LENGTH,
    )
  ) {
    throw createJournalError(
      "invalid",
      path,
      "contains invalid operator disposition metadata",
    );
  }
  return {
    failureId: value.failureId,
    updateId: value.updateId,
    action: value.action,
    committedAtMs: value.committedAtMs,
    attemptCount: value.attemptCount,
    failureClass: value.failureClass,
    terminalAtMs: value.terminalAtMs,
    terminalReason: value.terminalReason,
  };
}

function validateJournalEntry(
  value: unknown,
  path: string,
): TelegramUpdateJournalEntry {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "updateId",
      "update",
      "admittedAtMs",
      "state",
      "queueKind",
      "queueReceiptId",
      "queueOwner",
      "queueHandoff",
      "failure",
      "nextRetryAtMs",
      "terminalAtMs",
      "terminalReason",
      "terminalFailureId",
    ]) ||
    !isSafeNonNegativeInteger(value.updateId) ||
    !isSafeNonNegativeInteger(value.admittedAtMs) ||
    (value.state !== "pending" &&
      value.state !== "retry-wait" &&
      value.state !== "queued" &&
      value.state !== "failed")
  ) {
    throw createJournalError("invalid", path, "contains an invalid entry");
  }
  const update = validateJournaledUpdate(value.update, path);
  if (update.update_id !== value.updateId) {
    throw createJournalError(
      "invalid",
      path,
      "contains an entry/update id mismatch",
    );
  }
  const queueKind = value.queueKind;
  const queueReceiptId = value.queueReceiptId;
  const hasQueueMetadata =
    queueKind !== undefined ||
    queueReceiptId !== undefined ||
    value.queueOwner !== undefined ||
    value.queueHandoff !== undefined;
  const hasFailureMetadata =
    value.failure !== undefined ||
    value.nextRetryAtMs !== undefined ||
    value.terminalAtMs !== undefined ||
    value.terminalReason !== undefined ||
    value.terminalFailureId !== undefined;
  if (value.state === "pending" && (hasQueueMetadata || hasFailureMetadata)) {
    throw createJournalError(
      "invalid",
      path,
      "contains metadata on a pending entry",
    );
  }
  if (
    value.state === "queued" &&
    ((queueKind !== "prompt" && queueKind !== "control") ||
      !isNonEmptyString(queueReceiptId) ||
      value.queueOwner === undefined ||
      hasFailureMetadata)
  ) {
    throw createJournalError(
      "invalid",
      path,
      "contains invalid queued entry metadata",
    );
  }
  const queueOwner =
    value.state === "queued" && value.queueOwner !== undefined
      ? validateJournalQueueOwner(value.queueOwner, path)
      : undefined;
  const queueHandoff =
    value.state === "queued" && value.queueHandoff !== undefined
      ? validateJournalQueueHandoff(value.queueHandoff, path)
      : undefined;
  if (queueHandoff && !queueOwner) {
    throw createJournalError(
      "invalid",
      path,
      "contains a queue handoff without donor authority",
    );
  }
  let failure: TelegramUpdateJournalFailure | undefined;
  if (value.state === "retry-wait" || value.state === "failed") {
    if (hasQueueMetadata) {
      throw createJournalError(
        "invalid",
        path,
        "contains queue metadata on a failed execution entry",
      );
    }
    failure = validateJournalFailure(value.failure, path);
  }
  if (
    value.state === "retry-wait" &&
    (!isSafeNonNegativeInteger(value.nextRetryAtMs) ||
      value.nextRetryAtMs < failure!.failedAtMs ||
      value.terminalAtMs !== undefined ||
      value.terminalReason !== undefined ||
      value.terminalFailureId !== undefined)
  ) {
    throw createJournalError(
      "invalid",
      path,
      "contains invalid retry-wait metadata",
    );
  }
  if (
    value.state === "failed" &&
    (!isSafeNonNegativeInteger(value.terminalAtMs) ||
      value.terminalAtMs < failure!.failedAtMs ||
      !isBoundedString(
        value.terminalReason,
        TELEGRAM_UPDATE_JOURNAL_TERMINAL_REASON_MAX_LENGTH,
      ) ||
      (value.terminalFailureId !== undefined &&
        !isBoundedString(
          value.terminalFailureId,
          TELEGRAM_UPDATE_JOURNAL_FAILURE_ID_MAX_LENGTH,
        )) ||
      value.nextRetryAtMs !== undefined)
  ) {
    throw createJournalError(
      "invalid",
      path,
      "contains invalid terminal failure metadata",
    );
  }
  return {
    updateId: value.updateId,
    update,
    admittedAtMs: value.admittedAtMs,
    state: value.state,
    ...(queueKind === "prompt" || queueKind === "control"
      ? { queueKind }
      : {}),
    ...(isNonEmptyString(queueReceiptId) ? { queueReceiptId } : {}),
    ...(queueOwner ? { queueOwner } : {}),
    ...(queueHandoff ? { queueHandoff } : {}),
    ...(failure ? { failure } : {}),
    ...(isSafeNonNegativeInteger(value.nextRetryAtMs)
      ? { nextRetryAtMs: value.nextRetryAtMs }
      : {}),
    ...(isSafeNonNegativeInteger(value.terminalAtMs)
      ? { terminalAtMs: value.terminalAtMs }
      : {}),
    ...(isNonEmptyString(value.terminalReason)
      ? { terminalReason: value.terminalReason }
      : {}),
    ...(value.state === "failed"
      ? {
          terminalFailureId:
            isBoundedString(
              value.terminalFailureId,
              TELEGRAM_UPDATE_JOURNAL_FAILURE_ID_MAX_LENGTH,
            )
              ? value.terminalFailureId
              : createTelegramUpdateTerminalFailureId({
                  updateId: value.updateId,
                  attemptCount: failure!.attemptCount,
                  failedAtMs: failure!.failedAtMs,
                  failureClass: failure!.failureClass,
                  terminalAtMs: value.terminalAtMs as number,
                  terminalReason: value.terminalReason as string,
                }),
        }
      : {}),
  };
}

function parseJournalFile(
  value: unknown,
  path: string,
): TelegramUpdateJournalFile {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "version",
      "revision",
      "acceptedThroughUpdateId",
      "profile",
      "botIdentity",
      "entries",
      "operatorDispositions",
    ]) ||
    !Number.isSafeInteger(value.version) ||
    (value.revision !== undefined &&
      (!isSafeNonNegativeInteger(value.revision) || value.revision === 0)) ||
    (value.acceptedThroughUpdateId !== undefined &&
      !isSafeNonNegativeInteger(value.acceptedThroughUpdateId)) ||
    !isNonEmptyString(value.profile) ||
    !Array.isArray(value.entries) ||
    (value.operatorDispositions !== undefined &&
      !Array.isArray(value.operatorDispositions))
  ) {
    throw createJournalError("invalid", path, "has a malformed schema");
  }
  if (value.version !== TELEGRAM_UPDATE_JOURNAL_VERSION) {
    throw createJournalError(
      "unsupported-version",
      path,
      `uses unsupported version ${String(value.version)}`,
    );
  }
  const entries = value.entries.map((entry) =>
    validateJournalEntry(entry, path),
  );
  for (let index = 1; index < entries.length; index += 1) {
    if (entries[index]!.updateId <= entries[index - 1]!.updateId) {
      throw createJournalError(
        "invalid",
        path,
        "contains duplicate or unordered entry ids",
      );
    }
  }
  if (
    entries.length > 0 &&
    value.acceptedThroughUpdateId !== undefined &&
    value.acceptedThroughUpdateId < entries.at(-1)!.updateId
  ) {
    throw createJournalError(
      "invalid",
      path,
      "has an admission cursor behind its active entries",
    );
  }
  const queuedReceipts = new Map<
    string,
    {
      queueKind: TelegramUpdateJournalQueueKind;
      queueOwner?: TelegramUpdateJournalQueueOwner;
      queueHandoff?: TelegramUpdateJournalQueueHandoff;
    }
  >();
  for (const entry of entries) {
    if (
      entry.state !== "queued" ||
      !entry.queueKind ||
      !entry.queueReceiptId
    ) {
      continue;
    }
    const existing = queuedReceipts.get(entry.queueReceiptId);
    if (
      existing &&
      (existing.queueKind !== entry.queueKind ||
        (existing.queueOwner === undefined) !==
          (entry.queueOwner === undefined) ||
        (existing.queueOwner !== undefined &&
          entry.queueOwner !== undefined &&
          !areTelegramUpdateJournalQueueOwnersEqual(
            existing.queueOwner,
            entry.queueOwner,
          )) ||
        (existing.queueHandoff === undefined) !==
          (entry.queueHandoff === undefined) ||
        (existing.queueHandoff !== undefined &&
          entry.queueHandoff !== undefined &&
          !isDeepStrictEqual(existing.queueHandoff, entry.queueHandoff)))
    ) {
      throw createJournalError(
        "invalid",
        path,
        `contains inconsistent queued receipt ${entry.queueReceiptId}`,
      );
    }
    if (!existing) {
      queuedReceipts.set(entry.queueReceiptId, {
        queueKind: entry.queueKind,
        ...(entry.queueOwner
          ? { queueOwner: cloneJournalQueueOwner(entry.queueOwner) }
          : {}),
        ...(entry.queueHandoff
          ? { queueHandoff: cloneJournalQueueHandoff(entry.queueHandoff) }
          : {}),
      });
    }
  }
  const operatorDispositions = (
    (value.operatorDispositions as unknown[] | undefined) ?? []
  ).map((disposition) =>
    validateJournalOperatorDisposition(disposition, path),
  );
  const dispositionFailureIds = new Set<string>();
  const entriesByUpdateId = new Map(
    entries.map((entry) => [entry.updateId, entry]),
  );
  for (const disposition of operatorDispositions) {
    if (dispositionFailureIds.has(disposition.failureId)) {
      throw createJournalError(
        "invalid",
        path,
        "contains duplicate operator disposition failure ids",
      );
    }
    const currentEntry = entriesByUpdateId.get(disposition.updateId);
    if (
      currentEntry?.terminalFailureId === disposition.failureId ||
      (disposition.action === "discard" && currentEntry !== undefined)
    ) {
      throw createJournalError(
        "invalid",
        path,
        "contains operator-disposed active authority",
      );
    }
    dispositionFailureIds.add(disposition.failureId);
  }
  return {
    version: TELEGRAM_UPDATE_JOURNAL_VERSION,
    ...(value.revision !== undefined
      ? { revision: value.revision as number }
      : {}),
    ...(value.acceptedThroughUpdateId !== undefined
      ? { acceptedThroughUpdateId: value.acceptedThroughUpdateId as number }
      : {}),
    profile: value.profile,
    botIdentity: validateBotIdentity(value.botIdentity, path),
    entries,
    ...(operatorDispositions.length > 0 ? { operatorDispositions } : {}),
  };
}

function parseJournalSegment(
  value: unknown,
  path: string,
): TelegramUpdateJournalSegment {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "version",
      "revision",
      "previousRevision",
      "acceptedThroughUpdateId",
      "profile",
      "botIdentity",
      "upsertedEntries",
      "removedUpdateIds",
      "operatorDispositions",
    ]) ||
    value.version !== TELEGRAM_UPDATE_JOURNAL_VERSION ||
    !isSafePositiveInteger(value.revision) ||
    !isSafeNonNegativeInteger(value.previousRevision) ||
    (value.acceptedThroughUpdateId !== undefined &&
      !isSafeNonNegativeInteger(value.acceptedThroughUpdateId)) ||
    !isNonEmptyString(value.profile) ||
    !Array.isArray(value.upsertedEntries) ||
    !Array.isArray(value.removedUpdateIds) ||
    (value.operatorDispositions !== undefined &&
      !Array.isArray(value.operatorDispositions))
  ) {
    throw createJournalError("invalid", path, "has a malformed segment schema");
  }
  const upsertedEntries = value.upsertedEntries.map((entry) =>
    validateJournalEntry(entry, path),
  );
  const upsertedIds = new Set<number>();
  for (const entry of upsertedEntries) {
    if (upsertedIds.has(entry.updateId)) {
      throw createJournalError("invalid", path, "has duplicate segment upserts");
    }
    upsertedIds.add(entry.updateId);
  }
  if (
    upsertedEntries.length > 0 &&
    value.acceptedThroughUpdateId !== undefined &&
    value.acceptedThroughUpdateId <
      Math.max(...upsertedEntries.map((entry) => entry.updateId))
  ) {
    throw createJournalError(
      "invalid",
      path,
      "has an admission cursor behind its segment upserts",
    );
  }
  const removedUpdateIds: number[] = [];
  const removedIds = new Set<number>();
  for (const updateId of value.removedUpdateIds) {
    if (
      !isSafeNonNegativeInteger(updateId) ||
      removedIds.has(updateId) ||
      upsertedIds.has(updateId)
    ) {
      throw createJournalError("invalid", path, "has invalid segment removals");
    }
    removedIds.add(updateId);
    removedUpdateIds.push(updateId);
  }
  const operatorDispositions = (
    (value.operatorDispositions as unknown[] | undefined) ?? []
  ).map((disposition) =>
    validateJournalOperatorDisposition(disposition, path),
  );
  return {
    version: TELEGRAM_UPDATE_JOURNAL_VERSION,
    revision: value.revision,
    previousRevision: value.previousRevision,
    ...(value.acceptedThroughUpdateId !== undefined
      ? { acceptedThroughUpdateId: value.acceptedThroughUpdateId as number }
      : {}),
    profile: value.profile,
    botIdentity: validateBotIdentity(value.botIdentity, path),
    upsertedEntries,
    removedUpdateIds,
    ...(value.operatorDispositions !== undefined
      ? { operatorDispositions }
      : {}),
  };
}

function cloneEntry(entry: TelegramUpdateJournalEntry): TelegramUpdateJournalEntry {
  return {
    ...entry,
    update: structuredClone(entry.update),
    ...(entry.queueOwner
      ? { queueOwner: cloneJournalQueueOwner(entry.queueOwner) }
      : {}),
    ...(entry.queueHandoff
      ? { queueHandoff: cloneJournalQueueHandoff(entry.queueHandoff) }
      : {}),
    ...(entry.failure ? { failure: { ...entry.failure } } : {}),
  };
}

function cloneFile(file: TelegramUpdateJournalFile): TelegramUpdateJournalFile {
  return {
    version: TELEGRAM_UPDATE_JOURNAL_VERSION,
    ...(file.revision !== undefined ? { revision: file.revision } : {}),
    ...(file.acceptedThroughUpdateId !== undefined
      ? { acceptedThroughUpdateId: file.acceptedThroughUpdateId }
      : {}),
    profile: file.profile,
    botIdentity: { ...file.botIdentity },
    entries: file.entries.map(cloneEntry),
    ...(file.operatorDispositions?.length
      ? {
          operatorDispositions: file.operatorDispositions.map(
            (disposition) => ({ ...disposition }),
          ),
        }
      : {}),
  };
}

function serializeJournalFile(file: TelegramUpdateJournalFile): string {
  return `${JSON.stringify(file, null, 2)}\n`;
}

function getTelegramUpdateJournalSegmentDirectory(path: string): string {
  return `${path}.segments`;
}

function getTelegramUpdateJournalSegmentPath(
  path: string,
  revision: number,
): string {
  return join(
    getTelegramUpdateJournalSegmentDirectory(path),
    `${String(revision).padStart(16, "0")}.json`,
  );
}

function publishTelegramUpdateJournalSegmentUnlocked(
  path: string,
  segment: TelegramUpdateJournalSegment,
  onPublicationBoundary?: TelegramUpdateJournalStoreOptions["onPublicationBoundary"],
): TelegramUpdateJournalSegmentPublicationResult {
  const segmentDirectory = getTelegramUpdateJournalSegmentDirectory(path);
  const segmentPath = getTelegramUpdateJournalSegmentPath(
    path,
    segment.revision,
  );
  const serialized = `${JSON.stringify(segment, null, 2)}\n`;
  mkdirSync(segmentDirectory, { recursive: true, mode: 0o700 });
  const revisions = readdirSync(segmentDirectory)
      .flatMap((name) => {
        const match = name.match(/^(\d{16})\.json$/u);
        return match ? [Number(match[1])] : [];
      })
      .filter(Number.isSafeInteger);
    let snapshotRevision = 0;
    try {
      const parsed = JSON.parse(readFileSync(path, "utf8")) as unknown;
      snapshotRevision = parseJournalFile(parsed, path).revision ?? 0;
    } catch (error) {
      if ((error as { code?: unknown })?.code !== "ENOENT") throw error;
    }
    const latestRevision = Math.max(snapshotRevision, ...revisions, 0);
    if (latestRevision >= segment.revision) {
      try {
        if (readFileSync(segmentPath, "utf8") === serialized) {
          return {
            path: segmentPath,
            revision: segment.revision,
            serializedBytes: Buffer.byteLength(serialized),
          };
        }
      } catch {
        // A compacted snapshot may already contain this revision.
      }
      throw new Error("Telegram update journal segment revision conflicts.");
    }
    if (latestRevision !== segment.previousRevision) {
      throw new Error("Telegram update journal segment revision has a gap.");
    }
  writeJournalFile(segmentPath, serialized, onPublicationBoundary);
  return {
    path: segmentPath,
    revision: segment.revision,
    serializedBytes: Buffer.byteLength(serialized),
  };
}

export function publishTelegramUpdateJournalSegment(
  path: string,
  segment: TelegramUpdateJournalSegment,
): TelegramUpdateJournalSegmentPublicationResult {
  if (
    segment.version !== TELEGRAM_UPDATE_JOURNAL_VERSION ||
    !isSafePositiveInteger(segment.revision) ||
    segment.previousRevision !== segment.revision - 1 ||
    !isNonEmptyString(segment.profile) ||
    !Array.isArray(segment.upsertedEntries) ||
    !Array.isArray(segment.removedUpdateIds)
  ) {
    throw new Error("Telegram update journal segment is invalid.");
  }
  return withTelegramFileTransaction(`${path}.transaction`, () =>
    publishTelegramUpdateJournalSegmentUnlocked(path, segment),
  );
}

function normalizeCapacityLimit(
  value: number | undefined,
  fallback: number,
  label: string,
): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new Error(`Telegram update journal ${label} must be a positive integer.`);
  }
  return resolved;
}

function identitiesMatch(
  left: TelegramUpdateJournalBotIdentity,
  right: TelegramUpdateJournalBotIdentity,
): boolean {
  if (
    left.botId !== undefined &&
    right.botId !== undefined &&
    left.botId !== right.botId
  ) {
    return false;
  }
  return (
    (left.botId !== undefined && left.botId === right.botId) ||
    left.tokenSha256 === right.tokenSha256
  );
}

function mergeBotIdentity(
  stored: TelegramUpdateJournalBotIdentity,
  current: TelegramUpdateJournalBotIdentity,
): TelegramUpdateJournalBotIdentity {
  return {
    ...(current.botId !== undefined
      ? { botId: current.botId }
      : stored.botId !== undefined
        ? { botId: stored.botId }
        : {}),
    tokenSha256: current.tokenSha256,
  };
}

function writeJournalFile(
  path: string,
  serialized: string,
  onPublicationBoundary?: TelegramUpdateJournalStoreOptions["onPublicationBoundary"],
): void {
  const tempPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  try {
    onPublicationBoundary?.("before-write", path);
    writeFileSync(tempPath, serialized, { encoding: "utf8", mode: 0o600 });
    chmodSync(tempPath, 0o600);
    onPublicationBoundary?.("after-write-before-rename", path);
    if (!renameTelegramPathWithRetry(tempPath, path)) {
      throw new Error("Temporary journal file disappeared before publication.");
    }
    chmodSync(path, 0o600);
  } finally {
    try {
      unlinkSync(tempPath);
    } catch {
      // The successful atomic rename already consumed the temporary path.
    }
  }
}

export function createTelegramUpdateQueueHandoffToken(): string {
  return randomBytes(32).toString("base64url");
}

export function createTelegramUpdateJournalBotIdentity(input: {
  botToken: string;
  botId?: number;
}): TelegramUpdateJournalBotIdentity {
  if (!input.botToken) {
    throw new Error("Telegram update journal requires a configured bot token.");
  }
  if (input.botId !== undefined && !isSafePositiveInteger(input.botId)) {
    throw new Error("Telegram update journal bot id must be a safe integer.");
  }
  return {
    ...(input.botId !== undefined ? { botId: input.botId } : {}),
    tokenSha256: createHash("sha256").update(input.botToken).digest("hex"),
  };
}

export function createTelegramUpdateJournalReceiptScope(input: {
  profileName?: string;
  botIdentity: TelegramUpdateJournalBotIdentity;
}): string {
  const profile = (input.profileName ?? TELEGRAM_DEFAULT_PROFILE_NAME).trim();
  if (!profile) {
    throw new Error("Telegram update journal receipt scope requires a profile.");
  }
  if (
    input.botIdentity.botId !== undefined &&
    !isSafePositiveInteger(input.botIdentity.botId)
  ) {
    throw new Error("Telegram update journal bot id must be a safe integer.");
  }
  if (!/^[a-f0-9]{64}$/u.test(input.botIdentity.tokenSha256)) {
    throw new Error(
      "Telegram update journal receipt scope requires a SHA-256 token fingerprint.",
    );
  }
  return JSON.stringify({
    version: TELEGRAM_UPDATE_JOURNAL_VERSION,
    profile,
    bot:
      input.botIdentity.botId === undefined
        ? { tokenSha256: input.botIdentity.tokenSha256 }
        : { botId: input.botIdentity.botId },
  });
}

export function createTelegramUpdateJournalBindingKey(input: {
  path: string;
  profileName?: string;
  botIdentity: TelegramUpdateJournalBotIdentity;
}): string {
  if (!input.path) {
    throw new Error("Telegram update journal binding requires a path.");
  }
  return JSON.stringify({
    version: TELEGRAM_UPDATE_JOURNAL_VERSION,
    path: input.path,
    receiptScope: createTelegramUpdateJournalReceiptScope(input),
  });
}

export function getTelegramUpdateJournalBindingPath(
  journalBindingKey: string,
): string | undefined {
  try {
    const value = JSON.parse(journalBindingKey) as Record<string, unknown>;
    return value.version === TELEGRAM_UPDATE_JOURNAL_VERSION &&
      typeof value.path === "string" &&
      value.path.length > 0 &&
      typeof value.receiptScope === "string"
      ? value.path
      : undefined;
  } catch {
    return undefined;
  }
}

export function createTelegramUpdateJournalReceiptScopeResolver(deps: {
  getProfileName: () => string | undefined;
  getBotToken: () => string | undefined;
  getBotId: () => number | undefined;
}): () => string | undefined {
  let identityKey: string | undefined;
  let receiptScope: string | undefined;
  return () => {
    const botToken = deps.getBotToken();
    if (!botToken) {
      identityKey = undefined;
      receiptScope = undefined;
      return undefined;
    }
    const profileName = deps.getProfileName() ?? TELEGRAM_DEFAULT_PROFILE_NAME;
    const botIdentity = createTelegramUpdateJournalBotIdentity({
      botToken,
      botId: deps.getBotId(),
    });
    const nextIdentityKey = `${profileName}\u0000${botIdentity.tokenSha256}`;
    if (nextIdentityKey === identityKey && receiptScope) return receiptScope;
    identityKey = nextIdentityKey;
    receiptScope = createTelegramUpdateJournalReceiptScope({
      profileName,
      botIdentity,
    });
    return receiptScope;
  };
}

export interface TelegramUpdateJournalRuntimeBinding {
  runtimeKey: string;
  recoveryKey: string;
  journal: TelegramUpdateJournalStore;
}

export interface TelegramUpdateJournalRuntimeBindingResolverDeps {
  getProfileName: () => string | undefined;
  getBotToken: () => string | undefined;
  getBotId: () => number | undefined;
  getJournalPath: (profileName?: string) => string;
  getQueueRuntimeIdentity?: () => TelegramUpdateJournalQueueRuntimeIdentity;
  getWorkspaceAdmission?: () => Pick<
    TelegramWorkspaceAdmissionLedger,
    "acquireAdmission" | "releaseAdmission"
  > | undefined;
  onRecovery?: (event: TelegramUpdateJournalRecoveryEvent) => void;
}

export function createTelegramUpdateJournalRuntimeBindingResolver(
  deps: TelegramUpdateJournalRuntimeBindingResolverDeps,
): () => TelegramUpdateJournalRuntimeBinding | undefined {
  return () => {
    const botToken = deps.getBotToken();
    if (!botToken) return undefined;
    const configuredProfileName = deps.getProfileName();
    const profileName =
      configuredProfileName ?? TELEGRAM_DEFAULT_PROFILE_NAME;
    const botIdentity = createTelegramUpdateJournalBotIdentity({
      botToken,
      botId: deps.getBotId(),
    });
    const path = deps.getJournalPath(configuredProfileName);
    const workspaceAdmission = deps.getWorkspaceAdmission?.();
    return {
      runtimeKey: JSON.stringify({
        path,
        profileName,
        botIdentity,
      }),
      recoveryKey: createTelegramUpdateJournalBindingKey({
        path,
        profileName,
        botIdentity,
      }),
      journal: createTelegramUpdateJournalStore({
        path,
        profileName,
        botIdentity,
        ...(deps.getQueueRuntimeIdentity
          ? { queueRuntimeIdentity: deps.getQueueRuntimeIdentity() }
          : {}),
        ...(workspaceAdmission ? { workspaceAdmission } : {}),
        ...(deps.onRecovery ? { onRecovery: deps.onRecovery } : {}),
      }),
    };
  };
}

export interface TelegramUpdateJournalBindingRuntime {
  resolveLeader: () => TelegramUpdateJournalRuntimeBinding | undefined;
  resolveFollower: () => TelegramUpdateJournalRuntimeBinding | undefined;
  resolveActive: () => TelegramUpdateJournalRuntimeBinding | undefined;
  getActiveRecoveryKey: () => string | undefined;
  createRecipientResolver: (
    recipientBindingKey: string,
  ) => () => TelegramUpdateJournalRuntimeBinding | undefined;
  createPathResolver: (
    path: string,
  ) => () => TelegramUpdateJournalRuntimeBinding | undefined;
}

export function createTelegramUpdateJournalBindingRuntime(deps: {
  base: Omit<TelegramUpdateJournalRuntimeBindingResolverDeps, "getJournalPath">;
  getLeaderJournalPath: (profileName?: string) => string;
  getFollowerJournalPath: (
    bindingKey: string,
    profileName?: string,
  ) => string;
  getActiveFollowerBindingKey: () => string;
  isFollowerRegistered: () => boolean;
}): TelegramUpdateJournalBindingRuntime {
  const resolveLeader = createTelegramUpdateJournalRuntimeBindingResolver({
    ...deps.base,
    getJournalPath: deps.getLeaderJournalPath,
  });
  const createFollowerResolver = (
    bindingKey: string,
    includeQueueRuntimeIdentity: boolean,
  ) =>
    createTelegramUpdateJournalRuntimeBindingResolver({
      getProfileName: deps.base.getProfileName,
      getBotToken: deps.base.getBotToken,
      getBotId: deps.base.getBotId,
      ...(includeQueueRuntimeIdentity && deps.base.getQueueRuntimeIdentity
        ? { getQueueRuntimeIdentity: deps.base.getQueueRuntimeIdentity }
        : {}),
      ...(deps.base.getWorkspaceAdmission
        ? { getWorkspaceAdmission: deps.base.getWorkspaceAdmission }
        : {}),
      ...(deps.base.onRecovery ? { onRecovery: deps.base.onRecovery } : {}),
      getJournalPath(profileName) {
        return deps.getFollowerJournalPath(bindingKey, profileName);
      },
    });
  const resolveFollower = () =>
    createFollowerResolver(deps.getActiveFollowerBindingKey(), true)();
  const resolveActive = () =>
    deps.isFollowerRegistered() ? resolveFollower() : resolveLeader();
  return {
    resolveLeader,
    resolveFollower,
    resolveActive,
    getActiveRecoveryKey: () => resolveActive()?.recoveryKey,
    createRecipientResolver: (bindingKey) =>
      createFollowerResolver(bindingKey, false),
    createPathResolver: (path) =>
      createTelegramUpdateJournalRuntimeBindingResolver({
        getProfileName: deps.base.getProfileName,
        getBotToken: deps.base.getBotToken,
        getBotId: deps.base.getBotId,
        ...(deps.base.getWorkspaceAdmission
          ? { getWorkspaceAdmission: deps.base.getWorkspaceAdmission }
          : {}),
        ...(deps.base.onRecovery ? { onRecovery: deps.base.onRecovery } : {}),
        getJournalPath: () => path,
      }),
  };
}

export function createTelegramUpdateJournalStore(
  options: TelegramUpdateJournalStoreOptions,
): TelegramUpdateJournalStore {
  const path = options.path;
  const profile = options.profileName ?? TELEGRAM_DEFAULT_PROFILE_NAME;
  if (!path) throw new Error("Telegram update journal path is required.");
  if (!profile) throw new Error("Telegram update journal profile is required.");
  const expectedIdentity = validateBotIdentity(options.botIdentity, path);
  const queueRuntimeIdentity = options.queueRuntimeIdentity;
  if (
    queueRuntimeIdentity !== undefined &&
    (!isBoundedString(
      queueRuntimeIdentity.instanceId,
      TELEGRAM_UPDATE_JOURNAL_QUEUE_OWNER_ID_MAX_LENGTH,
    ) ||
      !isSafePositiveInteger(queueRuntimeIdentity.processId) ||
      !isBoundedString(
        queueRuntimeIdentity.processBirthId,
        TELEGRAM_UPDATE_JOURNAL_QUEUE_OWNER_ID_MAX_LENGTH,
      ))
  ) {
    throw new Error(
      "Telegram update journal queue process identity is invalid.",
    );
  }
  const maxEntries = normalizeCapacityLimit(
    options.maxEntries,
    TELEGRAM_UPDATE_JOURNAL_MAX_ENTRIES,
    "entry limit",
  );
  const maxBytes = normalizeCapacityLimit(
    options.maxBytes,
    TELEGRAM_UPDATE_JOURNAL_MAX_BYTES,
    "byte limit",
  );
  const getNowMs = options.getNowMs ?? Date.now;
  const onPublicationBoundary = options.onPublicationBoundary;
  const workspaceAdmission = options.workspaceAdmission;
  const notifyRecovery = (event: TelegramUpdateJournalRecoveryEvent): void => {
    try {
      options.onRecovery?.(event);
    } catch {
      // Recovery diagnostics must not break recovered journal authority.
    }
  };
  const getQueueProcessLiveness =
    options.getQueueProcessLiveness ?? getTelegramProcessLiveness;

  const validateQueueHandoffInput = (
    input: TelegramUpdateJournalQueueHandoffInput,
    operation: "offer" | "accept" | "cancel",
  ): {
    expectedOwner: TelegramUpdateJournalQueueOwner;
    recipientOwner: TelegramUpdateJournalQueueOwnerIdentity;
    requestedIds: Set<number>;
    handoffId: string;
  } => {
    if (
      (input.queueKind !== "prompt" && input.queueKind !== "control") ||
      !isNonEmptyString(input.receiptId) ||
      !Array.isArray(input.sourceUpdateIds) ||
      input.sourceUpdateIds.length === 0 ||
      !isBoundedString(
        input.handoffToken,
        TELEGRAM_UPDATE_JOURNAL_QUEUE_HANDOFF_TOKEN_MAX_LENGTH,
      ) ||
      input.handoffToken.length <
        TELEGRAM_UPDATE_JOURNAL_QUEUE_HANDOFF_TOKEN_MIN_LENGTH
    ) {
      throw createJournalError(
        "invalid",
        path,
        `received an invalid queue handoff ${operation}`,
      );
    }
    const expectedOwner = validateJournalQueueOwner(input.expectedOwner, path);
    const recipientOwner = validateJournalQueueOwnerIdentity(
      input.recipientOwner,
      path,
    );
    const runtimeIdentity = operation === "accept" ? recipientOwner : expectedOwner;
    if (
      queueRuntimeIdentity &&
      (runtimeIdentity.instanceId !== queueRuntimeIdentity.instanceId ||
        runtimeIdentity.processId !== queueRuntimeIdentity.processId ||
        runtimeIdentity.processBirthId !== queueRuntimeIdentity.processBirthId)
    ) {
      throw createJournalError(
        "conflict",
        path,
        operation === "accept"
          ? `cannot accept queue receipt ${input.receiptId} for another runtime`
          : `cannot ${operation} foreign queue receipt ${input.receiptId}`,
      );
    }
    const requestedIds = new Set<number>();
    for (const updateId of input.sourceUpdateIds) {
      if (!isSafeNonNegativeInteger(updateId) || requestedIds.has(updateId)) {
        throw createJournalError(
          "invalid",
          path,
          `received invalid queue handoff ${operation} update ids`,
        );
      }
      requestedIds.add(updateId);
    }
    return {
      expectedOwner,
      recipientOwner,
      requestedIds,
      handoffId: createTelegramUpdateQueueHandoffId({
        handoffToken: input.handoffToken,
        queueKind: input.queueKind,
        receiptId: input.receiptId,
        sourceUpdateIds: [...requestedIds],
        expectedOwner,
        recipientOwner,
      }),
    };
  };

  const getExactQueuedReceiptEntries = (
    current: ReadTelegramUpdateJournalResult,
    input: Pick<
      TelegramUpdateJournalQueueHandoffInput,
      "queueKind" | "receiptId"
    >,
    requestedIds: ReadonlySet<number>,
  ): TelegramUpdateJournalEntry[] => {
    const receiptEntries = current.file.entries.filter(
      (entry) => entry.queueReceiptId === input.receiptId,
    );
    if (
      receiptEntries.length !== requestedIds.size ||
      receiptEntries.some(
        (entry) =>
          !requestedIds.has(entry.updateId) ||
          entry.state !== "queued" ||
          entry.queueKind !== input.queueKind,
      )
    ) {
      throw createJournalError(
        "conflict",
        path,
        `cannot hand off stale queue receipt ${input.receiptId}`,
      );
    }
    return receiptEntries;
  };

  const assertCapacity = (
    file: TelegramUpdateJournalFile,
    serialized = serializeJournalFile(file),
  ): number => {
    if (file.entries.length > maxEntries) {
      throw createJournalError(
        "capacity",
        path,
        `exceeds its ${maxEntries}-entry limit`,
      );
    }
    if ((file.operatorDispositions?.length ?? 0) > maxEntries) {
      throw createJournalError(
        "capacity",
        path,
        `exceeds its ${maxEntries}-operator-disposition limit`,
      );
    }
    const serializedBytes = Buffer.byteLength(serialized);
    if (serializedBytes > maxBytes) {
      throw createJournalError(
        "capacity",
        path,
        `exceeds its ${maxBytes}-byte limit`,
      );
    }
    return serializedBytes;
  };

  const emptyFile = (): TelegramUpdateJournalFile => ({
    version: TELEGRAM_UPDATE_JOURNAL_VERSION,
    profile,
    botIdentity: { ...expectedIdentity },
    entries: [],
  });

  const readCurrentStrict = (): ReadTelegramUpdateJournalResult => {
    let source: string;
    let recoveringMissingSnapshot = false;
    try {
      const size = statSync(path).size;
      if (size > maxBytes) {
        throw createJournalError(
          "capacity",
          path,
          `exceeds its ${maxBytes}-byte limit`,
        );
      }
      source = readFileSync(path, "utf8");
    } catch (error) {
      if (error instanceof TelegramUpdateJournalError) throw error;
      if ((error as { code?: unknown })?.code === "ENOENT") {
        const segmentDirectory = getTelegramUpdateJournalSegmentDirectory(path);
        let orphanedSegmentNames: string[];
        try {
          orphanedSegmentNames = readdirSync(segmentDirectory).filter((name) =>
            /^\d{16}\.json$/u.test(name),
          );
        } catch (segmentError) {
          if ((segmentError as { code?: unknown })?.code === "ENOENT") {
            return { file: emptyFile(), exists: false, serializedBytes: 0 };
          }
          throw createJournalError(
            "io",
            segmentDirectory,
            "could not be read while the journal snapshot is missing",
            segmentError,
          );
        }
        if (orphanedSegmentNames.length > 0) {
          recoveringMissingSnapshot = true;
          source = serializeJournalFile(emptyFile());
        } else {
          return { file: emptyFile(), exists: false, serializedBytes: 0 };
        }
      } else {
        throw createJournalError("io", path, "could not be read", error);
      }
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(source) as unknown;
    } catch (error) {
      throw createJournalError("invalid", path, "contains invalid JSON", error);
    }
    let file = parseJournalFile(parsed, path);
    const storedProfile = file.profile;
    const storedIdentity = file.botIdentity;
    const segmentDirectory = getTelegramUpdateJournalSegmentDirectory(path);
    let segmentNames: string[] = [];
    try {
      segmentNames = readdirSync(segmentDirectory).filter((name) =>
        /^\d{16}\.json$/u.test(name),
      );
    } catch (error) {
      if ((error as { code?: unknown })?.code !== "ENOENT") {
        throw createJournalError("io", segmentDirectory, "could not be read", error);
      }
    }
    segmentNames.sort();
    let revision = file.revision ?? 0;
    let unappliedSegmentBytes = 0;
    let orphanRecoverySawUpsert = false;
    let orphanRecoverySawBaseRemoval = false;
    let orphanRecoveryUnsafe = false;
    for (const name of segmentNames) {
      const nameRevision = Number(name.slice(0, 16));
      if (nameRevision <= revision) continue;
      const segmentPath = join(segmentDirectory, name);
      let segment: TelegramUpdateJournalSegment;
      try {
        const segmentSize = statSync(segmentPath).size;
        if (segmentSize > maxBytes) {
          throw createJournalError(
            "capacity",
            segmentPath,
            `exceeds its ${maxBytes}-byte limit`,
          );
        }
        unappliedSegmentBytes += segmentSize;
        if (unappliedSegmentBytes > maxBytes) {
          throw createJournalError(
            "capacity",
            segmentDirectory,
            `exceeds its ${maxBytes}-byte unapplied-segment limit`,
          );
        }
        segment = parseJournalSegment(
          JSON.parse(readFileSync(segmentPath, "utf8")) as unknown,
          segmentPath,
        );
      } catch (error) {
        if (error instanceof TelegramUpdateJournalError) throw error;
        throw createJournalError(
          "invalid",
          segmentPath,
          "contains invalid JSON",
          error,
        );
      }
      if (segment.revision !== nameRevision) {
        throw createJournalError(
          "invalid",
          segmentPath,
          "revision does not match its file name",
        );
      }
      if (segment.previousRevision !== revision) {
        throw createJournalError(
          "invalid",
          segmentPath,
          `has a revision gap after ${revision}`,
        );
      }
      if (
        segment.acceptedThroughUpdateId !== undefined &&
        file.acceptedThroughUpdateId !== undefined &&
        segment.acceptedThroughUpdateId < file.acceptedThroughUpdateId
      ) {
        throw createJournalError(
          "invalid",
          segmentPath,
          "regresses the admission cursor",
        );
      }
      if (
        segment.profile !== storedProfile ||
        !identitiesMatch(segment.botIdentity, storedIdentity)
      ) {
        throw createJournalError(
          "identity-mismatch",
          segmentPath,
          "belongs to another journal identity",
        );
      }
      const entriesById = new Map(
        file.entries.map((entry) => [entry.updateId, entry]),
      );
      for (const updateId of segment.removedUpdateIds) {
        if (recoveringMissingSnapshot && !entriesById.has(updateId)) {
          orphanRecoverySawBaseRemoval = true;
          if (orphanRecoverySawUpsert) orphanRecoveryUnsafe = true;
        }
        entriesById.delete(updateId);
      }
      for (const entry of segment.upsertedEntries) {
        entriesById.set(entry.updateId, entry);
      }
      if (segment.upsertedEntries.length > 0) orphanRecoverySawUpsert = true;
      file = parseJournalFile(
        {
          version: TELEGRAM_UPDATE_JOURNAL_VERSION,
          revision: segment.revision,
          ...(segment.acceptedThroughUpdateId !== undefined
            ? { acceptedThroughUpdateId: segment.acceptedThroughUpdateId }
            : file.acceptedThroughUpdateId !== undefined
              ? { acceptedThroughUpdateId: file.acceptedThroughUpdateId }
              : {}),
          profile: storedProfile,
          botIdentity: mergeBotIdentity(file.botIdentity, segment.botIdentity),
          entries: [...entriesById.values()].sort(
            (left, right) => left.updateId - right.updateId,
          ),
          ...(segment.operatorDispositions !== undefined
            ? { operatorDispositions: segment.operatorDispositions }
            : file.operatorDispositions?.length
              ? { operatorDispositions: file.operatorDispositions }
              : {}),
        },
        segmentPath,
      );
      revision = segment.revision;
    }
    const identityChanged =
      storedProfile !== profile ||
      !identitiesMatch(file.botIdentity, expectedIdentity);
    if (identityChanged) {
      if (file.entries.length > 0) {
        throw createJournalError(
          "identity-mismatch",
          path,
          storedProfile !== profile
            ? `belongs to profile ${storedProfile}, not ${profile}`
            : "belongs to another Telegram bot identity",
        );
      }
      file = {
        version: TELEGRAM_UPDATE_JOURNAL_VERSION,
        ...(file.revision !== undefined ? { revision: file.revision } : {}),
        profile,
        botIdentity: { ...expectedIdentity },
        entries: [],
      };
      const rebound = serializeJournalFile(file);
      const reboundBytes = assertCapacity(file, rebound);
      writeJournalFile(path, rebound, onPublicationBoundary);
      for (const name of segmentNames) {
        if (Number(name.slice(0, 16)) <= revision) {
          try {
            unlinkSync(join(segmentDirectory, name));
          } catch {
            // The rebound snapshot owns the current revision and no old
            // identity authority; redundant cleanup remains best-effort.
          }
        }
      }
      try {
        rmdirSync(segmentDirectory);
      } catch {
        // Redundant old segments are ignored at or below the snapshot revision.
      }
      return { file, exists: true, serializedBytes: reboundBytes };
    }
    if (recoveringMissingSnapshot) {
      if (
        orphanRecoveryUnsafe ||
        !orphanRecoverySawBaseRemoval ||
        file.entries.length > 0
      ) {
        throw createJournalError(
          "invalid",
          path,
          `is missing while ${segmentDirectory} retains revision segments`,
        );
      }
      const recovered = serializeJournalFile(file);
      const recoveredBytes = assertCapacity(file, recovered);
      writeJournalFile(path, recovered, onPublicationBoundary);
      notifyRecovery({
        kind: "repaired",
        path,
        revision: file.revision,
        reason: "Recovered a missing snapshot from a complete empty segment history.",
      });
      return { file, exists: true, serializedBytes: recoveredBytes };
    }
    const serializedBytes = assertCapacity(file);
    return { file, exists: true, serializedBytes };
  };

  const readCurrent = (): ReadTelegramUpdateJournalResult => {
    try {
      return readCurrentStrict();
    } catch (error) {
      if (
        !(error instanceof TelegramUpdateJournalError) ||
        error.code !== "invalid"
      ) {
        throw error;
      }
      let snapshotExists = false;
      try {
        statSync(path);
        snapshotExists = true;
      } catch (snapshotError) {
        if ((snapshotError as { code?: unknown })?.code !== "ENOENT") throw error;
      }
      const segmentDirectory = getTelegramUpdateJournalSegmentDirectory(path);
      let segmentNames: string[];
      try {
        segmentNames = readdirSync(segmentDirectory)
          .filter((name) => /^\d{16}\.json$/u.test(name))
          .sort();
      } catch {
        throw error;
      }
      if (segmentNames.length === 0) throw error;

      if (snapshotExists) {
        try {
          const snapshot = parseJournalFile(
            JSON.parse(readFileSync(path, "utf8")) as unknown,
            path,
          );
          const firstSegmentPath = join(segmentDirectory, segmentNames[0]);
          const firstSegment = parseJournalSegment(
            JSON.parse(readFileSync(firstSegmentPath, "utf8")) as unknown,
            firstSegmentPath,
          );
          if (
            snapshot.revision === undefined &&
            firstSegment.previousRevision > 0 &&
            snapshot.profile === firstSegment.profile &&
            identitiesMatch(snapshot.botIdentity, firstSegment.botIdentity)
          ) {
            writeJournalFile(
              path,
              serializeJournalFile({
                ...snapshot,
                revision: firstSegment.previousRevision,
              }),
              onPublicationBoundary,
            );
            const repaired = readCurrentStrict();
            notifyRecovery({
              kind: "repaired",
              path,
              revision: repaired.file.revision,
              reason: `Recovered a revisionless snapshot from segment revision ${firstSegment.revision}.`,
            });
            return repaired;
          }
        } catch {
          // Fall through to evidence-preserving quarantine and reset.
        }
      }

      const recoveryDirectory = join(
        dirname(path),
        "recovery",
        `${getNowMs()}-${process.pid}-${randomUUID()}`,
      );
      mkdirSync(recoveryDirectory, { recursive: true, mode: 0o700 });
      const snapshotQuarantinePath = join(recoveryDirectory, basename(path));
      const segmentQuarantinePath = join(
        recoveryDirectory,
        basename(segmentDirectory),
      );
      if (
        snapshotExists &&
        !renameTelegramPathWithRetry(path, snapshotQuarantinePath)
      ) {
        throw createJournalError(
          "io",
          path,
          "could not quarantine an unrecoverable journal snapshot",
          error,
        );
      }
      if (!renameTelegramPathWithRetry(segmentDirectory, segmentQuarantinePath)) {
        if (snapshotExists) {
          renameTelegramPathWithRetry(snapshotQuarantinePath, path);
        }
        throw createJournalError(
          "io",
          segmentDirectory,
          "could not quarantine an unrecoverable journal segment history",
          error,
        );
      }
      const reset = emptyFile();
      const serialized = serializeJournalFile(reset);
      const serializedBytes = assertCapacity(reset, serialized);
      try {
        writeJournalFile(path, serialized, onPublicationBoundary);
      } catch (publicationError) {
        const segmentsRestored = renameTelegramPathWithRetry(
          segmentQuarantinePath,
          segmentDirectory,
        );
        const snapshotRestored =
          !snapshotExists ||
          renameTelegramPathWithRetry(snapshotQuarantinePath, path);
        if (!segmentsRestored || !snapshotRestored) {
          throw createJournalError(
            "io",
            path,
            "reset publication failed after journal evidence was quarantined",
            publicationError,
          );
        }
        throw publicationError;
      }
      notifyRecovery({
        kind: "reset",
        path,
        quarantinePath: recoveryDirectory,
        reason: error.message,
      });
      return { file: reset, exists: true, serializedBytes };
    }
  };

  const runMutation = <T>(operation: () => T): T => {
    try {
      return withTelegramFileTransaction(`${path}.transaction`, operation);
    } catch (error) {
      if (error instanceof TelegramUpdateJournalError) throw error;
      throw createJournalError("io", path, "mutation failed", error);
    }
  };

  const publishMutation = (
    current: ReadTelegramUpdateJournalResult,
    entries: TelegramUpdateJournalEntry[],
    contentChanged: boolean,
    operatorDispositions = current.file.operatorDispositions,
    acceptedThroughUpdateId = current.file.acceptedThroughUpdateId,
  ): { file: TelegramUpdateJournalFile; serializedBytes: number } => {
    const botIdentity = mergeBotIdentity(
      current.file.botIdentity,
      expectedIdentity,
    );
    if (
      !contentChanged &&
      isDeepStrictEqual(current.file.botIdentity, botIdentity) &&
      isDeepStrictEqual(
        current.file.operatorDispositions ?? [],
        operatorDispositions ?? [],
      ) &&
      current.file.acceptedThroughUpdateId === acceptedThroughUpdateId
    ) {
      return { file: current.file, serializedBytes: current.serializedBytes };
    }
    const file: TelegramUpdateJournalFile = {
      version: TELEGRAM_UPDATE_JOURNAL_VERSION,
      profile,
      botIdentity,
      entries,
      ...(acceptedThroughUpdateId !== undefined
        ? { acceptedThroughUpdateId }
        : {}),
      ...(operatorDispositions?.length
        ? { operatorDispositions }
        : {}),
    };
    const serialized = serializeJournalFile(file);
    const serializedBytes = assertCapacity(file, serialized);
    const changed =
      contentChanged ||
      current.file.acceptedThroughUpdateId !== acceptedThroughUpdateId ||
      !isDeepStrictEqual(current.file.botIdentity, file.botIdentity);
    if (!changed) {
      return { file, serializedBytes: current.serializedBytes };
    }
    if (!current.exists) {
      writeJournalFile(path, serialized, onPublicationBoundary);
      return { file, serializedBytes };
    }
    const previousEntries = new Map(
      current.file.entries.map((entry) => [entry.updateId, entry]),
    );
    const nextEntries = new Map(entries.map((entry) => [entry.updateId, entry]));
    const upsertedEntries = entries.filter(
      (entry) =>
        !previousEntries.has(entry.updateId) ||
        !isDeepStrictEqual(previousEntries.get(entry.updateId), entry),
    );
    const removedUpdateIds = current.file.entries
      .filter((entry) => !nextEntries.has(entry.updateId))
      .map((entry) => entry.updateId);
    const revision = (current.file.revision ?? 0) + 1;
    publishTelegramUpdateJournalSegmentUnlocked(path, {
      version: TELEGRAM_UPDATE_JOURNAL_VERSION,
      revision,
      previousRevision: revision - 1,
      profile,
      botIdentity: file.botIdentity,
      upsertedEntries,
      removedUpdateIds,
      ...(acceptedThroughUpdateId !== undefined
        ? { acceptedThroughUpdateId }
        : {}),
      ...(!isDeepStrictEqual(
        current.file.operatorDispositions ?? [],
        operatorDispositions ?? [],
      )
        ? { operatorDispositions: operatorDispositions ?? [] }
        : {}),
    }, onPublicationBoundary);
    const revisedFile: TelegramUpdateJournalFile = { ...file, revision };
    const segmentDirectory = getTelegramUpdateJournalSegmentDirectory(path);
    const segmentNames = readdirSync(segmentDirectory).filter((name) =>
      /^\d{16}\.json$/u.test(name),
    );
    let snapshotRevision = 0;
    try {
      snapshotRevision = parseJournalFile(
        JSON.parse(readFileSync(path, "utf8")) as unknown,
        path,
      ).revision ?? 0;
    } catch (error) {
      if ((error as { code?: unknown })?.code !== "ENOENT") throw error;
    }
    const unappliedSegmentNames = segmentNames.filter(
      (name) => Number(name.slice(0, 16)) > snapshotRevision,
    );
    const segmentBytes = unappliedSegmentNames.reduce(
      (total, name) => total + statSync(join(segmentDirectory, name)).size,
      0,
    );
    if (
      unappliedSegmentNames.length >=
        TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_COUNT ||
      segmentBytes >= TELEGRAM_UPDATE_JOURNAL_COMPACTION_SEGMENT_BYTES
    ) {
      const compacted = serializeJournalFile(revisedFile);
      const compactedBytes = assertCapacity(revisedFile, compacted);
      writeJournalFile(path, compacted, onPublicationBoundary);
      for (const name of segmentNames) {
        if (Number(name.slice(0, 16)) <= revision) {
          try {
            unlinkSync(join(segmentDirectory, name));
          } catch {
            // The published snapshot already owns this revision; redundant
            // segments are safe and will be ignored on reconstruction.
          }
        }
      }
      try {
        rmdirSync(segmentDirectory);
      } catch {
        // Interrupted cleanup may leave an empty directory or old segments.
      }
      return { file: revisedFile, serializedBytes: compactedBytes };
    }
    return { file: revisedFile, serializedBytes };
  };

  return {
    read() {
      return runMutation(() => {
        const current = readCurrent();
        return {
          ...cloneFile(current.file),
          exists: current.exists,
          serializedBytes: current.serializedBytes,
        };
      });
    },
    appendBatch(updates, requestedAcceptedThroughUpdateId) {
      const append = () => runMutation(() => {
        if (
          requestedAcceptedThroughUpdateId !== undefined &&
          !isSafeNonNegativeInteger(requestedAcceptedThroughUpdateId)
        ) {
          throw createJournalError(
            "invalid",
            path,
            "received an invalid admission cursor",
          );
        }
        const current = readCurrent();
        const normalizedUpdates: TelegramJournaledUpdate[] = [];
        for (const update of updates) {
          const normalized = normalizeIncomingJournaledUpdate(update, path);
          const previous = normalizedUpdates.at(-1);
          if (previous && normalized.update_id < previous.update_id) {
            throw createJournalError(
              "invalid",
              path,
              "received an unordered update batch",
            );
          }
          if (previous?.update_id === normalized.update_id) {
            if (!isDeepStrictEqual(previous, normalized)) {
              throw createJournalError(
                "conflict",
                path,
                `received conflicting update ${normalized.update_id}`,
              );
            }
            continue;
          }
          normalizedUpdates.push(normalized);
        }

        const entriesById = new Map(
          current.file.entries.map((entry) => [entry.updateId, entry]),
        );
        const previousAcceptedThroughUpdateId =
          current.file.acceptedThroughUpdateId;
        const discardedUpdateIds = new Set(
          (current.file.operatorDispositions ?? [])
            .filter((disposition) => disposition.action === "discard")
            .map((disposition) => disposition.updateId),
        );
        let admittedAtMs: number | undefined;
        const addedUpdateIds: number[] = [];
        const duplicateUpdateIds: number[] = [];
        for (const update of normalizedUpdates) {
          if (discardedUpdateIds.has(update.update_id)) {
            duplicateUpdateIds.push(update.update_id);
            continue;
          }
          const existing = entriesById.get(update.update_id);
          if (existing) {
            if (!isDeepStrictEqual(existing.update, update)) {
              throw createJournalError(
                "conflict",
                path,
                `received conflicting update ${update.update_id}`,
              );
            }
            duplicateUpdateIds.push(update.update_id);
            continue;
          }
          if (admittedAtMs === undefined) {
            admittedAtMs = getNowMs();
            if (!isSafeNonNegativeInteger(admittedAtMs)) {
              throw createJournalError(
                "invalid",
                path,
                "received an invalid admission timestamp",
              );
            }
          }
          const entry: TelegramUpdateJournalEntry = {
            updateId: update.update_id,
            update,
            admittedAtMs,
            state: "pending",
          };
          entriesById.set(entry.updateId, entry);
          addedUpdateIds.push(entry.updateId);
        }

        const batchLastUpdateId = normalizedUpdates.at(-1)?.update_id;
        if (
          requestedAcceptedThroughUpdateId !== undefined &&
          batchLastUpdateId !== undefined &&
          requestedAcceptedThroughUpdateId < batchLastUpdateId
        ) {
          throw createJournalError(
            "invalid",
            path,
            "received an admission cursor behind its batch",
          );
        }
        if (
          requestedAcceptedThroughUpdateId !== undefined &&
          previousAcceptedThroughUpdateId !== undefined &&
          requestedAcceptedThroughUpdateId < previousAcceptedThroughUpdateId
        ) {
          throw createJournalError(
            "conflict",
            path,
            "received a regressing admission cursor",
          );
        }
        const acceptedThroughUpdateId =
          requestedAcceptedThroughUpdateId ?? previousAcceptedThroughUpdateId;
        const contentChanged = addedUpdateIds.length > 0;
        const published = publishMutation(
          current,
          contentChanged
            ? Array.from(entriesById.values()).sort(
                (left, right) => left.updateId - right.updateId,
              )
            : current.file.entries,
          contentChanged,
          current.file.operatorDispositions,
          acceptedThroughUpdateId,
        );
        return {
          addedUpdateIds,
          duplicateUpdateIds,
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
      if (!workspaceAdmission) return append();
      const operationHash = createHash("sha256").update(path).update("\0");
      for (const update of updates) {
        operationHash.update(String(update.update_id)).update("\0");
      }
      operationHash.update(String(requestedAcceptedThroughUpdateId ?? "none"));
      return runWithTelegramWorkspaceAdmissions({
        ledger: workspaceAdmission,
        operationId: `journal:${operationHash.digest("hex")}`,
        operationKind: "journal.append",
        scopes: getTelegramUpdateJournalAdmissionScopes(
          updates as readonly TelegramJournaledUpdate[],
        ),
        operation: append,
      });
    },
    markQueued(receipt) {
      return runMutation(() => {
        if (
          (receipt.queueKind !== "prompt" &&
            receipt.queueKind !== "control") ||
          !isNonEmptyString(receipt.receiptId) ||
          !Array.isArray(receipt.sourceUpdateIds) ||
          receipt.sourceUpdateIds.length === 0
        ) {
          throw createJournalError(
            "invalid",
            path,
            "received an invalid queue receipt",
          );
        }
        const requestedOwner = validateJournalQueueOwnerIdentity(
          receipt.owner,
          path,
        );
        if (
          queueRuntimeIdentity &&
          (requestedOwner.instanceId !== queueRuntimeIdentity.instanceId ||
            requestedOwner.processId !== queueRuntimeIdentity.processId ||
            requestedOwner.processBirthId !==
              queueRuntimeIdentity.processBirthId)
        ) {
          throw createJournalError(
            "conflict",
            path,
            "cannot acquire a queue receipt for another runtime process generation",
          );
        }
        const requestedIds = new Set<number>();
        for (const updateId of receipt.sourceUpdateIds) {
          if (
            !isSafeNonNegativeInteger(updateId) ||
            requestedIds.has(updateId)
          ) {
            throw createJournalError(
              "invalid",
              path,
              "received invalid or duplicate queue receipt update ids",
            );
          }
          requestedIds.add(updateId);
        }
        const current = readCurrent();
        const existingReceiptEntries: TelegramUpdateJournalEntry[] = [];
        const entriesById = new Map<number, TelegramUpdateJournalEntry>();
        for (const entry of current.file.entries) {
          entriesById.set(entry.updateId, entry);
          if (entry.queueReceiptId === receipt.receiptId) {
            existingReceiptEntries.push(entry);
          }
        }
        const existingReceiptIds = new Set(
          existingReceiptEntries.map((entry) => entry.updateId),
        );
        if (
          existingReceiptIds.size > 0 &&
          (existingReceiptIds.size !== requestedIds.size ||
            [...existingReceiptIds].some(
              (updateId) => !requestedIds.has(updateId),
            ))
        ) {
          throw createJournalError(
            "conflict",
            path,
            `has a conflicting queue receipt ${receipt.receiptId}`,
          );
        }
        let queueOwner = existingReceiptEntries[0]?.queueOwner;
        if (existingReceiptEntries.length === 0) {
          const acquiredAtMs = getNowMs();
          if (!isSafeNonNegativeInteger(acquiredAtMs)) {
            throw createJournalError(
              "invalid",
              path,
              "received an invalid queue acquisition timestamp",
            );
          }
          queueOwner = {
            ...requestedOwner,
            acquisitionId: randomUUID(),
            acquiredAtMs,
          };
        }
        const queuedUpdateIds: number[] = [];
        const duplicateUpdateIds: number[] = [];
        for (const updateId of receipt.sourceUpdateIds) {
          const entry = entriesById.get(updateId);
          if (!entry) {
            throw createJournalError(
              "conflict",
              path,
              `cannot queue missing update ${updateId}`,
            );
          }
          if (entry.state === "queued") {
            if (
              entry.queueKind !== receipt.queueKind ||
              entry.queueReceiptId !== receipt.receiptId
            ) {
              throw createJournalError(
                "conflict",
                path,
                `update ${updateId} belongs to another queue receipt`,
              );
            }
            duplicateUpdateIds.push(updateId);
            continue;
          }
          if (entry.state === "failed") {
            throw createJournalError(
              "conflict",
              path,
              `cannot queue failed update ${updateId}`,
            );
          }
          entriesById.set(updateId, {
            updateId: entry.updateId,
            update: entry.update,
            admittedAtMs: entry.admittedAtMs,
            state: "queued",
            queueKind: receipt.queueKind,
            queueReceiptId: receipt.receiptId,
            queueOwner: cloneJournalQueueOwner(queueOwner!),
          });
          queuedUpdateIds.push(updateId);
        }
        const contentChanged = queuedUpdateIds.length > 0;
        const published = publishMutation(
          current,
          contentChanged
            ? current.file.entries.map(
                (entry) => entriesById.get(entry.updateId)!,
              )
            : current.file.entries,
          contentChanged,
        );
        return {
          queuedUpdateIds,
          duplicateUpdateIds,
          ...(queueOwner
            ? { queueOwner: cloneJournalQueueOwner(queueOwner) }
            : {}),
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
    markExecutionFailure(input) {
      return runMutation(() => {
        if (
          !isSafeNonNegativeInteger(input.updateId) ||
          !isSafeNonNegativeInteger(input.expectedAttemptCount) ||
          !isSafeNonNegativeInteger(input.failedAtMs) ||
          !isBoundedString(
            input.failureClass,
            TELEGRAM_UPDATE_JOURNAL_FAILURE_CLASS_MAX_LENGTH,
          ) ||
          !isBoundedString(
            input.summary,
            TELEGRAM_UPDATE_JOURNAL_FAILURE_SUMMARY_MAX_LENGTH,
          ) ||
          (input.disposition !== "retry-wait" &&
            input.disposition !== "failed")
        ) {
          throw createJournalError(
            "invalid",
            path,
            "received invalid execution failure metadata",
          );
        }
        if (
          input.disposition === "retry-wait" &&
          (!isSafeNonNegativeInteger(input.nextRetryAtMs) ||
            input.nextRetryAtMs < input.failedAtMs ||
            input.terminalReason !== undefined)
        ) {
          throw createJournalError(
            "invalid",
            path,
            "received invalid retry-wait disposition",
          );
        }
        if (
          input.disposition === "failed" &&
          (!isBoundedString(
            input.terminalReason,
            TELEGRAM_UPDATE_JOURNAL_TERMINAL_REASON_MAX_LENGTH,
          ) ||
            input.nextRetryAtMs !== undefined)
        ) {
          throw createJournalError(
            "invalid",
            path,
            "received invalid terminal failure disposition",
          );
        }
        const current = readCurrent();
        const entry = current.file.entries.find(
          (candidate) => candidate.updateId === input.updateId,
        );
        if (!entry) {
          throw createJournalError(
            "conflict",
            path,
            `cannot fail missing update ${input.updateId}`,
          );
        }
        if (entry.state !== "pending" && entry.state !== "retry-wait") {
          throw createJournalError(
            "conflict",
            path,
            `cannot fail ${entry.state} update ${input.updateId}`,
          );
        }
        const previousAttemptCount = entry.failure?.attemptCount ?? 0;
        if (previousAttemptCount !== input.expectedAttemptCount) {
          throw createJournalError(
            "conflict",
            path,
            `update ${input.updateId} execution attempt changed`,
          );
        }
        const failure: TelegramUpdateJournalFailure = {
          attemptCount: previousAttemptCount + 1,
          failedAtMs: input.failedAtMs,
          failureClass: input.failureClass,
          summary: input.summary,
        };
        const terminalFailureId =
          input.disposition === "failed"
            ? createTelegramUpdateTerminalFailureId({
                updateId: entry.updateId,
                attemptCount: failure.attemptCount,
                failedAtMs: failure.failedAtMs,
                failureClass: failure.failureClass,
                terminalAtMs: input.failedAtMs,
                terminalReason: input.terminalReason!,
              })
            : undefined;
        const nextEntry: TelegramUpdateJournalEntry = {
          updateId: entry.updateId,
          update: entry.update,
          admittedAtMs: entry.admittedAtMs,
          state: input.disposition,
          failure,
          ...(input.disposition === "retry-wait"
            ? { nextRetryAtMs: input.nextRetryAtMs! }
            : {
                terminalAtMs: input.failedAtMs,
                terminalReason: input.terminalReason!,
                terminalFailureId: terminalFailureId!,
              }),
        };
        const published = publishMutation(
          current,
          current.file.entries.map((candidate) =>
            candidate.updateId === input.updateId ? nextEntry : candidate,
          ),
          true,
        );
        return {
          entry: cloneEntry(nextEntry),
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
    applyOperatorDisposition(input) {
      return runMutation(() => {
        if (
          !isSafeNonNegativeInteger(input.updateId) ||
          !isBoundedString(
            input.failureId,
            TELEGRAM_UPDATE_JOURNAL_FAILURE_ID_MAX_LENGTH,
          ) ||
          (input.action !== "retry" && input.action !== "discard")
        ) {
          throw createJournalError(
            "invalid",
            path,
            "received an invalid operator disposition",
          );
        }
        const current = readCurrent();
        const existingDisposition = current.file.operatorDispositions?.find(
          (candidate) => candidate.failureId === input.failureId,
        );
        if (existingDisposition) {
          if (
            existingDisposition.updateId !== input.updateId ||
            existingDisposition.action !== input.action
          ) {
            throw createJournalError(
              "conflict",
              path,
              `terminal failure ${input.failureId} already has another operator disposition`,
            );
          }
          return {
            disposition: { ...existingDisposition },
            duplicate: true,
            entryCount: current.file.entries.length,
            serializedBytes: current.serializedBytes,
          };
        }
        const entry = current.file.entries.find(
          (candidate) => candidate.updateId === input.updateId,
        );
        if (
          entry?.state !== "failed" ||
          !entry.failure ||
          entry.terminalAtMs === undefined ||
          !entry.terminalReason ||
          entry.terminalFailureId !== input.failureId
        ) {
          throw createJournalError(
            "conflict",
            path,
            `cannot apply a stale disposition to terminal failure ${input.failureId}`,
          );
        }
        const nowMs = getNowMs();
        if (!isSafeNonNegativeInteger(nowMs)) {
          throw createJournalError(
            "invalid",
            path,
            "received an invalid operator disposition timestamp",
          );
        }
        const disposition: TelegramUpdateJournalOperatorDisposition = {
          failureId: entry.terminalFailureId,
          updateId: entry.updateId,
          action: input.action,
          committedAtMs: Math.max(nowMs, entry.terminalAtMs),
          attemptCount: entry.failure.attemptCount,
          failureClass: entry.failure.failureClass,
          terminalAtMs: entry.terminalAtMs,
          terminalReason: entry.terminalReason,
        };
        const nextEntries =
          input.action === "retry"
            ? current.file.entries.map((candidate) =>
                candidate.updateId === entry.updateId
                  ? {
                      updateId: entry.updateId,
                      update: entry.update,
                      admittedAtMs: entry.admittedAtMs,
                      state: "retry-wait" as const,
                      failure: entry.failure,
                      nextRetryAtMs: disposition.committedAtMs,
                    }
                  : candidate,
              )
            : current.file.entries.filter(
                (candidate) => candidate.updateId !== entry.updateId,
              );
        const published = publishMutation(
          current,
          nextEntries,
          true,
          [...(current.file.operatorDispositions ?? []), disposition],
        );
        return {
          disposition: { ...disposition },
          duplicate: false,
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
    offerQueuedHandoff(input) {
      return runMutation(() => {
        const { expectedOwner, recipientOwner, requestedIds, handoffId } =
          validateQueueHandoffInput(input, "offer");
        if (isTelegramUpdateJournalQueueOwnerProcess(expectedOwner, recipientOwner)) {
          throw createJournalError(
            "conflict",
            path,
            `cannot hand queue receipt ${input.receiptId} to the same runtime process`,
          );
        }
        const current = readCurrent();
        const receiptEntries = getExactQueuedReceiptEntries(
          current,
          input,
          requestedIds,
        );
        if (
          receiptEntries.some(
            (entry) =>
              !entry.queueOwner ||
              !areTelegramUpdateJournalQueueOwnersEqual(
                entry.queueOwner,
                expectedOwner,
              ),
          )
        ) {
          throw createJournalError(
            "conflict",
            path,
            `cannot offer stale queue receipt ${input.receiptId}`,
          );
        }
        const existingHandoff = receiptEntries[0]?.queueHandoff;
        if (existingHandoff) {
          if (
            existingHandoff.handoffId !== handoffId ||
            !isDeepStrictEqual(existingHandoff.recipientOwner, recipientOwner)
          ) {
            throw createJournalError(
              "conflict",
              path,
              `queue receipt ${input.receiptId} already has another handoff offer`,
            );
          }
          return {
            handoff: cloneJournalQueueHandoff(existingHandoff),
            previousOwner: cloneJournalQueueOwner(expectedOwner),
            offeredUpdateIds: [...requestedIds].sort((a, b) => a - b),
            duplicate: true,
            entryCount: current.file.entries.length,
            serializedBytes: current.serializedBytes,
          };
        }
        const offeredAtMs = getNowMs();
        if (!isSafeNonNegativeInteger(offeredAtMs)) {
          throw createJournalError(
            "invalid",
            path,
            "received an invalid queue handoff offer timestamp",
          );
        }
        const handoff: TelegramUpdateJournalQueueHandoff = {
          handoffId,
          offeredAtMs,
          recipientOwner,
        };
        const published = publishMutation(
          current,
          current.file.entries.map((entry) =>
            requestedIds.has(entry.updateId)
              ? { ...entry, queueHandoff: cloneJournalQueueHandoff(handoff) }
              : entry,
          ),
          true,
        );
        return {
          handoff: cloneJournalQueueHandoff(handoff),
          previousOwner: cloneJournalQueueOwner(expectedOwner),
          offeredUpdateIds: [...requestedIds].sort((a, b) => a - b),
          duplicate: false,
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
    acceptQueuedHandoff(input) {
      return runMutation(() => {
        const { expectedOwner, recipientOwner, requestedIds, handoffId } =
          validateQueueHandoffInput(input, "accept");
        const current = readCurrent();
        const receiptEntries = getExactQueuedReceiptEntries(
          current,
          input,
          requestedIds,
        );
        const existingOwner = receiptEntries[0]?.queueOwner;
        if (
          existingOwner &&
          existingOwner.handoffId === handoffId &&
          isTelegramUpdateJournalQueueOwnerProcess(existingOwner, recipientOwner)
        ) {
          if (
            receiptEntries.some(
              (entry) =>
                !entry.queueOwner ||
                !areTelegramUpdateJournalQueueOwnersEqual(
                  entry.queueOwner,
                  existingOwner,
                ) ||
                entry.queueHandoff !== undefined,
            )
          ) {
            throw createJournalError(
              "conflict",
              path,
              `queue receipt ${input.receiptId} has inconsistent accepted handoff authority`,
            );
          }
          return {
            handoffId,
            queueOwner: cloneJournalQueueOwner(existingOwner),
            acceptedUpdateIds: [...requestedIds].sort((a, b) => a - b),
            duplicate: true,
            entryCount: current.file.entries.length,
            serializedBytes: current.serializedBytes,
          };
        }
        if (
          receiptEntries.some(
            (entry) =>
              !entry.queueOwner ||
              !areTelegramUpdateJournalQueueOwnersEqual(
                entry.queueOwner,
                expectedOwner,
              ) ||
              entry.queueHandoff?.handoffId !== handoffId ||
              !isDeepStrictEqual(
                entry.queueHandoff.recipientOwner,
                recipientOwner,
              ),
          )
        ) {
          throw createJournalError(
            "conflict",
            path,
            `cannot accept stale or unauthenticated queue handoff ${input.receiptId}`,
          );
        }
        const acquiredAtMs = getNowMs();
        if (!isSafeNonNegativeInteger(acquiredAtMs)) {
          throw createJournalError(
            "invalid",
            path,
            "received an invalid queue handoff acquisition timestamp",
          );
        }
        const queueOwner: TelegramUpdateJournalQueueOwner = {
          ...recipientOwner,
          acquisitionId: randomUUID(),
          acquiredAtMs,
          handoffId,
        };
        const published = publishMutation(
          current,
          current.file.entries.map((entry) =>
            requestedIds.has(entry.updateId)
              ? {
                  updateId: entry.updateId,
                  update: entry.update,
                  admittedAtMs: entry.admittedAtMs,
                  state: "queued" as const,
                  queueKind: input.queueKind,
                  queueReceiptId: input.receiptId,
                  queueOwner: cloneJournalQueueOwner(queueOwner),
                }
              : entry,
          ),
          true,
        );
        return {
          handoffId,
          previousOwner: cloneJournalQueueOwner(expectedOwner),
          queueOwner: cloneJournalQueueOwner(queueOwner),
          acceptedUpdateIds: [...requestedIds].sort((a, b) => a - b),
          duplicate: false,
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
    cancelQueuedHandoff(input) {
      return runMutation(() => {
        const { expectedOwner, recipientOwner, requestedIds, handoffId } =
          validateQueueHandoffInput(input, "cancel");
        const current = readCurrent();
        const receiptEntries = getExactQueuedReceiptEntries(
          current,
          input,
          requestedIds,
        );
        if (
          receiptEntries.some(
            (entry) =>
              !entry.queueOwner ||
              !areTelegramUpdateJournalQueueOwnersEqual(
                entry.queueOwner,
                expectedOwner,
              ),
          )
        ) {
          throw createJournalError(
            "conflict",
            path,
            `cannot cancel stale queue handoff ${input.receiptId}`,
          );
        }
        const existingHandoff = receiptEntries[0]?.queueHandoff;
        if (!existingHandoff) {
          throw createJournalError(
            "conflict",
            path,
            `cannot cancel missing queue handoff offer ${input.receiptId}`,
          );
        }
        if (
          existingHandoff.handoffId !== handoffId ||
          !isDeepStrictEqual(existingHandoff.recipientOwner, recipientOwner)
        ) {
          throw createJournalError(
            "conflict",
            path,
            `cannot cancel another queue handoff offer ${input.receiptId}`,
          );
        }
        const published = publishMutation(
          current,
          current.file.entries.map((entry) => {
            if (!requestedIds.has(entry.updateId)) return entry;
            const { queueHandoff: _queueHandoff, ...retained } = entry;
            return retained;
          }),
          true,
        );
        return {
          handoffId,
          previousOwner: cloneJournalQueueOwner(expectedOwner),
          cancelledUpdateIds: [...requestedIds].sort((a, b) => a - b),
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
    completeQueued(receipts) {
      return runMutation(() => {
        if (!Array.isArray(receipts) || receipts.length === 0) {
          throw createJournalError(
            "invalid",
            path,
            "received no queued receipts to complete",
          );
        }
        const current = readCurrent();
        const receiptIds = new Set<string>();
        const requestedUpdateIds = new Set<number>();
        for (const receipt of receipts) {
          if (
            (receipt.queueKind !== "prompt" &&
              receipt.queueKind !== "control") ||
            !isNonEmptyString(receipt.receiptId) ||
            receiptIds.has(receipt.receiptId) ||
            !Array.isArray(receipt.sourceUpdateIds) ||
            receipt.sourceUpdateIds.length === 0
          ) {
            throw createJournalError(
              "invalid",
              path,
              "received an invalid queued completion receipt",
            );
          }
          receiptIds.add(receipt.receiptId);
          const queueOwner = validateJournalQueueOwner(
            receipt.queueOwner,
            path,
          );
          if (
            queueRuntimeIdentity &&
            (queueOwner.instanceId !== queueRuntimeIdentity.instanceId ||
              queueOwner.processId !== queueRuntimeIdentity.processId ||
              queueOwner.processBirthId !==
                queueRuntimeIdentity.processBirthId)
          ) {
            throw createJournalError(
              "conflict",
              path,
              `cannot complete foreign queue receipt ${receipt.receiptId}`,
            );
          }
          const sourceUpdateIds = new Set<number>();
          for (const updateId of receipt.sourceUpdateIds) {
            if (
              !isSafeNonNegativeInteger(updateId) ||
              sourceUpdateIds.has(updateId) ||
              requestedUpdateIds.has(updateId)
            ) {
              throw createJournalError(
                "invalid",
                path,
                "received overlapping queued completion update ids",
              );
            }
            sourceUpdateIds.add(updateId);
            requestedUpdateIds.add(updateId);
          }
          const persistedReceiptEntries = current.file.entries.filter(
            (entry) => entry.queueReceiptId === receipt.receiptId,
          );
          if (
            persistedReceiptEntries.length !== sourceUpdateIds.size ||
            persistedReceiptEntries.some(
              (entry) =>
                !sourceUpdateIds.has(entry.updateId) ||
                entry.state !== "queued" ||
                entry.queueKind !== receipt.queueKind ||
                !entry.queueOwner ||
                entry.queueHandoff !== undefined ||
                !areTelegramUpdateJournalQueueOwnersEqual(
                  entry.queueOwner,
                  queueOwner,
                ),
            )
          ) {
            throw createJournalError(
              "conflict",
              path,
              `cannot complete stale or foreign queue receipt ${receipt.receiptId}`,
            );
          }
        }
        const removedUpdateIds = current.file.entries
          .filter((entry) => requestedUpdateIds.has(entry.updateId))
          .map((entry) => entry.updateId);
        if (removedUpdateIds.length !== requestedUpdateIds.size) {
          throw createJournalError(
            "conflict",
            path,
            "queued completion did not resolve every source update",
          );
        }
        const published = publishMutation(
          current,
          current.file.entries.filter(
            (entry) => !requestedUpdateIds.has(entry.updateId),
          ),
          true,
        );
        return {
          removedUpdateIds,
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
    discardQueued(input) {
      return runMutation(() => {
        if (
          (input.queueKind !== "prompt" &&
            input.queueKind !== "control") ||
          !isNonEmptyString(input.receiptId) ||
          !Array.isArray(input.sourceUpdateIds) ||
          input.sourceUpdateIds.length === 0
        ) {
          throw createJournalError(
            "invalid",
            path,
            "received an invalid queue discard",
          );
        }
        const expectedOwner = validateJournalQueueOwner(
          input.expectedOwner,
          path,
        );
        if (
          queueRuntimeIdentity &&
          (expectedOwner.instanceId !== queueRuntimeIdentity.instanceId ||
            expectedOwner.processId !== queueRuntimeIdentity.processId ||
            expectedOwner.processBirthId !==
              queueRuntimeIdentity.processBirthId)
        ) {
          throw createJournalError(
            "conflict",
            path,
            `cannot discard foreign queue receipt ${input.receiptId}`,
          );
        }
        const requestedIds = new Set<number>();
        for (const updateId of input.sourceUpdateIds) {
          if (
            !isSafeNonNegativeInteger(updateId) ||
            requestedIds.has(updateId)
          ) {
            throw createJournalError(
              "invalid",
              path,
              "received invalid queue discard update ids",
            );
          }
          requestedIds.add(updateId);
        }
        const current = readCurrent();
        const receiptEntries = current.file.entries.filter(
          (entry) => entry.queueReceiptId === input.receiptId,
        );
        if (
          receiptEntries.length !== requestedIds.size ||
          receiptEntries.some(
            (entry) =>
              !requestedIds.has(entry.updateId) ||
              entry.state !== "queued" ||
              entry.queueKind !== input.queueKind ||
              !entry.queueOwner ||
              entry.queueHandoff !== undefined ||
              !areTelegramUpdateJournalQueueOwnersEqual(
                entry.queueOwner,
                expectedOwner,
              ),
          )
        ) {
          throw createJournalError(
            "conflict",
            path,
            `cannot discard stale queue receipt ${input.receiptId}`,
          );
        }
        const removedUpdateIds = [...requestedIds].sort((a, b) => a - b);
        const published = publishMutation(
          current,
          current.file.entries.filter(
            (entry) => !requestedIds.has(entry.updateId),
          ),
          true,
        );
        return {
          previousOwner: cloneJournalQueueOwner(expectedOwner),
          removedUpdateIds,
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
    recoverDeadQueueOwner(input) {
      return runMutation(() => {
        if (!getQueueProcessLiveness) {
          throw createJournalError(
            "conflict",
            path,
            "cannot recover queue authority without a process-liveness proof",
          );
        }
        const deadOwner = validateJournalQueueOwner(input.deadOwner, path);
        let ownerLiveness: TelegramProcessLiveness;
        try {
          ownerLiveness = getQueueProcessLiveness({
            processId: deadOwner.processId,
            processBirthId: deadOwner.processBirthId,
          });
        } catch (error) {
          throw createJournalError(
            "io",
            path,
            "could not prove queued owner liveness",
            error,
          );
        }
        const recoveryOwner = validateJournalQueueOwnerIdentity(
          input.recoveryOwner,
          path,
        );
        if (
          queueRuntimeIdentity &&
          (recoveryOwner.instanceId !== queueRuntimeIdentity.instanceId ||
            recoveryOwner.processId !== queueRuntimeIdentity.processId ||
            recoveryOwner.processBirthId !==
              queueRuntimeIdentity.processBirthId)
        ) {
          throw createJournalError(
            "conflict",
            path,
            `cannot recover queue receipt ${input.receiptId} for another runtime`,
          );
        }
        const requestedIds = new Set<number>();
        for (const updateId of input.sourceUpdateIds) {
          if (
            !isSafeNonNegativeInteger(updateId) ||
            requestedIds.has(updateId)
          ) {
            throw createJournalError(
              "invalid",
              path,
              "received invalid queue recovery update ids",
            );
          }
          requestedIds.add(updateId);
        }
        const current = readCurrent();
        const receiptEntries = current.file.entries.filter(
          (entry) => entry.queueReceiptId === input.receiptId,
        );
        if (
          (input.queueKind !== "prompt" &&
            input.queueKind !== "control") ||
          !isNonEmptyString(input.receiptId) ||
          requestedIds.size === 0 ||
          receiptEntries.length !== requestedIds.size ||
          receiptEntries.some(
            (entry) =>
              !requestedIds.has(entry.updateId) ||
              entry.state !== "queued" ||
              entry.queueKind !== input.queueKind ||
              !entry.queueOwner ||
              entry.queueHandoff !== undefined ||
              !areTelegramUpdateJournalQueueOwnersEqual(
                entry.queueOwner,
                deadOwner,
              ),
          )
        ) {
          throw createJournalError(
            "conflict",
            path,
            `cannot recover stale queue receipt ${input.receiptId}`,
          );
        }
        if (ownerLiveness !== "dead") {
          return {
            status:
              ownerLiveness === "alive"
                ? ("owner-alive" as const)
                : ("owner-unverifiable" as const),
            previousOwner: cloneJournalQueueOwner(deadOwner),
            recoveredUpdateIds: [] as [],
            entryCount: current.file.entries.length,
            serializedBytes: current.serializedBytes,
          };
        }
        const recoveredUpdateIds = [...requestedIds].sort((a, b) => a - b);
        const published = publishMutation(
          current,
          current.file.entries.filter(
            (entry) => !requestedIds.has(entry.updateId),
          ),
          true,
        );
        return {
          status: "recovered" as const,
          previousOwner: cloneJournalQueueOwner(deadOwner),
          recoveredUpdateIds,
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
    removeCompleted(updateIds) {
      return runMutation(() => {
        const current = readCurrent();
        const requestedIds = new Set<number>();
        for (const updateId of updateIds) {
          if (!isSafeNonNegativeInteger(updateId)) {
            throw createJournalError(
              "invalid",
              path,
              "received an invalid removal update id",
            );
          }
          requestedIds.add(updateId);
        }
        const requestedEntries = current.file.entries.filter((entry) =>
          requestedIds.has(entry.updateId),
        );
        const protectedEntry = requestedEntries.find(
          (entry) => entry.state === "failed" || entry.state === "queued",
        );
        if (protectedEntry) {
          throw createJournalError(
            "conflict",
            path,
            protectedEntry.state === "failed"
              ? `cannot complete terminal update ${protectedEntry.updateId} without an operator disposition`
              : `cannot complete queued update ${protectedEntry.updateId} without its exact owner receipt`,
          );
        }
        const removedUpdateIds = requestedEntries.map(
          (entry) => entry.updateId,
        );
        const published = publishMutation(
          current,
          current.file.entries.filter(
            (entry) => !requestedIds.has(entry.updateId),
          ),
          removedUpdateIds.length > 0,
        );
        return {
          removedUpdateIds,
          entryCount: published.file.entries.length,
          serializedBytes: published.serializedBytes,
        };
      });
    },
  };
}
