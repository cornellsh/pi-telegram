/**
 * Cross-instance Telegram agent message resolution and turn injection
 * Zones: multi-instance bus, inbound routing, operational delivery
 * Owns live target resolution, source attribution, same-target fencing, and synthetic turn construction; excludes Bot API delivery and bus transport.
 */

import type {
  TelegramBusAgentMessage,
  TelegramBusAgentTargetSelector,
  TelegramBusFollowerRegistry,
} from "./bus.ts";
import type { TelegramRoutedMessage } from "./routing.ts";
import type { TelegramTarget } from "./target.ts";
import * as Threads from "./threads.ts";
import { TELEGRAM_INTERNAL_AGENT_MESSAGE } from "./updates.ts";

export interface TelegramAgentMessageRuntimeDeps<TContext, TUpdate> {
  instanceId: string;
  getAllowedChatId: () => number | undefined;
  getLeaderTarget: () => TelegramTarget | undefined;
  getLeaderThreadName: () => string | undefined;
  followerRegistry: TelegramBusFollowerRegistry;
  getContext: () => TContext | undefined;
  handleUpdate: (update: TUpdate, ctx: TContext) => Promise<void>;
  getNowMs?: () => number;
}

export function createTelegramAgentMessageRuntime<TContext, TUpdate>(
  deps: TelegramAgentMessageRuntimeDeps<TContext, TUpdate>,
) {
  const getNowMs = deps.getNowMs ?? Date.now;
  const sameTarget = (left?: TelegramTarget, right?: TelegramTarget) =>
    left?.chatId === right?.chatId && left?.threadId === right?.threadId;
  const listLiveTargets = () => {
    const targets: Array<{
      target: TelegramTarget & { threadId: number };
      threadName?: string;
    }> = [];
    const leaderTarget = deps.getLeaderTarget();
    if (leaderTarget?.threadId) {
      targets.push({
        target: { chatId: leaderTarget.chatId, threadId: leaderTarget.threadId },
        threadName: deps.getLeaderThreadName(),
      });
    }
    for (const follower of deps.followerRegistry.list()) {
      if (!follower.target?.threadId) continue;
      targets.push({
        target: {
          chatId: follower.target.chatId,
          threadId: follower.target.threadId,
        },
        threadName: follower.threadName,
      });
    }
    return targets;
  };
  const resolveTarget = (
    selector: TelegramBusAgentTargetSelector,
    sourceTarget?: TelegramTarget,
  ): (TelegramTarget & { threadId: number }) | undefined => {
    const allowedChatId = deps.getAllowedChatId();
    const chatId = selector.chatId ?? allowedChatId;
    if (chatId === undefined || chatId !== allowedChatId) return undefined;
    const matches = listLiveTargets().filter((candidate) => {
      if (candidate.target.chatId !== chatId) return false;
      if (selector.threadId !== undefined) {
        return candidate.target.threadId === selector.threadId;
      }
      const requestedName = selector.threadName?.trim().toLocaleLowerCase();
      if (requestedName === undefined || requestedName.length === 0) return false;
      // The topic's display name is what turn labels show the model, so it is
      // what the model will ask for; the palette name keeps working too.
      const displayName = Threads.readTelegramTopicDisplayName(candidate.target.threadId);
      return (
        candidate.threadName?.trim().toLocaleLowerCase() === requestedName ||
        displayName?.toLocaleLowerCase() === requestedName
      );
    });
    if (matches.length !== 1) return undefined;
    return sameTarget(matches[0]!.target, sourceTarget)
      ? undefined
      : matches[0]!.target;
  };
  return {
    resolveTarget,
    async route(input: {
      sourceTarget?: TelegramTarget;
      sourceThreadName?: string;
      message: TelegramBusAgentMessage;
    }): Promise<void> {
      const target = resolveTarget(
        {
          chatId: input.message.target.chatId,
          threadId: input.message.target.threadId,
        },
        input.sourceTarget,
      );
      if (!target) throw new Error("Telegram agent target is no longer live.");
      const allowedChatId = deps.getAllowedChatId();
      const ctx = deps.getContext();
      if (allowedChatId === undefined || !ctx) {
        throw new Error("Telegram agent turn routing is unavailable.");
      }
      const sourceLabel =
        input.sourceThreadName
          ?.replace(/[\r\n\[\]]+/g, " ")
          .trim()
          .slice(0, 64) || "Pi";
      const message = {
        message_id: input.message.messageId,
        date: Math.floor(getNowMs() / 1000),
        chat: { id: target.chatId, type: "private" as const },
        from: { id: allowedChatId, is_bot: false, first_name: "Pi Agent" },
        message_thread_id: target.threadId,
        pi_telegram_agent_source_thread: sourceLabel,
        text: input.message.text,
      } as TelegramRoutedMessage;
      await deps.handleUpdate(
        { message, [TELEGRAM_INTERNAL_AGENT_MESSAGE]: true } as TUpdate,
        ctx,
      );
    },
  };
}
