/**
 * Auto-Connect Domain
 * Zones: telegram, pi agent
 *
 * Automatic Telegram connection for newly started pi sessions.
 *
 * Without this, every session has to be connected by hand with
 * /telegram-connect, which is exactly the step people forget. The symptom is
 * silent: the tmux window exists, the agent works, and nothing ever appears in
 * Telegram.
 *
 * Two properties matter and both are easy to get wrong.
 *
 * First, **auto-connect must be narrower than the manual command**. A person
 * typing /telegram-connect has decided; this code is guessing. So it refuses
 * every case where the answer is not obvious: non-interactive runs, missing
 * credentials, a session that is already connected. Guessing wrong does not
 * produce an error message, it produces a junk Telegram topic that someone has
 * to find and delete.
 *
 * Second, **a failure here must never take the session down with it**. Session
 * start is not a good place to throw. Telegram is reachable over the network,
 * and the network fails; the observed failure in this project was a string of
 * ETIMEDOUT against api.telegram.org. So the attempt is retried a bounded
 * number of times and then gives up loudly in the log, rather than retrying
 * forever or propagating.
 *
 * The decision is a pure function so every rule is testable without a session,
 * a socket or a network.
 */

/** Why auto-connect did not run. Each value is a distinct, loggable cause. */
export type TelegramAutoConnectSkipReason =
  | "disabled"
  | "non-interactive"
  | "no-bot-token"
  | "already-connected"
  | "already-attempted";

export type TelegramAutoConnectDecision =
  | { kind: "connect" }
  | { kind: "skip"; reason: TelegramAutoConnectSkipReason };

export interface TelegramAutoConnectInput {
  /** `autoConnect` in telegram.json. Absent means off. */
  enabled: boolean;
  /** ctx.mode. Only "tui" is a real human session. */
  mode: string;
  /** True when this process already holds a Telegram binding. */
  alreadyConnected: boolean;
  /** True when a bot token resolves for the active profile. */
  hasBotToken: boolean;
  /** True once this process has already made its one automatic attempt. */
  alreadyAttempted: boolean;
}

/**
 * Decide whether a session_start should connect to Telegram.
 *
 * Order matters only for which reason gets reported, not for correctness: the
 * rules are independent and any one of them is sufficient to refuse. They are
 * ordered cheapest and most explicit first, so the logged reason is the one a
 * person would consider the real cause.
 */
export function planTelegramAutoConnect(
  input: TelegramAutoConnectInput,
): TelegramAutoConnectDecision {
  if (!input.enabled) return { kind: "skip", reason: "disabled" };
  // Only an interactive terminal session is a session a human is sitting at.
  // "print", "json" and "rpc" cover one-shot runs, scripted invocations and
  // subagents, which start and exit in seconds. Connecting those creates a
  // Telegram topic per invocation and deletes it moments later, which is how
  // this bridge accumulated orphan threads before.
  if (input.mode !== "tui") return { kind: "skip", reason: "non-interactive" };
  // Never prompt. The manual command may open a setup dialog; doing that
  // unasked would block session start behind a modal nobody requested.
  if (!input.hasBotToken) return { kind: "skip", reason: "no-bot-token" };
  if (input.alreadyConnected)
    return { kind: "skip", reason: "already-connected" };
  // session_start also fires on reload, resume and fork. Reconnecting on each
  // would tear down and recreate the thread under a user mid-conversation, so
  // one process gets exactly one automatic attempt.
  if (input.alreadyAttempted)
    return { kind: "skip", reason: "already-attempted" };
  return { kind: "connect" };
}

/**
 * Backoff for the bounded retry, in milliseconds, one entry per attempt.
 *
 * The first attempt is immediate. The later ones exist for a transient network
 * failure at login time, not for a misconfiguration: a wrong token fails the
 * same way three times and then stops, which is the intended outcome.
 */
export const TELEGRAM_AUTO_CONNECT_BACKOFF_MS: readonly number[] = [
  0, 15_000, 60_000,
];

export interface TelegramAutoConnectRuntimeDeps {
  /** Runs the same path as /telegram-connect. Must resolve or reject. */
  connect: () => Promise<void>;
  /** True when the bridge is connected; checked before every attempt. */
  isConnected: () => boolean;
  sleep: (ms: number) => Promise<void>;
  recordRuntimeEvent?: (
    category: string,
    error: Error,
    details?: Record<string, unknown>,
  ) => void;
  backoffMs?: readonly number[];
}

/**
 * Run the bounded attempt sequence. Never rejects: a caller on the session
 * start path has nothing useful to do with the failure, and throwing there
 * would be worse than being disconnected.
 */
export async function runTelegramAutoConnect(
  deps: TelegramAutoConnectRuntimeDeps,
): Promise<"connected" | "gave-up" | "already-connected"> {
  const backoff = deps.backoffMs ?? TELEGRAM_AUTO_CONNECT_BACKOFF_MS;
  for (let attempt = 0; attempt < backoff.length; attempt += 1) {
    if (attempt > 0) await deps.sleep(backoff[attempt]!);
    // Re-checked every pass: the user may have connected by hand while we were
    // waiting out the backoff, and a second connect would replace their thread.
    if (deps.isConnected()) {
      return attempt === 0 ? "already-connected" : "connected";
    }
    try {
      await deps.connect();
      // connect() resolving is not proof of a binding; the command reports some
      // refusals as a normal return. Trust the observable state instead.
      if (deps.isConnected()) return "connected";
      deps.recordRuntimeEvent?.(
        "auto-connect",
        new Error("Telegram auto-connect finished without a binding"),
        { phase: "no-binding", attempt: attempt + 1 },
      );
    } catch (error) {
      deps.recordRuntimeEvent?.(
        "auto-connect",
        error instanceof Error ? error : new Error(String(error)),
        { phase: "attempt-failed", attempt: attempt + 1 },
      );
    }
  }
  deps.recordRuntimeEvent?.(
    "auto-connect",
    new Error("Telegram auto-connect gave up; run /telegram-connect by hand"),
    { phase: "gave-up", attempts: backoff.length },
  );
  return "gave-up";
}

/**
 * Holds the registered /telegram-connect handler so auto-connect can reuse it.
 *
 * The command is registered by one binding and needed by another, and the
 * entrypoint is a composition root that may not hold mutable state itself.
 * This is that one piece of state, named and owned in one place.
 */
export interface TelegramConnectCommandHolder {
  capture: (handler: TelegramConnectCommandHandler) => void;
  get: () => TelegramConnectCommandHandler | undefined;
}

export type TelegramConnectCommandHandler = (
  args: string,
  ctx: never,
) => Promise<void> | void;

export function createTelegramConnectCommandHolder(): TelegramConnectCommandHolder {
  let handler: TelegramConnectCommandHandler | undefined;
  return {
    capture(next) {
      handler = next;
    },
    get() {
      return handler;
    },
  };
}

export interface TelegramAutoConnectSessionRuntimeDeps {
  isAutoConnectEnabled: () => boolean;
  hasBotToken: () => boolean;
  isConnected: () => boolean;
  getConnectHandler: () => TelegramConnectCommandHandler | undefined;
  recordRuntimeEvent: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
  /** Overridable for tests. */
  sleep?: (ms: number) => Promise<void>;
  backoffMs?: readonly number[];
}

/**
 * Session-start behaviour, as a hook rather than its own `pi.on` listener.
 *
 * It must be appended to the bridge's existing session-start hook, not
 * registered as a second listener for the same event. Handler registries key
 * by event name, so a second `pi.on("session_start")` can silently replace the
 * bridge's own and take the whole session lifecycle with it.
 *
 * Running last also makes the already-connected check meaningful: by then a
 * restored binding is visible, so a reload does not reconnect over itself.
 */
export function createTelegramAutoConnectSessionRuntime<TContext>(
  deps: TelegramAutoConnectSessionRuntimeDeps,
): { onSessionStart: (mode: string, ctx: TContext) => void } {
  let attempted = false;
  const sleep =
    deps.sleep ??
    ((ms: number) =>
      new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, ms);
        // A pending backoff must never hold the process open.
        (timer as { unref?: () => void }).unref?.();
      }));
  return {
    onSessionStart(mode, ctx) {
      const decision = planTelegramAutoConnect({
        enabled: deps.isAutoConnectEnabled(),
        mode,
        alreadyConnected: deps.isConnected(),
        hasBotToken: deps.hasBotToken(),
        alreadyAttempted: attempted,
      });
      if (decision.kind === "skip") {
        // "disabled" is the configured default, not a problem to report.
        if (decision.reason !== "disabled") {
          deps.recordRuntimeEvent(
            "auto-connect",
            new Error(`Telegram auto-connect skipped: ${decision.reason}`),
            { phase: "skipped", reason: decision.reason },
          );
        }
        return;
      }
      attempted = true;
      const handler = deps.getConnectHandler();
      if (!handler) {
        deps.recordRuntimeEvent(
          "auto-connect",
          new Error("Telegram connect command was never registered"),
          { phase: "no-handler" },
        );
        return;
      }
      // Detached on purpose. Session start must not wait on the network, and
      // the bounded backoff runs for over a minute in the worst case.
      void runTelegramAutoConnect({
        connect: async () => {
          await handler("", ctx as never);
        },
        isConnected: deps.isConnected,
        sleep,
        backoffMs: deps.backoffMs,
        recordRuntimeEvent: deps.recordRuntimeEvent,
      });
    },
  };
}
