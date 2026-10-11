/**
 * Voice Edge-TTS Adapter
 * Zones: telegram, voice
 *
 * Concrete synthesis backend for the provider registry in lib/voice.ts.
 *
 * That registry existed with nothing registered into it: not this extension,
 * not any other installed one. So every voice reply met an empty provider list
 * and failed with "every voice synthesis provider failed", which is true and
 * useless, because there were zero of them.
 *
 * The damage is worse than a missing voice note. When a reply is planned as
 * voice the words move into voiceText and the markdown is blanked, so a failed
 * synthesis loses the message body entirely rather than falling back to text.
 * That makes every failure path here load-bearing, which is why this module
 * returns undefined rather than throwing on each of them.
 */

import { execFile } from "node:child_process";
import { existsSync, statSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

const run = promisify(execFile);

/** A hung synthesiser must not stall a reply indefinitely. */
export const TELEGRAM_EDGE_TTS_TIMEOUT_MS = 60_000;

/** Relative to the agent directory. */
export const TELEGRAM_EDGE_TTS_SCRIPT = "skills/edge-tts/scripts/say.mjs";

export interface TelegramEdgeTtsProviderDeps {
  agentDir: string;
  recordRuntimeEvent?: (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ) => void;
  /** Overridable for tests. */
  runScript?: (script: string, args: string[]) => Promise<void>;
  fileExists?: (path: string) => boolean;
  fileSize?: (path: string) => number;
  removeFile?: (path: string) => void;
  uniqueSuffix?: () => string;
}

export function createTelegramEdgeTtsVoiceSynthesisProvider(
  deps: TelegramEdgeTtsProviderDeps,
): (
  text: string,
  options?: { lang?: string; rate?: string },
) => Promise<string | undefined> {
  const fileExists = deps.fileExists ?? existsSync;
  const fileSize = deps.fileSize ?? ((path: string) => statSync(path).size);
  const removeFile = deps.removeFile ?? unlinkSync;
  const uniqueSuffix =
    deps.uniqueSuffix ?? (() => `${Date.now()}-${process.pid}`);
  const runScript =
    deps.runScript ??
    (async (script: string, args: string[]) => {
      await run("node", [script, ...args], {
        timeout: TELEGRAM_EDGE_TTS_TIMEOUT_MS,
      });
    });

  return async function synthesize(text, options) {
    const script = join(deps.agentDir, TELEGRAM_EDGE_TTS_SCRIPT);
    // Reported rather than attempted: exec would fail with ENOENT, which reads
    // as a broken synthesiser instead of an uninstalled one.
    if (!fileExists(script)) {
      deps.recordRuntimeEvent?.(
        "voice",
        new Error(`edge-tts script not found at ${script}`),
        { phase: "edge-tts-missing" },
      );
      return undefined;
    }
    const out = join(tmpdir(), `pi-telegram-voice-${uniqueSuffix()}.mp3`);
    const args = ["--text", text, "--write-media", out];
    if (options?.lang) args.push("--lang", options.lang);
    if (options?.rate) args.push("--rate", options.rate);
    try {
      await runScript(script, args);
      // Exit zero is not proof of audio. A truncated or empty file sent as a
      // voice note is worse than no voice note, because it looks delivered.
      if (!fileExists(out) || fileSize(out) <= 0) {
        deps.recordRuntimeEvent?.(
          "voice",
          new Error("edge-tts produced no audio"),
          { phase: "edge-tts-empty" },
        );
        discard(out, fileExists, removeFile);
        return undefined;
      }
      return out;
    } catch (error) {
      deps.recordRuntimeEvent?.("voice", error, { phase: "edge-tts-failed" });
      discard(out, fileExists, removeFile);
      return undefined;
    }
  };
}

/** Best effort: a leftover temp file must never fail a reply. */
function discard(
  path: string,
  fileExists: (path: string) => boolean,
  removeFile: (path: string) => void,
): void {
  try {
    if (fileExists(path)) removeFile(path);
  } catch {
    // ignore
  }
}
