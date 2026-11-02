/**
 * Regression tests for extension callback handlers
 * Covers prefix validation, reserved prefixes, payload extraction and unregistering
 */

import assert from "node:assert/strict";
import test from "node:test";

import { findTelegramCallbackHandler, registerTelegramCallbackHandler } from "../lib/commands.ts";

test("extension callback resolves by prefix and passes the payload", () => {
  const handler = () => {};
  const off = registerTelegramCallbackHandler({ prefix: "agentd", handler });
  try {
    const hit = findTelegramCallbackHandler("agentd:pub:fix-login-3f9a");
    assert.equal(hit?.handler, handler);
    assert.equal(hit?.payload, "pub:fix-login-3f9a");
    assert.equal(findTelegramCallbackHandler("other:x"), undefined);
    assert.equal(findTelegramCallbackHandler("agentd"), undefined);
    assert.equal(findTelegramCallbackHandler(undefined), undefined);
  } finally {
    off();
  }
  assert.equal(findTelegramCallbackHandler("agentd:pub:x"), undefined);
});

test("extension callback rejects invalid and reserved prefixes", () => {
  assert.throws(() => registerTelegramCallbackHandler({ prefix: "Bad", handler: () => {} }), /Invalid/);
  assert.throws(() => registerTelegramCallbackHandler({ prefix: "a", handler: () => {} }), /Invalid/);
  assert.throws(() => registerTelegramCallbackHandler({ prefix: "tgbtn", handler: () => {} }), /reserved/);
  assert.throws(() => registerTelegramCallbackHandler({ prefix: "compact", handler: () => {} }), /reserved/);
});

test("re-registering a prefix replaces the handler; the stale unregister is a no-op", () => {
  const first = () => {};
  const second = () => {};
  const offFirst = registerTelegramCallbackHandler({ prefix: "agentd", handler: first });
  const offSecond = registerTelegramCallbackHandler({ prefix: "agentd", handler: second });
  offFirst();
  assert.equal(findTelegramCallbackHandler("agentd:x")?.handler, second);
  offSecond();
  assert.equal(findTelegramCallbackHandler("agentd:x"), undefined);
});
