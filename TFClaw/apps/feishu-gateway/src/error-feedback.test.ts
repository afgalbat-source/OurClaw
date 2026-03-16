import test from "node:test";
import assert from "node:assert/strict";
import {
  buildGatewayErrorFeedback,
  formatGatewayErrorFeedback,
} from "./error-feedback.js";

test("classifies trusted requester identity failures", () => {
  const feedback = buildGatewayErrorFeedback("openclaw", "trusted requester identity unavailable");

  assert.equal(feedback.code, "IDENTITY_UNAVAILABLE");
  assert.match(feedback.title, /飞书身份/);
  assert.match(feedback.action, /requesterSenderId/);
});

test("classifies timeout failures with source label", () => {
  const feedback = buildGatewayErrorFeedback("openclaw", "openclaw run failed: request timeout after 600000ms");

  assert.equal(feedback.code, "REQUEST_TIMEOUT");
  assert.match(feedback.title, /OpenClaw/);
  assert.match(feedback.reason, /10 分钟/);
});

test("classifies permission failures", () => {
  const feedback = buildGatewayErrorFeedback("access", "not allowed");

  assert.equal(feedback.code, "PERMISSION_DENIED");
  assert.match(feedback.reason, /权限/);
});

test("formats config errors without leaking file paths", () => {
  const text = formatGatewayErrorFeedback(
    "openclaw",
    "failed to parse config file (/tmp/secret/openclaw.json): unexpected token",
  );

  assert.match(text, /\[CONFIG_INVALID\]/);
  assert.doesNotMatch(text, /\/tmp\/secret\/openclaw\.json/);
});

test("keeps safe details for invalid requests", () => {
  const feedback = buildGatewayErrorFeedback("gateway", "workspace path is required");

  assert.equal(feedback.code, "INVALID_REQUEST");
  assert.equal(feedback.detail, "workspace path is required");
});
