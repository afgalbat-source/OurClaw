export type GatewayErrorSource =
  | "openclaw"
  | "nexchatbot"
  | "gateway"
  | "access";

export type GatewayErrorCode =
  | "CONFIG_DISABLED"
  | "CONFIG_INVALID"
  | "IDENTITY_UNAVAILABLE"
  | "PERMISSION_DENIED"
  | "REQUEST_TIMEOUT"
  | "SERVICE_UNAVAILABLE"
  | "INVALID_REQUEST"
  | "INTERNAL_ERROR";

export interface GatewayErrorFeedback {
  code: GatewayErrorCode;
  title: string;
  reason: string;
  action: string;
  detail?: string;
  rawMessage: string;
}

const WRAPPED_ERROR_PREFIX_RE =
  /^(?:error|openclaw run failed|openclaw bridge failed|nexchatbot bridge failed|failed to process message)\s*:\s*/i;
const PATH_LIKE_RE = /(?:[A-Za-z]:\\|\/)[^\s)]+/;
const STACK_FRAME_RE = /\bat\s+[^\s].+\(.+:\d+:\d+\)/;
const CONFIG_ERROR_RE =
  /\b(?:is disabled|not found|missing relay token|missing appid\/appsecret|failed to parse .*config|config template|openclaw root not found|openclaw entry not found)\b/i;
const IDENTITY_ERROR_RE =
  /\b(?:trusted requester identity unavailable|missing feishu sender identity|missing trusted .*identity|requestersenderid|sender_open_id|sender_id)\b/i;
const PERMISSION_ERROR_RE =
  /\b(?:not allowed|permission denied|admin\/super_root only|only .* can|forbidden|access denied)\b/i;
const TIMEOUT_ERROR_RE = /\b(?:timed out|timeout|time out|aborted)\b/i;
const UNAVAILABLE_ERROR_RE =
  /\b(?:econnrefused|econnreset|ehostunreach|enotfound|service unavailable|temporarily unavailable|overloaded|rate limit|http 50[234]|connection refused|socket hang up|network error|relay disconnected)\b/i;
const INVALID_REQUEST_RE =
  /\b(?:invalid|required|missing|malformed|bad request|group not found|workspace path is required|usage: )\b/i;

function sourceLabel(source: GatewayErrorSource): string {
  switch (source) {
    case "openclaw":
      return "OpenClaw";
    case "nexchatbot":
      return "NexChatBot";
    case "access":
      return "访问控制";
    case "gateway":
    default:
      return "网关";
  }
}

function collapseWhitespace(text: string): string {
  return text.replace(/\s+/g, " ").trim();
}

function unwrapErrorMessage(raw: string): string {
  let next = collapseWhitespace(raw);
  while (WRAPPED_ERROR_PREFIX_RE.test(next)) {
    next = next.replace(WRAPPED_ERROR_PREFIX_RE, "").trim();
  }
  return next;
}

function extractHttpStatus(raw: string): string | undefined {
  const match = raw.match(/\bhttp\s+(50[234]|429)\b/i);
  return match?.[1];
}

function extractTimeoutText(raw: string): string | undefined {
  const match = raw.match(/(\d+)\s*ms\b/i);
  if (!match) {
    return undefined;
  }
  const durationMs = Number.parseInt(match[1] ?? "", 10);
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    return undefined;
  }
  if (durationMs % 1000 === 0) {
    const seconds = durationMs / 1000;
    if (seconds >= 60 && seconds % 60 === 0) {
      return `${seconds / 60} 分钟`;
    }
    return `${seconds} 秒`;
  }
  return `${durationMs}ms`;
}

function isSafeUserDetail(detail: string): boolean {
  if (!detail) {
    return false;
  }
  if (detail.length > 160) {
    return false;
  }
  if (PATH_LIKE_RE.test(detail)) {
    return false;
  }
  if (STACK_FRAME_RE.test(detail)) {
    return false;
  }
  if (/\b(?:TypeError|ReferenceError|SyntaxError)\b/.test(detail)) {
    return false;
  }
  return true;
}

function summarizeInvalidRequest(raw: string): string | undefined {
  const normalized = raw.replace(/^usage:\s*/i, "用法错误：").trim();
  return isSafeUserDetail(normalized) ? normalized : undefined;
}

export function extractErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message || String(error);
  }
  if (typeof error === "string") {
    return error;
  }
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

export function buildGatewayErrorFeedback(source: GatewayErrorSource, error: unknown): GatewayErrorFeedback {
  const rawMessage = collapseWhitespace(extractErrorMessage(error));
  const normalized = unwrapErrorMessage(rawMessage);
  const label = sourceLabel(source);

  if (/is disabled/i.test(normalized)) {
    return {
      code: "CONFIG_DISABLED",
      title: `${label} 服务未启用`,
      reason: `${label} 当前没有启用，暂时无法处理这条请求。`,
      action: "请联系管理员检查服务配置后重试。",
      rawMessage,
    };
  }

  if (CONFIG_ERROR_RE.test(normalized)) {
    return {
      code: "CONFIG_INVALID",
      title: `${label} 配置异常`,
      reason: `${label} 当前配置不完整、无效，或依赖文件缺失。`,
      action: "请联系管理员检查服务配置和部署文件后重试。",
      rawMessage,
    };
  }

  if (IDENTITY_ERROR_RE.test(normalized)) {
    return {
      code: "IDENTITY_UNAVAILABLE",
      title: "无法确认当前飞书身份",
      reason: "当前请求缺少可信的请求者身份信息，系统无法安全地代你授权或执行关联操作。",
      action: "请重新发送一次；如果仍失败，请联系管理员检查 requesterSenderId / senderId 的透传链路。",
      rawMessage,
    };
  }

  if (PERMISSION_ERROR_RE.test(normalized)) {
    return {
      code: "PERMISSION_DENIED",
      title: "当前操作被拒绝",
      reason: "当前账号没有执行这项操作所需的权限。",
      action: "请确认你有对应权限，或联系管理员处理。",
      rawMessage,
    };
  }

  if (TIMEOUT_ERROR_RE.test(normalized)) {
    const timeoutText = extractTimeoutText(normalized);
    return {
      code: "REQUEST_TIMEOUT",
      title: `${label} 响应超时`,
      reason: timeoutText
        ? `${label} 在 ${timeoutText} 内没有返回结果。`
        : `${label} 在限定时间内没有返回结果。`,
      action: "请稍后重试；如果频繁出现，请联系管理员检查网关和对应用户进程状态。",
      rawMessage,
    };
  }

  if (UNAVAILABLE_ERROR_RE.test(normalized)) {
    const httpStatus = extractHttpStatus(normalized);
    return {
      code: "SERVICE_UNAVAILABLE",
      title: `${label} 暂时不可用`,
      reason: httpStatus
        ? `${label} 当前暂时不可用或上游服务繁忙（HTTP ${httpStatus}）。`
        : `${label} 当前连接异常，或上游服务暂时不可用。`,
      action: "请稍后重试；如果持续失败，请联系管理员检查服务和网络状态。",
      rawMessage,
    };
  }

  if (INVALID_REQUEST_RE.test(normalized)) {
    return {
      code: "INVALID_REQUEST",
      title: "请求内容不完整或格式不正确",
      reason: "系统无法按当前参数执行这次请求。",
      action: "请调整请求内容后重试。",
      detail: summarizeInvalidRequest(normalized),
      rawMessage,
    };
  }

  return {
    code: "INTERNAL_ERROR",
    title: `${label} 内部错误`,
    reason: `${label} 在处理请求时发生了内部错误。`,
    action: "请稍后重试；如果持续失败，请联系管理员查看日志。",
    rawMessage,
  };
}

export function formatGatewayErrorFeedback(source: GatewayErrorSource, error: unknown): string {
  const feedback = buildGatewayErrorFeedback(source, error);
  const lines = [
    `[${feedback.code}] ${feedback.title}`,
    `原因：${feedback.reason}`,
    feedback.detail ? `细节：${feedback.detail}` : "",
    `建议：${feedback.action}`,
  ].filter((line) => line.length > 0);
  return lines.join("\n");
}
