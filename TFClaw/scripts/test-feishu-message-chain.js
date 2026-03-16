#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

function parseArgs(argv) {
  const args = {
    configPath: path.resolve("TFClaw/config.json"),
    chatType: "p2p",
    text: "请回复：链路测试成功",
    timeoutMs: 180000,
    json: false,
  };

  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const value = argv[i + 1];
    switch (key) {
      case "--config":
        if (value && !value.startsWith("--")) {
          args.configPath = path.resolve(value);
          i += 1;
        }
        break;
      case "--map":
        if (value && !value.startsWith("--")) {
          args.mapPath = path.resolve(value);
          i += 1;
        }
        break;
      case "--chat-id":
        if (value && !value.startsWith("--")) {
          args.chatId = value.trim();
          i += 1;
        }
        break;
      case "--chat-type":
        if (value && !value.startsWith("--")) {
          args.chatType = value.trim().toLowerCase();
          i += 1;
        }
        break;
      case "--sender-open-id":
        if (value && !value.startsWith("--")) {
          args.senderOpenId = value.trim();
          i += 1;
        }
        break;
      case "--text":
        if (value && !value.startsWith("--")) {
          args.text = value;
          i += 1;
        }
        break;
      case "--timeout-ms":
        if (value && !value.startsWith("--")) {
          const n = Number.parseInt(value, 10);
          if (Number.isFinite(n) && n > 0) {
            args.timeoutMs = n;
          }
          i += 1;
        }
        break;
      case "--json":
        args.json = true;
        break;
      default:
        break;
    }
  }

  return args;
}

function die(message) {
  console.error(`ERROR: ${message}`);
  process.exit(1);
}

function toObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value;
}

function toString(value, fallback = "") {
  return typeof value === "string" ? value : fallback;
}

function resolveMapPath(args, configPath, rawConfig) {
  if (args.mapPath) {
    return args.mapPath;
  }
  const configDir = path.resolve(path.dirname(configPath));
  const rawStateDir = toString(toObject(rawConfig.openclawBridge).stateDir, ".runtime/openclaw_bridge");
  const stateDir = path.isAbsolute(rawStateDir)
    ? rawStateDir
    : path.resolve(configDir, rawStateDir);
  return path.join(stateDir, "feishu-user-map.json");
}

function findDefaultSenderOpenId(mapPath) {
  if (!fs.existsSync(mapPath)) {
    return "";
  }
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(mapPath, "utf8"));
  } catch {
    return "";
  }
  const users = toObject(parsed.users);
  for (const key of Object.keys(users)) {
    if (/^ou_[A-Za-z0-9]+$/i.test(key.trim())) {
      return key.trim();
    }
  }
  return "";
}

function previewText(text, limit = 220) {
  const normalized = String(text || "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }
  if (normalized.length <= limit) {
    return normalized;
  }
  return `${normalized.slice(0, limit)}...`;
}

function normalizePostText(content) {
  const parsed = toObject(JSON.parse(toString(content, "{}")));
  const zh = toObject(parsed.zh_cn);
  const rows = Array.isArray(zh.content) ? zh.content : [];
  const parts = [];
  for (const row of rows) {
    const cols = Array.isArray(row) ? row : [];
    for (const col of cols) {
      const block = toObject(col);
      const blockText = toString(block.text).trim();
      if (blockText) {
        parts.push(blockText);
      }
    }
  }
  return parts.join("\n").trim();
}

function normalizeInteractiveText(content) {
  const parsed = toObject(JSON.parse(toString(content, "{}")));
  const body = toObject(parsed.body);
  const elements = Array.isArray(body.elements) ? body.elements : [];
  const lines = [];
  for (const element of elements) {
    const item = toObject(element);
    const text = toString(item.content, toString(item.text)).trim();
    if (text) {
      lines.push(text);
    }
  }
  return lines.join("\n").trim();
}

function normalizeOutboundText(msgType, content) {
  try {
    const type = toString(msgType).trim().toLowerCase();
    if (type === "post") {
      return normalizePostText(content);
    }
    if (type === "interactive") {
      return normalizeInteractiveText(content);
    }
  } catch {
    return toString(content).trim();
  }
  return toString(content).trim();
}

async function main() {
  const args = parseArgs(process.argv);
  if (!fs.existsSync(args.configPath)) {
    die(`config not found: ${args.configPath}`);
  }

  let rawConfig;
  try {
    rawConfig = JSON.parse(fs.readFileSync(args.configPath, "utf8"));
  } catch (error) {
    die(`failed to parse config: ${error instanceof Error ? error.message : String(error)}`);
  }

  const mapPath = resolveMapPath(args, args.configPath, rawConfig);
  const senderOpenId = args.senderOpenId || findDefaultSenderOpenId(mapPath);
  if (!senderOpenId) {
    die(`missing --sender-open-id and no default open_id found in map: ${mapPath}`);
  }

  const chatId = args.chatId || `oc_tfclaw_sim_${Date.now()}`;
  const configDir = path.resolve(path.dirname(args.configPath));
  const gatewayDistPath = path.resolve(configDir, "apps/feishu-gateway/dist/index.js");
  if (!fs.existsSync(gatewayDistPath)) {
    die(`gateway dist not found: ${gatewayDistPath}. run: cd TFClaw && npm run build --workspace @tfclaw/feishu-gateway`);
  }

  process.env.TFCLAW_FEISHU_GATEWAY_SKIP_BOOTSTRAP = "1";
  process.env.TFCLAW_CONFIG_PATH = args.configPath;

  const mod = await import(`${pathToFileURL(gatewayDistPath).href}?t=${Date.now()}`);
  const hooks = mod?.__tfclawGatewayTestHooks ?? {};
  const {
    FeishuChatApp,
    RelayBridge,
    NexChatBridgeClient,
    OpenClawPerUserBridge,
    TfclawAccessManager,
    TfclawCommandRouter,
    loadGatewayConfig,
  } = hooks;
  if (
    typeof FeishuChatApp !== "function"
    || typeof RelayBridge !== "function"
    || typeof NexChatBridgeClient !== "function"
    || typeof OpenClawPerUserBridge !== "function"
    || typeof TfclawAccessManager !== "function"
    || typeof TfclawCommandRouter !== "function"
    || typeof loadGatewayConfig !== "function"
  ) {
    die("test hooks are incomplete. rebuild @tfclaw/feishu-gateway first");
  }

  const loaded = loadGatewayConfig();
  const relay = new RelayBridge(loaded.config.relay.url, loaded.config.relay.token, "feishu");
  const nexBridge = new NexChatBridgeClient(loaded.config.nexchatbot);
  const openclawBridge = new OpenClawPerUserBridge(loaded.config.openclawBridge);
  const accessManager = new TfclawAccessManager(
    loaded.config.openclawBridge.stateDir,
    loaded.config.openclawBridge.userHomeRoot,
  );
  const router = new TfclawCommandRouter(relay, nexBridge, openclawBridge, accessManager);
  const app = new FeishuChatApp(
    {
      ...loaded.config.channels.feishu,
      enabled: true,
    },
    router,
  );

  const appAny = app;
  const outbound = [];
  let seq = 0;
  const nextMessageId = () => `mock_out_${Date.now()}_${(seq += 1).toString().padStart(3, "0")}`;

  appAny.sendTextMessage = async (targetChatId, text, options) => {
    const messageId = nextMessageId();
    outbound.push({
      messageId,
      kind: "text",
      msgType: "post",
      chatId: targetChatId,
      replyToMessageId: toString(options?.replyToMessageId),
      replyInThread: Boolean(options?.replyInThread),
      text: toString(text),
      preview: previewText(text),
    });
    return { messageId };
  };

  appAny.replyImage = async (targetChatId, imageBase64, options) => {
    const messageId = nextMessageId();
    const size = Buffer.from(toString(imageBase64), "base64").byteLength;
    outbound.push({
      messageId,
      kind: "image",
      msgType: "image",
      chatId: targetChatId,
      replyToMessageId: toString(options?.replyToMessageId),
      replyInThread: Boolean(options?.replyInThread),
      sizeBytes: size,
      preview: `[image ${size} bytes]`,
    });
  };

  appAny.replyAudio = async (targetChatId, audioBase64, fileName, mimeType, options) => {
    const messageId = nextMessageId();
    const size = Buffer.from(toString(audioBase64), "base64").byteLength;
    outbound.push({
      messageId,
      kind: "audio",
      msgType: "audio",
      chatId: targetChatId,
      replyToMessageId: toString(options?.replyToMessageId),
      replyInThread: Boolean(options?.replyInThread),
      fileName: toString(fileName, ""),
      mimeType: toString(mimeType, ""),
      sizeBytes: size,
      preview: `[audio ${size} bytes] ${toString(fileName, "")}`,
    });
  };

  appAny.replyFile = async (targetChatId, fileBase64, fileName, mimeType, options) => {
    const messageId = nextMessageId();
    const size = Buffer.from(toString(fileBase64), "base64").byteLength;
    outbound.push({
      messageId,
      kind: "file",
      msgType: "file",
      chatId: targetChatId,
      replyToMessageId: toString(options?.replyToMessageId),
      replyInThread: Boolean(options?.replyInThread),
      fileName: toString(fileName, ""),
      mimeType: toString(mimeType, ""),
      sizeBytes: size,
      preview: `[file ${size} bytes] ${toString(fileName, "")}`,
    });
  };

  appAny.deleteMessage = async () => {};
  appAny.sendFeishuReplyOrDirectMessage = async (targetChatId, msgType, content, options) => {
    const messageId = nextMessageId();
    const normalizedText = normalizeOutboundText(msgType, content);
    outbound.push({
      messageId,
      kind: "raw",
      msgType: toString(msgType, "post"),
      chatId: targetChatId,
      replyToMessageId: toString(options?.replyToMessageId),
      replyInThread: Boolean(options?.replyInThread),
      text: normalizedText,
      preview: previewText(normalizedText),
    });
    return { messageId };
  };

  const eventId = `tfclaw-sim-event-${Date.now()}`;
  const messageId = `om_tfclaw_sim_${Date.now()}`;
  const fakeInboundEvent = {
    header: {
      event_id: eventId,
    },
    event: {
      sender: {
        sender_id: {
          open_id: senderOpenId,
        },
      },
      message: {
        message_id: messageId,
        chat_id: chatId,
        chat_type: args.chatType,
        message_type: "text",
        content: JSON.stringify({
          text: args.text,
        }),
        create_time: String(Date.now()),
      },
    },
  };

  let timeoutTimer;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutTimer = setTimeout(() => {
      reject(new Error(`timeout after ${args.timeoutMs}ms`));
    }, args.timeoutMs);
  });

  console.log("=== TFClaw Feishu Message Chain Simulation ===");
  console.log(`config: ${args.configPath}`);
  console.log(`map: ${mapPath}`);
  console.log(`sender_open_id: ${senderOpenId}`);
  console.log(`chat_id: ${chatId}`);
  console.log(`chat_type: ${args.chatType}`);
  console.log(`text: ${args.text}`);
  console.log("--- injecting fake inbound event ---");

  try {
    await Promise.race([
      appAny.handleInboundEvent(fakeInboundEvent),
      timeoutPromise,
    ]);
  } finally {
    clearTimeout(timeoutTimer);
  }

  if (outbound.length === 0) {
    die("no outbound bot message captured");
  }

  if (args.json) {
    console.log(JSON.stringify({
      ok: true,
      outboundCount: outbound.length,
      outbound,
    }, null, 2));
    return;
  }

  console.log("--- captured outbound bot messages ---");
  for (const item of outbound) {
    console.log(
      `- ${item.messageId} | ${item.msgType} | ${item.kind} | ${item.preview}`,
    );
  }
  const firstTextReply = outbound.find((item) => typeof item.text === "string" && item.text.trim());
  if (firstTextReply) {
    console.log("--- first text reply ---");
    console.log(firstTextReply.text.trim());
  }
  console.log("RESULT: PASS");
}

main().catch((error) => {
  console.error(`FATAL: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});
