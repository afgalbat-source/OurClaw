#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import * as Lark from "@larksuiteoapi/node-sdk";

function parseArgs(argv) {
  const args = {
    configPath: path.resolve("TFClaw/config.json"),
    prompt: "请用语音发送给我一段祝福",
    chatType: "p2p",
    senderOpenId: "",
    timeoutMs: 180_000,
    pollMs: 2_500,
    requireAudio: true,
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
      case "--prompt":
        if (value && !value.startsWith("--")) {
          args.prompt = value;
          i += 1;
        }
        break;
      case "--timeout-ms": {
        if (value && !value.startsWith("--")) {
          const n = Number.parseInt(value, 10);
          if (Number.isFinite(n) && n > 0) {
            args.timeoutMs = n;
          }
          i += 1;
        }
        break;
      }
      case "--poll-ms": {
        if (value && !value.startsWith("--")) {
          const n = Number.parseInt(value, 10);
          if (Number.isFinite(n) && n > 0) {
            args.pollMs = n;
          }
          i += 1;
        }
        break;
      }
      case "--app-id":
        if (value && !value.startsWith("--")) {
          args.appId = value.trim();
          i += 1;
        }
        break;
      case "--app-secret":
        if (value && !value.startsWith("--")) {
          args.appSecret = value.trim();
          i += 1;
        }
        break;
      case "--allow-no-audio":
        args.requireAudio = false;
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseFeishuCreateTime(rawValue) {
  const n = Number.parseInt(String(rawValue ?? "0"), 10);
  if (!Number.isFinite(n) || n <= 0) {
    return 0;
  }
  if (n < 1_000_000_000_000) {
    return n * 1000;
  }
  return n;
}

async function fetchTenantToken(appId, appSecret) {
  const resp = await fetch("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      app_id: appId,
      app_secret: appSecret,
    }),
  });
  const json = await resp.json();
  if (!resp.ok || Number(json.code) !== 0 || !json.tenant_access_token) {
    throw new Error(`tenant token request failed: status=${resp.status} body=${JSON.stringify(json)}`);
  }
  return String(json.tenant_access_token);
}

async function listRecentMessages(tenantToken, chatId, pageSize = 50) {
  const url = new URL("https://open.feishu.cn/open-apis/im/v1/messages");
  url.searchParams.set("container_id_type", "chat");
  url.searchParams.set("container_id", chatId);
  url.searchParams.set("sort_type", "ByCreateTimeDesc");
  url.searchParams.set("page_size", String(pageSize));
  const resp = await fetch(url.toString(), {
    method: "GET",
    headers: {
      authorization: `Bearer ${tenantToken}`,
    },
  });
  const json = await resp.json();
  if (!resp.ok || Number(json.code) !== 0) {
    throw new Error(`list messages failed: status=${resp.status} body=${JSON.stringify(json)}`);
  }
  const items = Array.isArray(json?.data?.items) ? json.data.items : [];
  return items.map((item) => {
    const body = item?.body && typeof item.body === "object" ? item.body : {};
    const content = typeof body.content === "string" ? body.content : "";
    return {
      messageId: String(item?.message_id ?? ""),
      msgType: String(item?.msg_type ?? ""),
      createTimeMs: parseFeishuCreateTime(item?.create_time),
      content,
      raw: item,
    };
  });
}

function dumpMessages(items, limit = 10) {
  const sliced = items.slice(0, limit);
  for (const item of sliced) {
    const preview = (item.content || "").replace(/\s+/g, " ").slice(0, 140);
    console.log(
      `- ${item.messageId} | ${item.msgType} | ${item.createTimeMs} | ${preview}`,
    );
  }
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.chatId) {
    die("missing required --chat-id");
  }
  if (!args.senderOpenId) {
    die("missing required --sender-open-id");
  }
  if (!fs.existsSync(args.configPath)) {
    die(`config not found: ${args.configPath}`);
  }

  const configDir = path.resolve(path.dirname(args.configPath));
  const gatewayDistPath = path.resolve(configDir, "apps/feishu-gateway/dist/index.js");
  if (!fs.existsSync(gatewayDistPath)) {
    die(`gateway dist not found: ${gatewayDistPath}. run: npm run build --workspace @tfclaw/feishu-gateway`);
  }

  const rawCfg = JSON.parse(fs.readFileSync(args.configPath, "utf8"));
  const feishuCfg = rawCfg?.channels?.feishu ?? {};
  const appId = (args.appId || feishuCfg.appId || "").trim();
  const appSecret = (args.appSecret || feishuCfg.appSecret || "").trim();
  if (!appId || !appSecret) {
    die("missing appId/appSecret");
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
      appId,
      appSecret,
    },
    router,
  );
  app.larkClient = new Lark.Client({ appId, appSecret });

  const tenantToken = await fetchTenantToken(appId, appSecret);
  const startMs = Date.now();
  const fakeInboundEvent = {
    header: {
      event_id: `openclaw-audio-test-${Date.now()}`,
    },
    event: {
      sender: {
        sender_id: {
          open_id: args.senderOpenId,
        },
      },
      message: {
        message_id: `om_tfclaw_test_${Date.now()}`,
        chat_id: args.chatId,
        chat_type: args.chatType,
        message_type: "text",
        content: JSON.stringify({
          text: args.prompt,
        }),
        create_time: String(Date.now()),
      },
    },
  };

  console.log("=== TFClaw OpenClaw Feishu Audio E2E Test ===");
  console.log(`chat_id: ${args.chatId}`);
  console.log(`chat_type: ${args.chatType}`);
  console.log(`sender_open_id: ${args.senderOpenId}`);
  console.log(`prompt: ${args.prompt}`);
  console.log(`require_audio: ${args.requireAudio}`);
  console.log("--- injecting fake inbound event into TFClaw Feishu gateway (real openclaw router) ---");
  await app.handleInboundEvent(fakeInboundEvent);

  console.log("--- waiting for outbound messages ---");
  const deadline = Date.now() + args.timeoutMs;
  while (Date.now() < deadline) {
    const items = await listRecentMessages(tenantToken, args.chatId, 50);
    const fresh = items.filter((item) => item.createTimeMs >= startMs - 3_000);
    const hasAudio = fresh.some((item) => item.msgType === "audio");
    const hasAnyReply = fresh.some((item) => item.msgType !== "text");
    if ((args.requireAudio && hasAudio) || (!args.requireAudio && hasAnyReply)) {
      console.log("RESULT: PASS");
      console.log(`fresh_message_count: ${fresh.length}`);
      console.log(`fresh_audio_count: ${fresh.filter((item) => item.msgType === "audio").length}`);
      dumpMessages(fresh, 10);
      return;
    }
    await sleep(args.pollMs);
  }

  const latest = await listRecentMessages(tenantToken, args.chatId, 20);
  console.log("RESULT: FAIL");
  console.log("reason: timeout waiting for expected outbound messages");
  console.log("latest messages:");
  dumpMessages(latest, 10);
  process.exit(2);
}

main().catch((error) => {
  console.error(`FATAL: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});

