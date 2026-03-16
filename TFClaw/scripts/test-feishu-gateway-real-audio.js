#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import * as Lark from "@larksuiteoapi/node-sdk";

function parseArgs(argv) {
  const args = {
    configPath: path.resolve("TFClaw/config.json"),
    prompt: "请用语音发送给我一段祝福",
    chatType: "p2p",
    senderOpenId: "ou_tfclaw_audio_test_sender",
    timeoutMs: 60_000,
    pollMs: 2_000,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const value = argv[i + 1];
    if (!key.startsWith("--")) {
      continue;
    }
    if (value == null || value.startsWith("--")) {
      continue;
    }
    switch (key) {
      case "--config":
        args.configPath = path.resolve(value);
        i += 1;
        break;
      case "--chat-id":
        args.chatId = value.trim();
        i += 1;
        break;
      case "--chat-type":
        args.chatType = value.trim().toLowerCase();
        i += 1;
        break;
      case "--sender-open-id":
        args.senderOpenId = value.trim();
        i += 1;
        break;
      case "--prompt":
        args.prompt = value;
        i += 1;
        break;
      case "--timeout-ms": {
        const n = Number.parseInt(value, 10);
        if (Number.isFinite(n) && n > 0) {
          args.timeoutMs = n;
        }
        i += 1;
        break;
      }
      case "--poll-ms": {
        const n = Number.parseInt(value, 10);
        if (Number.isFinite(n) && n > 0) {
          args.pollMs = n;
        }
        i += 1;
        break;
      }
      case "--app-id":
        args.appId = value.trim();
        i += 1;
        break;
      case "--app-secret":
        args.appSecret = value.trim();
        i += 1;
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

function requireCommand(cmd) {
  const probe = spawnSync("bash", ["-lc", `command -v ${cmd} >/dev/null 2>&1`], {
    encoding: "utf8",
  });
  if (probe.status !== 0) {
    die(`required command not found: ${cmd}`);
  }
}

function generateOpusBase64() {
  requireCommand("ffmpeg");
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "tfclaw-audio-test-"));
  const opusPath = path.join(tmpDir, "probe.opus");
  const ff = spawnSync(
    "ffmpeg",
    [
      "-y",
      "-f",
      "lavfi",
      "-i",
      "sine=frequency=880:duration=1.2",
      "-ac",
      "1",
      "-ar",
      "24000",
      "-c:a",
      "libopus",
      "-b:a",
      "32k",
      opusPath,
    ],
    {
      encoding: "utf8",
    },
  );
  if (ff.status !== 0) {
    die(`ffmpeg failed: ${(ff.stderr || ff.stdout || "").trim()}`);
  }
  if (!fs.existsSync(opusPath) || fs.statSync(opusPath).size <= 0) {
    die("generated opus file is missing or empty");
  }
  const buf = fs.readFileSync(opusPath);
  return {
    base64: buf.toString("base64"),
    fileName: `tfclaw-audio-test-${Date.now()}.opus`,
    filePath: opusPath,
  };
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

async function listRecentMessages(tenantToken, chatId) {
  const url = new URL("https://open.feishu.cn/open-apis/im/v1/messages");
  url.searchParams.set("container_id_type", "chat");
  url.searchParams.set("container_id", chatId);
  url.searchParams.set("sort_type", "ByCreateTimeDesc");
  url.searchParams.set("page_size", "20");
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

async function main() {
  const args = parseArgs(process.argv);
  if (!args.chatId) {
    die("missing required --chat-id");
  }
  if (!fs.existsSync(args.configPath)) {
    die(`config not found: ${args.configPath}`);
  }

  const cfg = JSON.parse(fs.readFileSync(args.configPath, "utf8"));
  const feishuCfg = cfg?.channels?.feishu ?? {};
  const appId = (args.appId || feishuCfg.appId || "").trim();
  const appSecret = (args.appSecret || feishuCfg.appSecret || "").trim();
  if (!appId || !appSecret) {
    die("missing appId/appSecret (provide --app-id/--app-secret or set in config)");
  }

  const rootDir = path.resolve(path.dirname(args.configPath));
  const gatewayDistPath = path.resolve(rootDir, "apps/feishu-gateway/dist/index.js");
  if (!fs.existsSync(gatewayDistPath)) {
    die(`gateway dist not found: ${gatewayDistPath}. run: npm run build --workspace @tfclaw/feishu-gateway`);
  }

  process.env.TFCLAW_FEISHU_GATEWAY_SKIP_BOOTSTRAP = "1";
  const moduleUrl = `${pathToFileURL(gatewayDistPath).href}?t=${Date.now()}`;
  const mod = await import(moduleUrl);
  const hooks = mod?.__tfclawGatewayTestHooks ?? {};
  const FeishuChatApp = hooks.FeishuChatApp;
  if (typeof FeishuChatApp !== "function") {
    die("failed to load FeishuChatApp test hook");
  }

  const marker = `TFCLAW_AUDIO_TEST_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
  const generated = generateOpusBase64();
  const startMs = Date.now();
  let routerFailure;
  let routerInvoked = false;

  const fakeRouter = {
    async handleInboundMessage(ctx) {
      routerInvoked = true;
      console.log(`router: invoked, chat_id=${ctx.chatId}, chat_type=${ctx.chatType}, message_type=${ctx.messageType}`);
      try {
        await ctx.responder.replyAudio(ctx.chatId, generated.base64, generated.fileName, "audio/ogg");
        console.log("router: replyAudio sent");
        await ctx.responder.replyText(ctx.chatId, `[audio-test-marker] ${marker}`);
        console.log("router: marker text sent");
      } catch (error) {
        routerFailure = error instanceof Error ? error.message : String(error);
        console.error(`router: send failed: ${routerFailure}`);
        throw error;
      }
    },
  };

  const appConfig = {
    ...feishuCfg,
    enabled: true,
    appId,
    appSecret,
  };
  const app = new FeishuChatApp(appConfig, fakeRouter);
  app.larkClient = new Lark.Client({
    appId,
    appSecret,
  });

  const fakeInboundEvent = {
    header: {
      event_id: `audio-test-${Date.now()}`,
    },
    event: {
      sender: {
        sender_id: {
          open_id: args.senderOpenId,
        },
      },
      message: {
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

  if (typeof app.handleInboundEvent !== "function") {
    die("gateway internal method handleInboundEvent is unavailable");
  }

  console.log("=== TFClaw Feishu Audio E2E Test ===");
  console.log(`chat_id: ${args.chatId}`);
  console.log(`chat_type: ${args.chatType}`);
  console.log(`sender_open_id: ${args.senderOpenId}`);
  console.log(`prompt: ${args.prompt}`);
  console.log(`marker: ${marker}`);
  console.log(`generated_audio: ${generated.filePath}`);
  console.log("--- injecting fake inbound event into TFClaw Feishu gateway ---");

  await app.handleInboundEvent(fakeInboundEvent);
  if (!routerInvoked) {
    die("router was not invoked by injected inbound event");
  }
  if (routerFailure) {
    die(`router send failed: ${routerFailure}`);
  }

  console.log("--- outbound send invoked, polling Feishu history ---");
  const tenantToken = await fetchTenantToken(appId, appSecret);
  const deadline = Date.now() + args.timeoutMs;
  let attempts = 0;
  while (Date.now() < deadline) {
    attempts += 1;
    const items = await listRecentMessages(tenantToken, args.chatId);
    const markerMsg = items.find((item) => item.createTimeMs >= startMs - 15_000 && item.content.includes(marker));
    const audioMsg = items.find((item) => item.createTimeMs >= startMs - 15_000 && item.msgType === "audio");
    if (markerMsg && audioMsg) {
      console.log("RESULT: PASS");
      console.log(`attempts: ${attempts}`);
      console.log(`marker_message_id: ${markerMsg.messageId}`);
      console.log(`audio_message_id: ${audioMsg.messageId}`);
      console.log(`audio_msg_type: ${audioMsg.msgType}`);
      console.log(`audio_create_time_ms: ${audioMsg.createTimeMs}`);
      return;
    }
    await sleep(args.pollMs);
  }

  console.log("RESULT: FAIL");
  console.log("reason: timeout waiting for marker + audio message in Feishu history");
  process.exit(2);
}

main().catch((error) => {
  console.error(`FATAL: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});
