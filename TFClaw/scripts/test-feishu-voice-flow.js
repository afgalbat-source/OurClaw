#!/usr/bin/env node

/*
 * Simulate a user prompt entering TFClaw->OpenClaw bridge and print the
 * final payload shape that TFClaw Feishu gateway will dispatch.
 *
 * Example:
 *   node TFClaw/scripts/test-feishu-voice-flow.js \
 *     --open-id ou_a2f6bc06c7734c4762ede53f8e23c018 \
 *     --text "请用语音发送给我一段祝福"
 */

const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

function parseArgs(argv) {
  const args = {
    mapPath: path.resolve("TFClaw/.runtime/openclaw_bridge/feishu-user-map.json"),
    host: "127.0.0.1",
    timeoutMs: 120000,
    text: "请用语音发送给我一段祝福",
  };

  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === "--map" && next) {
      args.mapPath = path.resolve(next);
      i += 1;
      continue;
    }
    if (key === "--open-id" && next) {
      args.openId = next.trim();
      i += 1;
      continue;
    }
    if (key === "--linux-user" && next) {
      args.linuxUser = next.trim();
      i += 1;
      continue;
    }
    if (key === "--text" && next) {
      args.text = next;
      i += 1;
      continue;
    }
    if (key === "--host" && next) {
      args.host = next.trim();
      i += 1;
      continue;
    }
    if (key === "--timeout-ms" && next) {
      const n = Number.parseInt(next, 10);
      if (Number.isFinite(n) && n > 0) {
        args.timeoutMs = n;
      }
      i += 1;
      continue;
    }
    if (key === "--session-key" && next) {
      args.sessionKey = next.trim();
      i += 1;
      continue;
    }
  }
  return args;
}

function loadBinding(mapPath, openId, linuxUser) {
  const raw = JSON.parse(fs.readFileSync(mapPath, "utf8"));
  const users = raw && raw.users ? raw.users : {};

  if (openId && users[openId]) {
    return {
      userKey: openId,
      ...users[openId],
    };
  }

  if (linuxUser) {
    for (const [userKey, value] of Object.entries(users)) {
      if ((value.linuxUser || "").trim() === linuxUser.trim()) {
        return {
          userKey,
          ...value,
        };
      }
    }
  }

  return undefined;
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

function extractTextFromMessage(message) {
  const msg = toObject(message);
  const text = toString(msg.text).trim();
  if (text) {
    return text;
  }
  const content = Array.isArray(msg.content) ? msg.content : [];
  const lines = [];
  for (const item of content) {
    const block = toObject(item);
    const type = toString(block.type).trim().toLowerCase();
    if (type !== "text") {
      continue;
    }
    const t = toString(block.text).trim();
    if (t) {
      lines.push(t);
    }
  }
  return lines.join("\n").trim();
}

function normalizeMime(mime) {
  return toString(mime).trim().split(";")[0].trim().toLowerCase();
}

function inferDispatchKind(media) {
  const mime = normalizeMime(media.mimeType);
  const fileName = toString(media.fileName).toLowerCase();
  const imageByExt = /\.(?:jpg|jpeg|png|gif|webp|bmp|ico|tiff)$/i.test(fileName);
  const audioByExt = /\.(?:mp3|wav|ogg|opus|m4a|aac|flac|amr)$/i.test(fileName);
  if (mime.startsWith("image/") || imageByExt || media.type === "image") {
    return "image";
  }
  if (mime.startsWith("audio/") || audioByExt || media.type === "audio") {
    return "audio";
  }
  return "file";
}

function extractDirectMediaFromMessage(message) {
  const msg = toObject(message);
  const content = Array.isArray(msg.content) ? msg.content : [];
  const result = [];
  for (const item of content) {
    const block = toObject(item);
    const type = toString(block.type).trim().toLowerCase();
    if (!["image", "file", "media", "audio"].includes(type)) {
      continue;
    }
    const rawData = toString(block.data, toString(block.base64, toString(block.content))).trim();
    if (!rawData) {
      continue;
    }
    let base64 = rawData.replace(/\s+/g, "");
    let mimeType = normalizeMime(toString(block.mimeType, toString(block.mime, toString(block.contentType))));

    const dataUrlMatch = rawData.match(/^data:([^;,]+)?;base64,(.+)$/i);
    if (dataUrlMatch) {
      mimeType = normalizeMime(dataUrlMatch[1]);
      base64 = (dataUrlMatch[2] || "").replace(/\s+/g, "");
    }

    if (!/^[A-Za-z0-9+/]+={0,2}$/.test(base64) || base64.length % 4 !== 0) {
      continue;
    }

    const size = Buffer.from(base64, "base64").byteLength;
    result.push({
      type,
      fileName: toString(block.fileName, toString(block.name, `${type}-${Date.now()}`)),
      mimeType: mimeType || "application/octet-stream",
      size,
      dispatchAs: inferDispatchKind({ type, fileName: toString(block.fileName, toString(block.name, "")), mimeType }),
    });
  }
  return result;
}

async function run(args) {
  const binding = loadBinding(args.mapPath, args.openId, args.linuxUser);
  if (!binding) {
    throw new Error(`binding not found (map=${args.mapPath}, openId=${args.openId || ""}, linuxUser=${args.linuxUser || ""})`);
  }

  const openId = args.openId || (binding.userKey.startsWith("ou_") ? binding.userKey : "");
  const sessionKey = args.sessionKey || `agent:main:feishu:dm:${openId || "unknown"}`;
  const url = `ws://${args.host}:${binding.gatewayPort}`;

  const connectReqId = `connect-${Date.now().toString(16)}`;
  const chatReqId = `chat-${(Date.now() + 1).toString(16)}`;

  const startedAt = Date.now();
  const trace = {
    url,
    linuxUser: binding.linuxUser,
    userKey: binding.userKey,
    sessionKey,
    prompt: args.text,
    events: [],
  };

  await new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    let timeout;
    let done = false;

    const fail = (err) => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timeout);
      try {
        ws.close();
      } catch {}
      reject(err instanceof Error ? err : new Error(String(err)));
    };

    const finish = () => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timeout);
      try {
        ws.close();
      } catch {}
      resolve();
    };

    timeout = setTimeout(() => {
      fail(new Error(`timeout after ${args.timeoutMs}ms`));
    }, args.timeoutMs);

    ws.once("open", () => {
      ws.send(JSON.stringify({
        type: "req",
        id: connectReqId,
        method: "connect",
        params: {
          minProtocol: 1,
          maxProtocol: 99,
          client: {
            id: "gateway-client",
            version: "1.0.0",
            platform: process.platform,
            mode: "backend",
          },
          role: "operator",
          scopes: ["operator.admin"],
          auth: { token: binding.gatewayToken },
        },
      }));
    });

    ws.on("message", (raw) => {
      let frame = {};
      try {
        frame = JSON.parse(typeof raw === "string" ? raw : raw.toString());
      } catch {
        return;
      }
      const frameType = toString(frame.type).toLowerCase();
      if (frameType === "res") {
        const id = toString(frame.id);
        const ok = Boolean(frame.ok);
        if (id === connectReqId) {
          if (!ok) {
            fail(new Error(`connect failed: ${JSON.stringify(frame.error || frame)}`));
            return;
          }
          ws.send(JSON.stringify({
            type: "req",
            id: chatReqId,
            method: "chat.send",
            params: {
              sessionKey,
              message: args.text,
              deliver: false,
              timeoutMs: args.timeoutMs,
              idempotencyKey: `voice-test-${Date.now().toString(16)}`,
            },
          }));
          return;
        }
        if (id === chatReqId && !ok) {
          fail(new Error(`chat.send failed: ${JSON.stringify(frame.error || frame)}`));
          return;
        }
        return;
      }

      if (frameType !== "event") {
        return;
      }
      const eventName = toString(frame.event).toLowerCase();
      const payload = toObject(frame.payload);
      const state = toString(payload.state).toLowerCase();
      const message = toObject(payload.message);

      if (eventName === "chat") {
        trace.events.push({
          event: eventName,
          state,
          text: extractTextFromMessage(message),
          media: extractDirectMediaFromMessage(message),
        });
      } else if (eventName === "agent") {
        trace.events.push({
          event: eventName,
          text: toString(toObject(payload.data).text).trim(),
        });
      }

      if (eventName === "chat" && (state === "final" || state === "error" || state === "aborted")) {
        finish();
      }
    });

    ws.once("error", (error) => {
      fail(error);
    });

    ws.once("close", (code, reason) => {
      if (!done) {
        fail(new Error(`socket closed early: code=${code} reason=${reason.toString()}`));
      }
    });
  });

  const finalEvent = [...trace.events].reverse().find((item) => item.event === "chat");
  const elapsedMs = Date.now() - startedAt;

  console.log("=== Voice Flow Test Result ===");
  console.log(`gateway: ${trace.url}`);
  console.log(`userKey: ${trace.userKey}`);
  console.log(`linuxUser: ${trace.linuxUser}`);
  console.log(`sessionKey: ${trace.sessionKey}`);
  console.log(`elapsedMs: ${elapsedMs}`);
  console.log(`prompt: ${trace.prompt}`);
  console.log("---");
  if (!finalEvent) {
    console.log("No chat event received.");
  } else {
    console.log(`chat.state: ${finalEvent.state || "unknown"}`);
    console.log(`chat.text: ${JSON.stringify(finalEvent.text || "")}`);
    console.log(`chat.media.count: ${Array.isArray(finalEvent.media) ? finalEvent.media.length : 0}`);
    if (Array.isArray(finalEvent.media)) {
      finalEvent.media.forEach((item, idx) => {
        console.log(
          `  [${idx + 1}] type=${item.type} mime=${item.mimeType} file=${item.fileName} size=${item.size} dispatchAs=${item.dispatchAs}`,
        );
      });
    }
  }
  console.log("--- raw events ---");
  console.log(JSON.stringify(trace.events, null, 2));
}

(async () => {
  try {
    const args = parseArgs(process.argv);
    await run(args);
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[voice-flow-test] failed: ${msg}`);
    process.exit(1);
  }
})();
