import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { NexusServer } from "@imdanibytes/nexus-sdk/server";

const PORT = 80;
const nexus = new NexusServer();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, "public");

const MIME_TYPES = {
  ".html": "text/html",
  ".css": "text/css",
  ".js": "application/javascript",
  ".json": "application/json",
};

// ── Extension Helpers ──────────────────────────────────────────

async function callExtension(operation, input = {}) {
  return nexus.callExtension("zenoh", operation, input);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ── MCP Tool Handlers ──────────────────────────────────────────

async function discoverZenohTopics(args) {
  const keyExpr = args.key_expr || "**";

  // Start discovery (idempotent)
  await callExtension("start_discovery", { key_expr: keyExpr });

  // Wait for samples to arrive
  await sleep(500);

  // Get topics
  const result = await callExtension("get_topics", {
    prefix: keyExpr === "**" ? "" : keyExpr,
  });

  const data = result.data || result;
  const topics = data.topics || [];

  if (topics.length === 0) {
    return {
      content: [
        {
          type: "text",
          text: `No Zenoh topics found matching "${keyExpr}". Discovery is now running — try again in a few seconds if publishers haven't started yet.`,
        },
      ],
      is_error: false,
    };
  }

  const lines = topics.map(
    (t) =>
      `- ${t.key_expr}  (${t.rate_hz} Hz, ${t.last_encoding}, avg ${t.avg_payload_size}B, ${t.sample_count} samples)`
  );

  return {
    content: [
      {
        type: "text",
        text: `Found ${topics.length} Zenoh topic(s):\n${lines.join("\n")}`,
      },
    ],
    is_error: false,
  };
}

async function readZenohTopic(args) {
  const keyExpr = args.key_expr;
  if (!keyExpr) {
    return {
      content: [{ type: "text", text: "Missing required field: key_expr" }],
      is_error: true,
    };
  }

  const waitMs = Math.min(args.wait_ms || 1000, 5000);
  const limit = args.limit || 10;

  // Subscribe
  const subResult = await callExtension("subscribe", {
    key_expr: keyExpr,
    buffer_size: limit * 2,
  });
  const subData = subResult.data || subResult;
  const subId = subData.sub_id;

  // Wait for data
  await sleep(waitMs);

  // Poll
  const pollResult = await callExtension("poll", { sub_id: subId, limit });
  const pollData = pollResult.data || pollResult;
  const samples = pollData.samples || [];

  // Unsubscribe
  await callExtension("unsubscribe", { sub_id: subId });

  if (samples.length === 0) {
    return {
      content: [
        {
          type: "text",
          text: `No samples received on "${keyExpr}" within ${waitMs}ms. The topic may be inactive.`,
        },
      ],
      is_error: false,
    };
  }

  const lines = samples.map((s, i) => {
    const payload = s.payload_str || `[base64] ${s.payload_b64}`;
    return `[${i + 1}] ${s.key_expr} (${s.encoding}): ${payload}`;
  });

  let text = `${samples.length} sample(s) from "${keyExpr}":\n${lines.join("\n")}`;
  if (pollData.overflow_count > 0) {
    text += `\n(${pollData.overflow_count} samples dropped due to buffer overflow)`;
  }

  return {
    content: [{ type: "text", text }],
    is_error: false,
  };
}

// ── Server ─────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok" }));
    return;
  }

  // Config endpoint — short-lived access token for the frontend
  if (req.url === "/api/config") {
    nexus
      .getAccessToken()
      .then(() => {
        res.writeHead(200, {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        });
        res.end(JSON.stringify(nexus.getClientConfig()));
      })
      .catch((err) => {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: err.message }));
      });
    return;
  }

  // MCP tool call handler
  if (req.method === "POST" && req.url === "/mcp/call") {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", async () => {
      try {
        const { tool_name, arguments: args } = JSON.parse(body);
        let result;

        switch (tool_name) {
          case "discover_zenoh_topics":
            result = await discoverZenohTopics(args || {});
            break;
          case "read_zenoh_topic":
            result = await readZenohTopic(args || {});
            break;
          default:
            result = {
              content: [{ type: "text", text: `Unknown tool: ${tool_name}` }],
              is_error: true,
            };
        }

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            content: [{ type: "text", text: `Error: ${err.message}` }],
            is_error: true,
          })
        );
      }
    });
    return;
  }

  // Serve index.html with API URL templated in
  if (req.url === "/" || req.url === "/index.html") {
    const html = fs
      .readFileSync(path.join(publicDir, "index.html"), "utf8")
      .replace(/\{\{NEXUS_API_URL\}\}/g, nexus.apiUrl);
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(html);
    return;
  }

  // Serve other static files
  const fullPath = path.join(publicDir, req.url);
  const ext = path.extname(fullPath);
  const contentType = MIME_TYPES[ext] || "application/octet-stream";

  fs.readFile(fullPath, (err, data) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("Not Found");
      return;
    }
    res.writeHead(200, { "Content-Type": contentType });
    res.end(data);
  });
});

server.listen(PORT, () => {
  console.log(`Zenoh Viewer plugin running on port ${PORT}`);
});
