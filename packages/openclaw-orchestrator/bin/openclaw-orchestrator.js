#!/usr/bin/env node
import { createServer } from "node:http";
import process from "node:process";
import { callGateway } from "openclaw/gateway-call";

function getArg(name) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) {
    return null;
  }
  const next = process.argv[idx + 1];
  return typeof next === "string" && !next.startsWith("--") ? next : "";
}

function readJson(req, maxBytes = 2_000_000) {
  return new Promise((resolve) => {
    let size = 0;
    const chunks = [];
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        req.destroy();
        resolve({ ok: false, error: "payload too large" });
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf-8");
        resolve({ ok: true, value: raw ? JSON.parse(raw) : {} });
      } catch {
        resolve({ ok: false, error: "invalid json" });
      }
    });
    req.on("error", () => resolve({ ok: false, error: "request error" }));
  });
}

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

function normalizeString(value) {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

async function main() {
  const port = Number(getArg("--port") || process.env.PORT || "8787");
  const url = normalizeString(getArg("--url") || process.env.OPENCLAW_GATEWAY_URL);
  const token = normalizeString(getArg("--token") || process.env.OPENCLAW_GATEWAY_TOKEN);
  const password = normalizeString(getArg("--password") || process.env.OPENCLAW_GATEWAY_PASSWORD);
  const tlsFingerprint = normalizeString(
    getArg("--tlsFingerprint") || process.env.OPENCLAW_GATEWAY_TLS_FINGERPRINT,
  );
  const timeoutMs = Math.max(
    1_000,
    Math.floor(Number(getArg("--timeoutMs") || process.env.OPENCLAW_GATEWAY_TIMEOUT_MS || "30000")),
  );

  if (!url) {
    console.error(
      'Missing gateway url. Provide --url or set OPENCLAW_GATEWAY_URL (e.g. "ws://127.0.0.1:8788").',
    );
    process.exit(1);
  }
  if (!token && !password) {
    console.error(
      "Missing gateway credentials. Provide --token/--password or set OPENCLAW_GATEWAY_TOKEN/OPENCLAW_GATEWAY_PASSWORD.",
    );
    process.exit(1);
  }

  const server = createServer(async (req, res) => {
    const method = (req.method || "GET").toUpperCase();
    const urlObj = new URL(req.url || "/", "http://localhost");
    if (method === "GET" && urlObj.pathname === "/health") {
      sendJson(res, 200, { ok: true });
      return;
    }

    if (method === "POST" && urlObj.pathname === "/proposal/compile") {
      const body = await readJson(req);
      if (!body.ok) {
        const status = body.error === "payload too large" ? 413 : 400;
        sendJson(res, status, { ok: false, error: body.error });
        return;
      }
      const payload = body.value && typeof body.value === "object" ? body.value : {};

      const proposalMarkdown = normalizeString(payload.proposalMarkdown);
      if (!proposalMarkdown) {
        sendJson(res, 400, {
          ok: false,
          error: "proposalMarkdown (string) required",
        });
        return;
      }

      try {
        const result = await callGateway({
          url,
          token: token || undefined,
          password: password || undefined,
          tlsFingerprint: tlsFingerprint || undefined,
          timeoutMs,
          method: "proposal.compile",
          params: {
            proposalMarkdown,
            discovery: normalizeString(payload.discovery) || undefined,
            agentId: normalizeString(payload.agentId) || undefined,
            workspaceDir: normalizeString(payload.workspaceDir) || undefined,
            outDir: normalizeString(payload.outDir) || undefined,
            modelOverride: normalizeString(payload.modelOverride) || undefined,
            useLlm: typeof payload.useLlm === "boolean" ? payload.useLlm : undefined,
          },
        });
        sendJson(res, 200, { ok: true, result });
      } catch (err) {
        sendJson(res, 500, { ok: false, error: String(err instanceof Error ? err.message : err) });
      }
      return;
    }

    sendJson(res, 404, { ok: false, error: "not found" });
  });

  server.listen(port, () => {
    console.log(`[openclaw-orchestrator] listening on http://127.0.0.1:${port}`);
  });
}

void main();
