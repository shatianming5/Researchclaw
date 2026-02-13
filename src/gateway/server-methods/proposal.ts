import fs from "node:fs/promises";
import type { GatewayRequestHandlers } from "./types.js";
import { resolveAgentWorkspaceDir, resolveDefaultAgentId } from "../../agents/agent-scope.js";
import { loadConfig } from "../../config/config.js";
import { compileProposal } from "../../proposal/compiler.js";
import { DiscoveryModeSchema } from "../../proposal/schema.js";
import { resolveUserPath } from "../../utils.js";
import { ErrorCodes, errorShape } from "../protocol/index.js";

function requireNonEmptyString(
  value: unknown,
  field: string,
): { ok: true; value: string } | { ok: false; error: string } {
  if (typeof value !== "string") {
    return { ok: false, error: `${field} (string) required` };
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return { ok: false, error: `${field} (non-empty string) required` };
  }
  return { ok: true, value: trimmed };
}

function optionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

export const proposalHandlers: GatewayRequestHandlers = {
  "proposal.compile": async ({ params, respond }) => {
    const proposalMarkdownRes = requireNonEmptyString(params.proposalMarkdown, "proposalMarkdown");
    if (!proposalMarkdownRes.ok) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid proposal.compile params: ${proposalMarkdownRes.error}`,
        ),
      );
      return;
    }

    const discoveryRaw = optionalString(params.discovery) ?? "plan";
    const discoveryParsed = DiscoveryModeSchema.safeParse(discoveryRaw);
    if (!discoveryParsed.success) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          'invalid proposal.compile params: discovery must be one of "off" | "plan" | "sample"',
        ),
      );
      return;
    }

    const cfg = loadConfig();
    const agentId = optionalString(params.agentId) ?? resolveDefaultAgentId(cfg);
    const workspaceDir =
      optionalString(params.workspaceDir) ?? resolveAgentWorkspaceDir(cfg, agentId);
    const resolvedWorkspaceDir = resolveUserPath(workspaceDir);
    await fs.mkdir(resolvedWorkspaceDir, { recursive: true });

    const outDirRaw = optionalString(params.outDir);
    const outDir = outDirRaw ? resolveUserPath(outDirRaw) : undefined;
    const modelOverride = optionalString(params.modelOverride);

    const useLlmRaw = params.useLlm;
    const useLlm = typeof useLlmRaw === "boolean" ? useLlmRaw : true;

    const result = await compileProposal({
      proposalMarkdown: proposalMarkdownRes.value,
      proposalSource: "gateway:proposal.compile",
      cfg,
      agentId,
      workspaceDir: resolvedWorkspaceDir,
      outDir,
      discovery: discoveryParsed.data,
      modelOverride,
      useLlm,
    });

    respond(
      true,
      {
        ok: result.ok,
        planId: result.planId,
        rootDir: result.rootDir,
        report: result.report,
        paths: result.paths,
      },
      undefined,
    );
  },
};
