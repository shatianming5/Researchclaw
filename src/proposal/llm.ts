import { completeSimple } from "@mariozechner/pi-ai";
import type { OpenClawConfig } from "../config/config.js";
import { getApiKeyForModel, requireApiKey } from "../agents/model-auth.js";
import {
  buildModelAliasIndex,
  resolveDefaultModelForAgent,
  resolveModelRefFromString,
} from "../agents/model-selection.js";
import { resolveModel } from "../agents/pi-embedded-runner/model.js";

type TextContent = { type: "text"; text: string };

function isTextContentBlock(block: { type: string }): block is TextContent {
  return block.type === "text";
}

export type ProposalLlmClient = {
  modelKey: string;
  completeText: (params: {
    prompt: string;
    maxTokens?: number;
    temperature?: number;
    timeoutMs?: number;
  }) => Promise<string>;
};

export type ResolveProposalLlmResult =
  | { ok: true; client: ProposalLlmClient }
  | { ok: false; error: string; modelKey?: string };

function resolveModelKey(cfg: OpenClawConfig, modelOverride?: string, agentId?: string): string {
  const defaultRef = resolveDefaultModelForAgent({ cfg, agentId });
  if (!modelOverride?.trim()) {
    return `${defaultRef.provider}/${defaultRef.model}`;
  }
  const aliasIndex = buildModelAliasIndex({ cfg, defaultProvider: defaultRef.provider });
  const resolved = resolveModelRefFromString({
    raw: modelOverride,
    defaultProvider: defaultRef.provider,
    aliasIndex,
  });
  const ref = resolved?.ref ?? defaultRef;
  return `${ref.provider}/${ref.model}`;
}

function resolveModelRef(cfg: OpenClawConfig, modelOverride?: string, agentId?: string) {
  const defaultRef = resolveDefaultModelForAgent({ cfg, agentId });
  if (!modelOverride?.trim()) {
    return defaultRef;
  }
  const aliasIndex = buildModelAliasIndex({ cfg, defaultProvider: defaultRef.provider });
  const resolved = resolveModelRefFromString({
    raw: modelOverride,
    defaultProvider: defaultRef.provider,
    aliasIndex,
  });
  return resolved?.ref ?? defaultRef;
}

export async function resolveProposalLlmClient(params: {
  cfg: OpenClawConfig;
  modelOverride?: string;
  agentId?: string;
}): Promise<ResolveProposalLlmResult> {
  const { cfg, modelOverride, agentId } = params;
  const modelKey = resolveModelKey(cfg, modelOverride, agentId);
  const ref = resolveModelRef(cfg, modelOverride, agentId);
  const resolved = resolveModel(ref.provider, ref.model, undefined, cfg);
  const model = resolved.model;
  if (!model) {
    return { ok: false, error: resolved.error ?? `Unknown model: ${modelKey}`, modelKey };
  }

  let apiKey: string;
  try {
    apiKey = requireApiKey(await getApiKeyForModel({ model, cfg }), ref.provider);
  } catch (err) {
    return { ok: false, error: String(err), modelKey };
  }

  const client: ProposalLlmClient = {
    modelKey,
    completeText: async ({ prompt, maxTokens, temperature, timeoutMs }) => {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), timeoutMs ?? 60_000);
      try {
        const res = await completeSimple(
          model,
          {
            messages: [
              {
                role: "user",
                content: prompt,
                timestamp: Date.now(),
              },
            ],
          },
          {
            apiKey,
            maxTokens,
            temperature,
            signal: controller.signal,
          },
        );

        const text = res.content
          .filter(isTextContentBlock)
          .map((block) => block.text)
          .join("")
          .trim();
        if (!text) {
          throw new Error("LLM returned empty response");
        }
        return text;
      } finally {
        clearTimeout(timeout);
      }
    },
  };

  return { ok: true, client };
}
