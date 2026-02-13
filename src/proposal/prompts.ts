import type { DiscoveryReport, ProposalEntities } from "./schema.js";

export function buildEntitiesExtractionPrompt(proposalMarkdown: string): string {
  const clipped =
    proposalMarkdown.length > 80_000
      ? `${proposalMarkdown.slice(0, 80_000)}\n...<clipped>`
      : proposalMarkdown;

  return (
    `You are a strict information extraction engine.\n` +
    `Extract a structured JSON object from the proposal markdown.\n\n` +
    `Rules:\n` +
    `- Return ONLY valid JSON (no markdown fences).\n` +
    `- Do NOT invent repo/dataset names, URLs, metrics, or thresholds.\n` +
    `- If something is uncertain, omit it or put it in hintText/notes.\n` +
    `- Keep arrays empty when unknown.\n\n` +
    `Required JSON shape:\n` +
    `{\n` +
    `  "repos": [{ "name"?: string, "url"?: string, "hintText"?: string, "branch"?: string, "commit"?: string }],\n` +
    `  "datasets": [{ "name"?: string, "url"?: string, "platform"?: "hf"|"kaggle"|"url"|"unknown", "hintText"?: string }],\n` +
    `  "metrics": [{ "name": string, "goal"?: "min"|"max", "target"?: number|string, "unit"?: string, "sourceText"?: string }],\n` +
    `  "constraints"?: { "gpu"?: number|string, "cpu"?: number|string, "memoryGB"?: number, "diskGB"?: number, "maxHours"?: number, "frameworkHints"?: string[] },\n` +
    `  "deliverables": string[],\n` +
    `  "notes"?: string\n` +
    `}\n\n` +
    `<proposal_markdown>\n${clipped}\n</proposal_markdown>`
  );
}

export function buildAcceptanceSuggestionPrompt(params: {
  entities: ProposalEntities;
  discovery: DiscoveryReport;
}): string {
  return (
    `You generate acceptance checks for an experiment plan.\n` +
    `Return ONLY valid JSON (no markdown).\n\n` +
    `Input:\n` +
    `- metrics extracted from proposal\n` +
    `- discovered repos/datasets (may be missing)\n\n` +
    `Output JSON shape:\n` +
    `{\n` +
    `  "checks": [\n` +
    `    {\n` +
    `      "type": "metric_threshold"|"artifact_exists"|"command_exit_code"|"manual_approval",\n` +
    `      "selector": string,\n` +
    `      "op"?: ">="|"<="|"=="|">"|"<"|"!=",\n` +
    `      "value"?: number|string,\n` +
    `      "unit"?: string,\n` +
    `      "needs_confirm"?: boolean,\n` +
    `      "suggested_by"?: "proposal"|"llm"|"network_evidence"|"compiler",\n` +
    `      "evidence"?: string[],\n` +
    `      "description"?: string\n` +
    `    }\n` +
    `  ]\n` +
    `}\n\n` +
    `Rules:\n` +
    `- If proposal does NOT specify a numeric target for a metric, you MAY suggest one but MUST set needs_confirm=true.\n` +
    `- Prefer artifact checks when metrics are unclear.\n\n` +
    `Entities:\n${JSON.stringify(params.entities, null, 2)}\n\n` +
    `Discovery:\n${JSON.stringify(params.discovery, null, 2)}\n`
  );
}
