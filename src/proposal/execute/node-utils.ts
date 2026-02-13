import path from "node:path";
import type { PlanNode } from "../schema.js";

export function inferHostWorkdir(planDir: string, node: PlanNode): string {
  const input = (node.inputs?.[0] ?? "").trim().replaceAll("\\", "/");
  if (input.startsWith("cache/git/")) {
    return path.join(planDir, input);
  }
  return planDir;
}

export function isGpuNode(node: PlanNode): boolean {
  if (node.type === "train" || node.type === "eval") {
    return true;
  }
  const gpu = node.resources?.gpuCount;
  return typeof gpu === "number" && Number.isFinite(gpu) && gpu > 0;
}
