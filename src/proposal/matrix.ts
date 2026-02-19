import crypto from "node:crypto";
import type { DagPatchOp, ExperimentOverrides } from "./design.schema.js";

export type ExperimentAxisLevel = {
  axisId: string;
  axisName: string;
  levelId: string;
  levelName: string;
  rationale?: string;
  overrides: ExperimentOverrides;
  dagPatchOps: DagPatchOp[];
};

export type ExpandedExperimentVariant = {
  id: string;
  name: string;
  rationale?: string;
  overrides: ExperimentOverrides;
  dagPatchOps: DagPatchOp[];
  levels: Array<{ axisId: string; levelId: string }>;
};

function sanitizeIdPart(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replaceAll(/[^a-z0-9._-]+/g, "-")
    .replaceAll(/-+/g, "-")
    .replaceAll(/^-|-$/g, "");
}

function stableSuffix(text: string, chars = 6): string {
  return crypto.createHash("sha256").update(text).digest("hex").slice(0, chars);
}

function mergeOverrides(list: ExperimentOverrides[]): ExperimentOverrides {
  const merged: ExperimentOverrides = {};
  for (const overrides of list) {
    if (!overrides) {
      continue;
    }
    if (overrides.env) {
      merged.env = merged.env ? { ...merged.env, ...overrides.env } : { ...overrides.env };
    }
    if (overrides.resources) {
      merged.resources = merged.resources
        ? { ...merged.resources, ...overrides.resources }
        : { ...overrides.resources };
    }
    if (overrides.acceptance) {
      // If multiple levels provide acceptance overrides, the later level wins (axis order).
      merged.acceptance = overrides.acceptance;
    }
  }
  return merged;
}

function cartesianProduct<T>(axes: T[][]): T[][] {
  if (axes.length === 0) {
    return [[]];
  }
  let acc: T[][] = [[]];
  for (const axis of axes) {
    const next: T[][] = [];
    for (const prefix of acc) {
      for (const value of axis) {
        next.push([...prefix, value]);
      }
    }
    acc = next;
  }
  return acc;
}

export function expandExperimentAxes(params: {
  axes: Array<{
    id: string;
    name: string;
    levels: Array<{
      id: string;
      name: string;
      rationale?: string;
      overrides?: ExperimentOverrides;
      dagPatchOps?: DagPatchOp[];
    }>;
  }>;
  maxVariants: number;
}): { variants: ExpandedExperimentVariant[]; warnings: string[] } {
  const warnings: string[] = [];
  const maxVariants = Math.max(0, Math.floor(params.maxVariants));

  const axes = (params.axes ?? [])
    .map((axis) => ({
      axisId: axis.id,
      axisName: axis.name,
      levels: (axis.levels ?? []).map((level) => ({
        axisId: axis.id,
        axisName: axis.name,
        levelId: level.id,
        levelName: level.name,
        rationale: level.rationale,
        overrides: level.overrides ?? {},
        dagPatchOps: level.dagPatchOps ?? [],
      })),
    }))
    .filter((axis) => axis.axisId.trim() && axis.levels.length > 0);

  const combos = cartesianProduct(axes.map((a) => a.levels));
  if (maxVariants > 0 && combos.length > maxVariants) {
    warnings.push(
      `Axis expansion produced ${combos.length} variants; truncating to ${maxVariants}.`,
    );
  }

  const used = new Set<string>();
  const out: ExpandedExperimentVariant[] = [];

  const take = maxVariants > 0 ? combos.slice(0, maxVariants) : combos;
  for (const levels of take) {
    const pairs = levels.map((l) => `${sanitizeIdPart(l.axisId)}-${sanitizeIdPart(l.levelId)}`);
    const baseId = pairs.filter(Boolean).join("__") || `variant-${out.length + 1}`;
    let id = baseId;
    if (used.has(id)) {
      id = `${baseId}-${stableSuffix(pairs.join("|"))}`;
    }
    let i = 2;
    while (used.has(id)) {
      id = `${baseId}-${i}`;
      i += 1;
    }
    used.add(id);

    const overrides = mergeOverrides(levels.map((l) => l.overrides));
    const dagPatchOps = levels.flatMap((l) => l.dagPatchOps);
    const name =
      levels
        .map((l) => l.levelName)
        .filter(Boolean)
        .join(" + ") || id;
    const rationale = levels
      .map((l) => (l.rationale ?? "").trim())
      .filter(Boolean)
      .join(" / ");

    out.push({
      id,
      name,
      rationale: rationale || undefined,
      overrides,
      dagPatchOps,
      levels: levels.map((l) => ({ axisId: l.axisId, levelId: l.levelId })),
    });
  }

  return { variants: out, warnings };
}
