import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { runCommandWithTimeout } from "../../../process/exec.js";
import {
  renderExtractDetectron2EvalMetricsCommand,
  renderExtractLightningEvalMetricsCommand,
  renderExtractMmengineEvalMetricsCommand,
  renderExtractTransformersEvalMetricsCommand,
} from "./shell.js";

const itUnix = process.platform === "win32" ? it.skip : it;

async function withTempPlanDir(
  fn: (params: { planDir: string; outputDir: string }) => Promise<void>,
): Promise<void> {
  const planDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-metrics-"));
  const outputDir = path.join(planDir, "artifacts", "model", "repo");
  await fs.mkdir(path.join(planDir, "report"), { recursive: true });
  await fs.mkdir(outputDir, { recursive: true });
  await fn({ planDir, outputDir });
}

async function runExtractor(params: { planDir: string; outputDir: string; script: string }) {
  return await runCommandWithTimeout(["sh", "-lc", params.script], {
    cwd: params.planDir,
    timeoutMs: 20_000,
    env: {
      OPENCLAW_PLAN_DIR: params.planDir,
      OPENCLAW_OUTPUT_DIR: params.outputDir,
    },
  });
}

describe("proposal/adapters/frameworks metrics extractors", () => {
  itUnix("extracts Lightning metrics from metrics.csv", async () => {
    await withTempPlanDir(async ({ planDir, outputDir }) => {
      const csvPath = path.join(outputDir, "lightning_logs", "version_0", "metrics.csv");
      await fs.mkdir(path.dirname(csvPath), { recursive: true });
      await fs.writeFile(
        csvPath,
        ["step,epoch,val_acc,val_loss", "1,0,0.5,2.0", "2,0,0.9,1.5"].join("\n") + "\n",
        "utf-8",
      );

      const res = await runExtractor({
        planDir,
        outputDir,
        script: renderExtractLightningEvalMetricsCommand(),
      });
      expect(res.code).toBe(0);

      const raw = await fs.readFile(path.join(planDir, "report", "eval_metrics.json"), "utf-8");
      const parsed = JSON.parse(raw) as { metrics?: Record<string, unknown> };
      expect(Number(parsed.metrics?.val_acc)).toBeCloseTo(0.9);
    });
  });

  itUnix("extracts MMEngine metrics from vis_data/scalars.json", async () => {
    await withTempPlanDir(async ({ planDir, outputDir }) => {
      const scalarsPath = path.join(outputDir, "vis_data", "scalars.json");
      await fs.mkdir(path.dirname(scalarsPath), { recursive: true });
      await fs.writeFile(
        scalarsPath,
        JSON.stringify(
          [
            { tag: "loss", step: 1, value: 9.9 },
            { tag: "coco/bbox_mAP", step: 1, value: 0.123 },
            { tag: "coco/bbox_mAP", step: 2, value: 0.456 },
          ],
          null,
          2,
        ) + "\n",
        "utf-8",
      );

      const res = await runExtractor({
        planDir,
        outputDir,
        script: renderExtractMmengineEvalMetricsCommand(),
      });
      expect(res.code).toBe(0);

      const raw = await fs.readFile(path.join(planDir, "report", "eval_metrics.json"), "utf-8");
      const parsed = JSON.parse(raw) as { metrics?: Record<string, unknown> };
      expect(Number(parsed.metrics?.["coco/bbox_mAP"])).toBeCloseTo(0.456);
    });
  });

  itUnix("extracts Detectron2 metrics from metrics.json", async () => {
    await withTempPlanDir(async ({ planDir, outputDir }) => {
      const metricsPath = path.join(outputDir, "metrics.json");
      await fs.writeFile(
        metricsPath,
        [
          JSON.stringify({ iteration: 1, total_loss: 9.9 }),
          JSON.stringify({ iteration: 2, "bbox/AP": 35.6, "bbox/AP50": 55.1 }),
        ].join("\n") + "\n",
        "utf-8",
      );

      const res = await runExtractor({
        planDir,
        outputDir,
        script: renderExtractDetectron2EvalMetricsCommand(),
      });
      expect(res.code).toBe(0);

      const raw = await fs.readFile(path.join(planDir, "report", "eval_metrics.json"), "utf-8");
      const parsed = JSON.parse(raw) as { metrics?: Record<string, unknown> };
      expect(Number(parsed.metrics?.["bbox/AP"])).toBeCloseTo(35.6);
    });
  });

  itUnix("extracts Transformers metrics from eval_results.json", async () => {
    await withTempPlanDir(async ({ planDir, outputDir }) => {
      const resultsPath = path.join(outputDir, "eval_results.json");
      await fs.writeFile(
        resultsPath,
        JSON.stringify({ eval_accuracy: 0.77, eval_loss: 1.2, epoch: 1 }, null, 2) + "\n",
        "utf-8",
      );

      const res = await runExtractor({
        planDir,
        outputDir,
        script: renderExtractTransformersEvalMetricsCommand(),
      });
      expect(res.code).toBe(0);

      const raw = await fs.readFile(path.join(planDir, "report", "eval_metrics.json"), "utf-8");
      const parsed = JSON.parse(raw) as { metrics?: Record<string, unknown> };
      expect(Number(parsed.metrics?.eval_accuracy)).toBeCloseTo(0.77);
    });
  });
});
