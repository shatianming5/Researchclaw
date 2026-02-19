import type { FrameworkAdapter } from "./types.js";
import {
  renderDiscoverPlanDirCommand,
  renderExtractDetectron2EvalMetricsCommand,
  renderExportStandardCacheEnvCommand,
  renderWritePipFreezeCommand,
} from "./shell.js";

function pickConfig(profileConfigs: string[]): string | null {
  return profileConfigs[0] ?? null;
}

export const detectron2Adapter: FrameworkAdapter = {
  id: "detectron2",
  buildTemplates(ctx) {
    const configPath = pickConfig(ctx.profile.configCandidates.detectron2);
    const configGuard = configPath
      ? [
          `CFG="${configPath}"`,
          'if [ ! -f "$CFG" ]; then echo "Missing Detectron2 config: $CFG" >&2; exit 1; fi',
        ].join("\n")
      : [
          'echo "No configs/*.yaml found for Detectron2; cannot pick a config automatically." >&2',
          "exit 1",
        ].join("\n");

    const gpuCount = Math.max(1, Math.floor(ctx.gpuCount ?? 1));

    const prologue: string[] = [
      renderDiscoverPlanDirCommand(),
      `export OPENCLAW_OUTPUT_DIR="$OPENCLAW_PLAN_DIR/${ctx.outputDirRel}"`,
      renderExportStandardCacheEnvCommand(),
      `VENV="$OPENCLAW_PLAN_DIR/cache/venv/${ctx.repoKey}"`,
    ];

    const ensureDirs =
      'mkdir -p "$OPENCLAW_OUTPUT_DIR" "$OPENCLAW_PLAN_DIR/report" "$OPENCLAW_PLAN_DIR/cache/venv" "$OPENCLAW_PLAN_DIR/cache/pip"';

    const setup: string[] = [
      ...prologue,
      ensureDirs,
      'if [ ! -d "$VENV" ]; then python3 -m venv "$VENV"; fi',
      '. "$VENV/bin/activate"',
      "python -m pip install -U pip",
    ];

    const install: string[] = [
      ...prologue,
      ensureDirs,
      'if [ ! -d "$VENV" ]; then python3 -m venv "$VENV"; fi',
      '. "$VENV/bin/activate"',
      "if [ -f requirements.txt ]; then python -m pip install -r requirements.txt; elif [ -f pyproject.toml ]; then python -m pip install -e .; else python -m pip install -e .; fi",
      ...renderWritePipFreezeCommand({ repoKey: ctx.repoKey }),
    ];

    const train: string[] = [
      ...prologue,
      ensureDirs,
      '. "$VENV/bin/activate"',
      configGuard,
      'RESUME_ARGS=""',
      'if [ -f "$OPENCLAW_OUTPUT_DIR/last_checkpoint" ]; then RESUME_ARGS="--resume"; fi',
      'if [ ! -f tools/train_net.py ]; then echo "Missing tools/train_net.py" >&2; exit 1; fi',
      `python3 tools/train_net.py --config-file "$CFG" --num-gpus ${gpuCount} $RESUME_ARGS OUTPUT_DIR "$OPENCLAW_OUTPUT_DIR"`,
      'echo "training finished (detectron2 adapter)"',
    ];

    const evalCommands: string[] = [
      ...prologue,
      ensureDirs,
      '. "$VENV/bin/activate"',
      configGuard,
      'WEIGHTS=""',
      'if [ -f "$OPENCLAW_OUTPUT_DIR/model_final.pth" ]; then WEIGHTS="$OPENCLAW_OUTPUT_DIR/model_final.pth"; fi',
      'if [ -z "$WEIGHTS" ]; then',
      '  for f in "$OPENCLAW_OUTPUT_DIR"/model_*.pth; do',
      '    if [ -f "$f" ]; then WEIGHTS="$f"; break; fi',
      "  done",
      "fi",
      'if [ -z "$WEIGHTS" ]; then echo "No weights found under $OPENCLAW_OUTPUT_DIR; continuing to metrics extraction" >&2; fi',
      `if [ -f tools/train_net.py ] && [ -n "$WEIGHTS" ]; then python3 tools/train_net.py --config-file "$CFG" --num-gpus ${gpuCount} --eval-only MODEL.WEIGHTS "$WEIGHTS" OUTPUT_DIR "$OPENCLAW_OUTPUT_DIR" || true; fi`,
      renderExtractDetectron2EvalMetricsCommand(),
    ];

    return {
      outputDirRel: ctx.outputDirRel,
      env: {},
      setup,
      install,
      train,
      eval: evalCommands,
      notes: [],
      warnings: configPath
        ? []
        : ["No configs/*.yaml detected; Detectron2 adapter cannot auto-select a config."],
    };
  },
};
