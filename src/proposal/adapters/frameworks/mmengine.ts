import type { FrameworkAdapter } from "./types.js";
import {
  renderDiscoverPlanDirCommand,
  renderExtractMmengineEvalMetricsCommand,
  renderExportStandardCacheEnvCommand,
  renderWritePipFreezeCommand,
} from "./shell.js";

function pickConfig(profileConfigs: string[]): string | null {
  return profileConfigs[0] ?? null;
}

export const mmengineAdapter: FrameworkAdapter = {
  id: "mmengine",
  buildTemplates(ctx) {
    const configPath = pickConfig(ctx.profile.configCandidates.mmengine);
    const configGuard = configPath
      ? [
          `CFG="${configPath}"`,
          'if [ ! -f "$CFG" ]; then echo "Missing MMEngine config: $CFG" >&2; exit 1; fi',
        ].join("\n")
      : [
          'echo "No configs/*.py found for MMEngine; cannot pick a config automatically." >&2',
          "exit 1",
        ].join("\n");

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
      'if [ -f "$OPENCLAW_OUTPUT_DIR/latest.pth" ]; then RESUME_ARGS="--resume"; fi',
      'if [ ! -f tools/train.py ]; then echo "Missing tools/train.py" >&2; exit 1; fi',
      'python3 tools/train.py "$CFG" --work-dir "$OPENCLAW_OUTPUT_DIR" --launcher none $RESUME_ARGS',
      'echo "training finished (mmengine adapter)"',
    ];

    const evalCommands: string[] = [
      ...prologue,
      ensureDirs,
      '. "$VENV/bin/activate"',
      configGuard,
      'CKPT=""',
      'if [ -f "$OPENCLAW_OUTPUT_DIR/latest.pth" ]; then CKPT="$OPENCLAW_OUTPUT_DIR/latest.pth"; fi',
      'if [ -z "$CKPT" ]; then',
      '  for f in "$OPENCLAW_OUTPUT_DIR"/*.pth; do',
      '    if [ -f "$f" ]; then CKPT="$f"; break; fi',
      "  done",
      "fi",
      'if [ -z "$CKPT" ]; then echo "No checkpoint found under $OPENCLAW_OUTPUT_DIR; continuing to metrics extraction" >&2; fi',
      'if [ -f tools/test.py ] && [ -n "$CKPT" ]; then python3 tools/test.py "$CFG" "$CKPT" --work-dir "$OPENCLAW_OUTPUT_DIR" --launcher none || true; fi',
      renderExtractMmengineEvalMetricsCommand(),
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
        : ["No configs/*.py detected; MMEngine adapter cannot auto-select a config."],
    };
  },
};
