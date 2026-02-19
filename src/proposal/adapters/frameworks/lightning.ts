import type { FrameworkAdapter } from "./types.js";
import {
  renderDiscoverPlanDirCommand,
  renderExportStandardCacheEnvCommand,
  renderExtractLightningEvalMetricsCommand,
  renderWritePipFreezeCommand,
} from "./shell.js";

function pickFirstMatch(candidates: string[], re: RegExp): string | null {
  for (const entry of candidates) {
    if (re.test(entry)) {
      return entry;
    }
  }
  return candidates[0] ?? null;
}

function resolveTrainEntrypoint(entrypoints: string[]): string | null {
  return pickFirstMatch(entrypoints, /(?:^|\/)train\.py$/i);
}

function resolveEvalEntrypoint(entrypoints: string[]): string | null {
  return pickFirstMatch(entrypoints, /(?:^|\/)(eval|evaluate)\.py$/i);
}

export const lightningAdapter: FrameworkAdapter = {
  id: "lightning",
  buildTemplates(ctx) {
    const trainEntrypoint =
      resolveTrainEntrypoint(ctx.profile.entrypointHints.train) ??
      resolveTrainEntrypoint(ctx.profile.entrypoints);
    const evalEntrypoint =
      resolveEvalEntrypoint(ctx.profile.entrypointHints.eval) ??
      resolveEvalEntrypoint(ctx.profile.entrypoints);

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

    const resumeLogic = [
      'CKPT=""',
      'for f in "$OPENCLAW_OUTPUT_DIR"/*.ckpt; do',
      '  if [ -f "$f" ]; then CKPT="$f"; break; fi',
      "done",
      'RESUME_ARGS=""',
      'if [ -n "$CKPT" ]; then RESUME_ARGS="--resume_from_checkpoint $CKPT"; fi',
    ].join("\n");

    const train: string[] = trainEntrypoint
      ? [
          ...prologue,
          ensureDirs,
          '. "$VENV/bin/activate"',
          resumeLogic,
          `HELP_OUT="$(python3 "${trainEntrypoint}" --help 2>/dev/null || true)"`,
          'EXTRA_ARGS=""',
          'if echo "$HELP_OUT" | grep -q -- "--default_root_dir"; then EXTRA_ARGS="$EXTRA_ARGS --default_root_dir $OPENCLAW_OUTPUT_DIR"; fi',
          `python3 "${trainEntrypoint}" $RESUME_ARGS $EXTRA_ARGS`,
          'echo "training finished (lightning adapter)"',
        ]
      : [
          renderDiscoverPlanDirCommand(),
          'echo "No Lightning train entrypoint found (expected train.py)." >&2',
          "exit 1",
        ];

    const evalRun = evalEntrypoint
      ? [
          `EVAL_EXIT=""`,
          `python3 "${evalEntrypoint}" || EVAL_EXIT="$?"`,
          'if [ -n "$EVAL_EXIT" ]; then echo "eval failed (exit=$EVAL_EXIT); continuing to metrics extraction" >&2; fi',
        ].join("\n")
      : 'echo "No eval.py found; extracting metrics only" >&2';

    const evalCmds: string[] = [
      ...prologue,
      ensureDirs,
      '. "$VENV/bin/activate"',
      evalRun,
      renderExtractLightningEvalMetricsCommand(),
    ];

    return {
      outputDirRel: ctx.outputDirRel,
      env: {},
      setup,
      install,
      train,
      eval: evalCmds,
      notes: [],
      warnings: [],
    };
  },
};
