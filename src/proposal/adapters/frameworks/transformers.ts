import type { FrameworkAdapter } from "./types.js";
import {
  renderDiscoverPlanDirCommand,
  renderExportStandardCacheEnvCommand,
  renderExtractTransformersEvalMetricsCommand,
  renderWritePipFreezeCommand,
} from "./shell.js";

type TransformersEntrypointKind = "hf_glue" | "hf_qa" | "hf_clm" | "hf_mlm" | "custom";

type PickedEntrypoint = { path: string; kind: TransformersEntrypointKind };

function uniqueNonEmpty(values: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed) {
      continue;
    }
    if (seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    out.push(trimmed);
  }
  return out;
}

function pickFirstMatch(candidates: string[], re: RegExp): string | null {
  for (const entry of candidates) {
    if (re.test(entry)) {
      return entry;
    }
  }
  return null;
}

function pickHfExampleEntrypoint(candidates: string[]): PickedEntrypoint | null {
  const glue = pickFirstMatch(candidates, /(?:^|\/)run_glue\.py$/i);
  if (glue) {
    return { path: glue, kind: "hf_glue" };
  }
  const qa = pickFirstMatch(candidates, /(?:^|\/)run_qa\.py$/i);
  if (qa) {
    return { path: qa, kind: "hf_qa" };
  }
  const clm = pickFirstMatch(candidates, /(?:^|\/)run_clm\.py$/i);
  if (clm) {
    return { path: clm, kind: "hf_clm" };
  }
  const mlm = pickFirstMatch(candidates, /(?:^|\/)run_mlm\.py$/i);
  if (mlm) {
    return { path: mlm, kind: "hf_mlm" };
  }
  return null;
}

function resolveTrainEntrypoint(candidates: string[]): PickedEntrypoint | null {
  const hf = pickHfExampleEntrypoint(candidates);
  if (hf) {
    return hf;
  }
  const trainPy = pickFirstMatch(candidates, /(?:^|\/)train\.py$/i);
  if (trainPy) {
    return { path: trainPy, kind: "custom" };
  }
  return null;
}

function resolveEvalEntrypoint(candidates: string[]): string | null {
  return (
    pickFirstMatch(candidates, /(?:^|\/)(eval|evaluate)\.py$/i) ??
    pickFirstMatch(candidates, /(?:^|\/)(test|predict)\.py$/i)
  );
}

export const transformersAdapter: FrameworkAdapter = {
  id: "transformers",
  buildTemplates(ctx) {
    const candidates = uniqueNonEmpty([
      ...ctx.profile.entrypointHints.train,
      ...ctx.profile.entrypointHints.eval,
      ...ctx.profile.entrypoints,
      ...ctx.profile.fileSample,
    ]);

    const trainEntrypoint = resolveTrainEntrypoint(candidates);
    const evalEntrypoint = resolveEvalEntrypoint([
      ...ctx.profile.entrypointHints.eval,
      ...ctx.profile.entrypoints,
      ...ctx.profile.fileSample,
    ]);

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

    const train: string[] = trainEntrypoint
      ? [
          ...prologue,
          ensureDirs,
          '. "$VENV/bin/activate"',
          `GPU_COUNT=${gpuCount}`,
          `TRAIN_SCRIPT="${trainEntrypoint.path}"`,
          'if [ ! -f "$TRAIN_SCRIPT" ]; then echo "Missing Transformers train script: $TRAIN_SCRIPT" >&2; exit 1; fi',
          'HELP_OUT="$(python3 "$TRAIN_SCRIPT" --help 2>/dev/null || true)"',
          'EXTRA_ARGS=""',
          'if echo "$HELP_OUT" | grep -q -- "--output_dir"; then EXTRA_ARGS="$EXTRA_ARGS --output_dir $OPENCLAW_OUTPUT_DIR"; fi',
          'if echo "$HELP_OUT" | grep -q -- "--overwrite_output_dir"; then EXTRA_ARGS="$EXTRA_ARGS --overwrite_output_dir"; fi',
          'if echo "$HELP_OUT" | grep -q -- "--do_train"; then EXTRA_ARGS="$EXTRA_ARGS --do_train"; fi',
          'if echo "$HELP_OUT" | grep -q -- "--do_eval"; then EXTRA_ARGS="$EXTRA_ARGS --do_eval"; fi',
          'if echo "$HELP_OUT" | grep -q -- "--report_to"; then EXTRA_ARGS="$EXTRA_ARGS --report_to none"; fi',
          ...(trainEntrypoint.kind === "hf_glue"
            ? [
                'if echo "$HELP_OUT" | grep -q -- "--model_name_or_path"; then EXTRA_ARGS="$EXTRA_ARGS --model_name_or_path distilbert-base-uncased"; fi',
                'if echo "$HELP_OUT" | grep -q -- "--task_name"; then EXTRA_ARGS="$EXTRA_ARGS --task_name mrpc"; fi',
              ]
            : []),
          ...(trainEntrypoint.kind === "hf_qa"
            ? [
                'if echo "$HELP_OUT" | grep -q -- "--model_name_or_path"; then EXTRA_ARGS="$EXTRA_ARGS --model_name_or_path distilbert-base-uncased"; fi',
                'if echo "$HELP_OUT" | grep -q -- "--dataset_name"; then EXTRA_ARGS="$EXTRA_ARGS --dataset_name squad"; fi',
              ]
            : []),
          ...(trainEntrypoint.kind === "hf_clm"
            ? [
                'if echo "$HELP_OUT" | grep -q -- "--model_name_or_path"; then EXTRA_ARGS="$EXTRA_ARGS --model_name_or_path distilgpt2"; fi',
                'if echo "$HELP_OUT" | grep -q -- "--dataset_name"; then EXTRA_ARGS="$EXTRA_ARGS --dataset_name wikitext"; fi',
                'if echo "$HELP_OUT" | grep -q -- "--dataset_config_name"; then EXTRA_ARGS="$EXTRA_ARGS --dataset_config_name wikitext-2-raw-v1"; fi',
              ]
            : []),
          ...(trainEntrypoint.kind === "hf_mlm"
            ? [
                'if echo "$HELP_OUT" | grep -q -- "--model_name_or_path"; then EXTRA_ARGS="$EXTRA_ARGS --model_name_or_path distilroberta-base"; fi',
                'if echo "$HELP_OUT" | grep -q -- "--dataset_name"; then EXTRA_ARGS="$EXTRA_ARGS --dataset_name wikitext"; fi',
                'if echo "$HELP_OUT" | grep -q -- "--dataset_config_name"; then EXTRA_ARGS="$EXTRA_ARGS --dataset_config_name wikitext-2-raw-v1"; fi',
              ]
            : []),
          'RESUME_ARGS=""',
          'if echo "$HELP_OUT" | grep -q -- "--resume_from_checkpoint"; then',
          '  CKPT=""',
          '  for d in "$OPENCLAW_OUTPUT_DIR"/checkpoint-*; do',
          '    if [ -d "$d" ]; then CKPT="$d"; fi',
          "  done",
          '  if [ -n "$CKPT" ]; then RESUME_ARGS="--resume_from_checkpoint $CKPT"; fi',
          "fi",
          'python3 -c "import accelerate" >/dev/null 2>&1 && HAS_ACCELERATE=1 || HAS_ACCELERATE=0',
          'if [ "$HAS_ACCELERATE" = "1" ] && [ "$GPU_COUNT" -gt 1 ]; then',
          '  accelerate launch --num_processes "$GPU_COUNT" "$TRAIN_SCRIPT" $EXTRA_ARGS $RESUME_ARGS',
          "else",
          '  python3 "$TRAIN_SCRIPT" $EXTRA_ARGS $RESUME_ARGS',
          "fi",
          'echo "training finished (transformers adapter)"',
        ]
      : [
          renderDiscoverPlanDirCommand(),
          'echo "No Transformers training entrypoint found (expected train.py or HF run_*.py scripts)." >&2',
          "exit 1",
        ];

    const evalCommands: string[] = [
      ...prologue,
      ensureDirs,
      '. "$VENV/bin/activate"',
      ...(evalEntrypoint
        ? [
            `EVAL_SCRIPT="${evalEntrypoint}"`,
            'if [ -f "$EVAL_SCRIPT" ]; then',
            '  HELP_OUT="$(python3 "$EVAL_SCRIPT" --help 2>/dev/null || true)"',
            '  EXTRA_ARGS=""',
            '  if echo "$HELP_OUT" | grep -q -- "--output_dir"; then EXTRA_ARGS="$EXTRA_ARGS --output_dir $OPENCLAW_OUTPUT_DIR"; fi',
            '  if echo "$HELP_OUT" | grep -q -- "--model_name_or_path"; then EXTRA_ARGS="$EXTRA_ARGS --model_name_or_path $OPENCLAW_OUTPUT_DIR"; fi',
            '  if echo "$HELP_OUT" | grep -q -- "--do_eval"; then EXTRA_ARGS="$EXTRA_ARGS --do_eval"; fi',
            '  python3 "$EVAL_SCRIPT" $EXTRA_ARGS || true',
            "else",
            '  echo "Missing eval script: $EVAL_SCRIPT; extracting metrics only" >&2',
            "fi",
          ]
        : ['echo "No eval.py detected; extracting metrics only" >&2']),
      renderExtractTransformersEvalMetricsCommand(),
    ];

    return {
      outputDirRel: ctx.outputDirRel,
      env: {},
      setup,
      install,
      train,
      eval: evalCommands,
      notes: [],
      warnings: trainEntrypoint ? [] : ["No Transformers train entrypoint detected."],
    };
  },
};
