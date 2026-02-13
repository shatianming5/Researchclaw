import { describe, expect, it } from "vitest";
import { z } from "zod";
import { completeJsonWithSchemaViaOpencode, runOpencodeJson } from "./run.js";

describe("proposal/opencode/run", () => {
  it("decodes streamed text events from opencode JSONL", async () => {
    const stdout = [
      JSON.stringify({ type: "step_start", part: { id: "1" } }),
      JSON.stringify({ type: "text", part: { text: "Hello" } }),
      JSON.stringify({ type: "text", part: { text: " world" } }),
      JSON.stringify({ type: "step_finish", part: { id: "1" } }),
    ].join("\n");

    const res = await runOpencodeJson({
      message: "ignored",
      model: "opencode/kimi-k2.5-free",
      deps: {
        runCommand: async () => ({
          stdout,
          stderr: "",
          code: 0,
          signal: null,
          killed: false,
        }),
      },
    });

    expect(res.ok).toBe(true);
    expect(res.text).toBe("Hello world");
    expect(res.warnings).toEqual([]);
  });

  it("repairs invalid JSON output across attempts", async () => {
    let calls = 0;
    const schema = z.object({ ok: z.boolean() });

    const res = await completeJsonWithSchemaViaOpencode({
      schema,
      prompt: 'Return ONLY valid JSON with {"ok":true}.',
      model: "opencode/kimi-k2.5-free",
      attempts: 2,
      deps: {
        runCommand: async () => {
          calls += 1;
          const text = calls === 1 ? "not json" : '{"ok":true}';
          const stdout = JSON.stringify({ type: "text", part: { text } });
          return {
            stdout,
            stderr: "",
            code: 0,
            signal: null,
            killed: false,
          };
        },
      },
    });

    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.value.ok).toBe(true);
      expect(res.raw).toContain('"ok"');
    }
  });
});
