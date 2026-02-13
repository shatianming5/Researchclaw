import { describe, expect, it } from "vitest";
import { extractJsonFromText, parseJsonBestEffort } from "./json.js";

describe("proposal/json", () => {
  it("extracts JSON from a fenced code block", () => {
    const input = [
      "Some text",
      "```json",
      "{",
      '  "a": 1,',
      '  "b": [2, 3]',
      "}",
      "```",
      "more text",
    ].join("\n");
    const extracted = extractJsonFromText(input);
    expect(extracted.ok).toBe(true);
    if (extracted.ok) {
      expect(extracted.jsonText.trim().startsWith("{")).toBe(true);
      expect(JSON.parse(extracted.jsonText)).toEqual({ a: 1, b: [2, 3] });
    }
  });

  it("parses JSON5 as fallback", () => {
    const input = "```json\n{\n  a: 1,\n}\n```";
    const parsed = parseJsonBestEffort(input);
    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.value).toEqual({ a: 1 });
    }
  });
});
