import JSON5 from "json5";

type ExtractResult = { ok: true; jsonText: string } | { ok: false; error: string };

function findFirstCodeFenceBlock(raw: string, language?: string): string | null {
  const langPattern = language ? language.replaceAll(/[.*+?^${}()|[\]\\]/g, "\\$&") : "";
  const re = language
    ? new RegExp(`(^|\\n)\\\`\\\`\\\`(?:\\s*${langPattern})\\s*\\n([\\s\\S]*?)\\n\\\`\\\`\\\``, "i")
    : /(^|\n)```\s*\n([\s\S]*?)\n```/i;
  const match = raw.match(re);
  if (!match) {
    return null;
  }
  return match[2] ?? null;
}

function scanJsonSubstring(text: string): ExtractResult {
  const firstObject = text.indexOf("{");
  const firstArray = text.indexOf("[");
  const start =
    firstObject === -1
      ? firstArray
      : firstArray === -1
        ? firstObject
        : Math.min(firstObject, firstArray);
  if (start === -1) {
    return { ok: false, error: "No JSON object/array start found" };
  }

  const openToClose: Record<string, string> = { "{": "}", "[": "]" };
  const stack: string[] = [];
  let inString = false;
  let escaped = false;

  for (let i = start; i < text.length; i += 1) {
    const ch = text[i] ?? "";
    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch === "\\") {
        escaped = true;
        continue;
      }
      if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === "{" || ch === "[") {
      stack.push(openToClose[ch]);
      continue;
    }
    if (ch === "}" || ch === "]") {
      const expected = stack.pop();
      if (!expected) {
        continue;
      }
      if (ch !== expected) {
        return { ok: false, error: `Mismatched JSON bracket: expected "${expected}" got "${ch}"` };
      }
      if (stack.length === 0) {
        return { ok: true, jsonText: text.slice(start, i + 1) };
      }
    }
  }

  return { ok: false, error: "Unterminated JSON (no matching closing bracket found)" };
}

export function extractJsonFromText(raw: string): ExtractResult {
  const candidates: string[] = [];
  const fencedJson = findFirstCodeFenceBlock(raw, "json");
  if (fencedJson) {
    candidates.push(fencedJson);
  }
  const fencedAny = findFirstCodeFenceBlock(raw);
  if (fencedAny) {
    candidates.push(fencedAny);
  }
  candidates.push(raw);

  const errors: string[] = [];
  for (const candidate of candidates) {
    const scanned = scanJsonSubstring(candidate);
    if (scanned.ok) {
      return scanned;
    }
    errors.push(scanned.error);
  }

  return { ok: false, error: errors.join("; ") };
}

export function parseJsonBestEffort(
  raw: string,
): { ok: true; value: unknown } | { ok: false; error: string } {
  const extracted = extractJsonFromText(raw);
  if (!extracted.ok) {
    return extracted;
  }
  try {
    return { ok: true, value: JSON.parse(extracted.jsonText) };
  } catch {
    try {
      return { ok: true, value: JSON5.parse(extracted.jsonText) };
    } catch (err) {
      return { ok: false, error: String(err) };
    }
  }
}
