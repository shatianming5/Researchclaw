const RESERVED_SECOND_SEGMENTS = new Set(["viewer", "tree", "resolve", "blob"]);

function isValidHfIdPart(value: string): boolean {
  return /^[\w.-]+$/.test(value);
}

export function extractHfDatasetIdFromUrl(rawUrl: string): string | null {
  const trimmed = rawUrl.trim();
  if (!trimmed) {
    return null;
  }
  let url: URL;
  try {
    url = new URL(trimmed);
  } catch {
    return null;
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    return null;
  }
  const host = url.hostname.toLowerCase();
  if (host !== "huggingface.co" && !host.endsWith(".huggingface.co")) {
    return null;
  }

  const segments = url.pathname.split("/").filter(Boolean);
  const datasetsIndex = segments.indexOf("datasets");
  if (datasetsIndex === -1) {
    return null;
  }
  const after = segments.slice(datasetsIndex + 1);
  if (after.length === 0) {
    return null;
  }

  const first = after[0] ?? "";
  if (!first || !isValidHfIdPart(first)) {
    return null;
  }
  if (after.length === 1) {
    return first;
  }

  const second = after[1] ?? "";
  if (!second || !isValidHfIdPart(second)) {
    return first;
  }

  if (after.length === 2 && RESERVED_SECOND_SEGMENTS.has(second.toLowerCase())) {
    return first;
  }

  return `${first}/${second}`;
}
