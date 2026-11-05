/**
 * Replace the volatile parts of an error message (ids, numbers, paths, timestamps) with
 * placeholders so "User 4812 not found" and "User 9930 not found" group together.
 *
 * Order matters: more specific patterns run before the generic number rule.
 */
const RULES: Array<[RegExp, string | ((m: string) => string)]> = [
  [/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, "<uuid>"],
  [/[\w.+-]+@[\w-]+(?:\.[\w-]+)+/g, "<email>"],
  [/\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?/g, "<ts>"],
  [/\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b/g, "<ip>"],
  [/https?:\/\/[^\s'")]+/g, "<url>"],
  // Unix-style paths with at least two segments.
  [/(?:\/[\w.@~-]+){2,}\/?/g, "<path>"],
  // Long hex-ish tokens (hashes, object ids). Must mix digits and a-f letters, otherwise a
  // plain word like "decaffeinated" or a plain number would be caught here.
  [/\b[0-9a-f]{8,}\b/gi, (m) => (/\d/.test(m) && /[a-f]/i.test(m) ? "<hex>" : m)],
  [/\b\d+(?:\.\d+)?\b/g, "<num>"],
];

export function normalizeMessage(input: string): string {
  let out = input.slice(0, 2048);
  for (const [re, repl] of RULES) {
    out = out.replace(re, repl as never);
  }
  return out.replace(/\s+/g, " ").trim();
}

/** Strip bundler content hashes and query strings, keep the last two path segments. */
export function normalizeFilename(filename: string): string {
  const noQuery = filename.split("?")[0] ?? filename;
  const noHash = noQuery.replace(/\.[0-9a-f]{8,}(?=\.)/gi, "");
  const parts = noHash.split(/[\\/]/).filter(Boolean);
  return parts.slice(-2).join("/");
}