/**
 * Shared core — JSONL streaming reader.
 *
 * Reentrant and side-effect-free: each call returns a fresh async iterator over
 * its own file handle / stream. No module-level mutable state, so it is safe to
 * use concurrently (e.g. multiple virtual producers) without cross-talk.
 *
 * Design notes:
 * - The emulator treats each line as OPAQUE JSON to send verbatim.
 *   This reader therefore yields the raw line text, not a parsed object — the
 *   producer must be able to send even syntactically-invalid lines to
 *   exercise TamaDB's PARSE_ERROR path.
 * - Blank lines and trailing whitespace-only lines are skipped (they are not
 *   documents). A `#`-prefixed line is NOT treated as a comment: JSONL has no
 *   comment convention and a document could legitimately start with other
 *   characters; we only skip truly empty lines.
 */

import { createReadStream } from 'node:fs';
import { createInterface } from 'node:readline';

export interface JsonlLine {
  /** 1-based line number in the source file (for logging / reconciliation). */
  readonly lineNo: number;
  /** Raw line text, verbatim, with the line terminator stripped. */
  readonly text: string;
}

/**
 * Stream a JSONL file line by line. Yields non-empty lines only, each tagged
 * with its 1-based line number.
 */
export async function* readJsonlLines(filePath: string): AsyncGenerator<JsonlLine> {
  const stream = createReadStream(filePath, { encoding: 'utf8' });
  // crlfDelay handles \r\n line endings transparently.
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  let lineNo = 0;
  try {
    for await (const raw of rl) {
      lineNo += 1;
      if (raw.trim() === '') continue; // skip blank lines
      yield { lineNo, text: raw };
    }
  } finally {
    rl.close();
    stream.close();
  }
}

/**
 * Count the number of non-empty lines in a JSONL file without holding them all
 * in memory. Useful for progress reporting and for partitioning work across
 * virtual producers.
 */
export async function countJsonlLines(filePath: string): Promise<number> {
  let count = 0;
  for await (const _ of readJsonlLines(filePath)) count += 1;
  return count;
}

/** Return true if `text` parses as a JSON object (not array / primitive). */
export function isJsonObject(text: string): boolean {
  try {
    const v: unknown = JSON.parse(text);
    return v !== null && typeof v === 'object' && !Array.isArray(v);
  } catch {
    return false;
  }
}
