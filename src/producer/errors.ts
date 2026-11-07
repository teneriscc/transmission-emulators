/**
 * Producer — deliberate error injection (brief §4.6, R13–R15).
 *
 * Two orthogonal concerns:
 *   1. SCHEDULING — decide WHICH messages get corrupted.
 *   2. KIND       — decide HOW a chosen message is corrupted.
 *
 * Parked (need per-D-Id awareness, deferred with optimistic-concurrency Q3):
 *   - `stale-version` error kind (VERSION_CONFLICT) — requires tracking the
 *     last returned versionNo per D-Id, which the opaque-line producer does not do.
 */

import { isJsonObject } from '../core/jsonl.js';

// ---------------------------------------------------------------------------
// Seeded RNG (mulberry32) — reproducible error injection (§5, R13 random+seed).
// ---------------------------------------------------------------------------

export function makeRng(seed: number): () => number {
  let a = seed >>> 0;
  return function next(): number {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

export type ErrorSchedule =
  | { kind: 'none' }
  | { kind: 'every'; n: number } // corrupt every Nth message
  | { kind: 'rate'; pct: number; rng: () => number } // X% chance per message
  | { kind: 'after'; k: number } // inject once after K consecutive good messages
  | { kind: 'burst'; b: number; at: number }; // a contiguous block of B bad messages starting at index `at`

/**
 * Stateful scheduler. `messageIndex` is 0-based across the whole run (per
 * producer). Returns true if THIS message should be corrupted.
 *
 * Per-producer instance (no shared state) so concurrent producers each hold
 * their own counter — consistent with the metrics concurrency discipline.
 */
export class ErrorScheduler {
  private consecutiveGood = 0;
  private afterFired = false;

  constructor(private readonly schedule: ErrorSchedule) {}

  shouldCorrupt(messageIndex: number): boolean {
    const s = this.schedule;
    switch (s.kind) {
      case 'none':
        return false;
      case 'every':
        return s.n > 0 && (messageIndex + 1) % s.n === 0;
      case 'rate':
        return s.rng() * 100 < s.pct;
      case 'after': {
        if (this.afterFired) return false;
        if (this.consecutiveGood >= s.k) {
          this.afterFired = true;
          this.consecutiveGood = 0;
          return true;
        }
        this.consecutiveGood += 1;
        return false;
      }
      case 'burst':
        return messageIndex >= s.at && messageIndex < s.at + s.b;
    }
  }
}

// ---------------------------------------------------------------------------
// Error kinds
// ---------------------------------------------------------------------------

export type ErrorKind = 'transport-malformed' | 'schema-invalid' | 'control-header-invalid';

export interface CorruptedMessage {
  /** Request body to send (may be non-JSON on purpose). */
  readonly body: string;
  /** Extra/overriding headers to merge (used by control-header-invalid). */
  readonly headerOverrides?: Record<string, string>;
  /** Human-readable label of what was injected, for the log/summary (R15). */
  readonly injectionLabel: string;
}

/**
 * Apply an error kind to an otherwise-sendable line.
 *
 * - transport-malformed : truncate the JSON so it fails to parse -> PARSE_ERROR.
 * - schema-invalid      : drop a required-looking field / set a wrong type so an
 *                         otherwise-valid document fails validation -> SCHEMA_VALIDATION_FAILED.
 *                         (Best-effort and schema-agnostic: the emulator does not
 *                         know the schema, so it mutates a top-level scalar field.)
 * - control-header-invalid : send an out-of-domain TamaDB-* header -> INVALID_CONTROL_HEADER.
 */
export function corrupt(line: string, kind: ErrorKind, rng: () => number): CorruptedMessage {
  switch (kind) {
    case 'transport-malformed':
      return {
        body: truncateJson(line),
        injectionLabel: 'transport-malformed (truncated JSON)',
      };

    case 'schema-invalid':
      return {
        body: mutateField(line, rng),
        injectionLabel: 'schema-invalid (mutated field)',
      };

    case 'control-header-invalid': {
      // Rotate through a few out-of-domain control-header values.
      const choices: Array<[string, string, string]> = [
        ['TamaDB-Version', '0', 'TamaDB-Version:0 (must be >= 1)'],
        ['TamaDB-Version', 'abc', 'TamaDB-Version:abc (non-integer)'],
        ['TamaDB-TTL', '-5', 'TamaDB-TTL:-5 (non-integer/negative)'],
        ['TamaDB-Expire-At', 'not-a-date', 'TamaDB-Expire-At:not-a-date (bad ISO-8601)'],
        ['TamaDB-Schema', '', 'TamaDB-Schema: (empty)'],
      ];
      const pick = choices[Math.floor(rng() * choices.length)] ?? choices[0]!;
      return {
        body: line,
        headerOverrides: { [pick[0]]: pick[1] },
        injectionLabel: `control-header-invalid (${pick[2]})`,
      };
    }
  }
}

/** Truncate a line so JSON parsing fails (guaranteed non-object). */
function truncateJson(line: string): string {
  if (line.length <= 1) return '{';
  // Chop the last ~40% (at least one char) and drop any closing brace.
  const cut = Math.max(1, Math.floor(line.length * 0.6));
  return line.slice(0, cut);
}

/**
 * Mutate one top-level field to make a valid document schema-invalid.
 * Schema-agnostic best effort: flips a scalar's type, or removes the first key.
 * If the line is not a JSON object we fall back to truncation so SOMETHING is
 * still rejected (and the label stays honest via the caller).
 */
function mutateField(line: string, rng: () => number): string {
  if (!isJsonObject(line)) return truncateJson(line);
  const obj = JSON.parse(line) as Record<string, unknown>;
  const keys = Object.keys(obj);
  if (keys.length === 0) {
    // Empty object: inject a bogus field of an unexpected type.
    return JSON.stringify({ __tamadb_emulator_bogus__: { unexpected: true } });
  }
  const key = keys[Math.floor(rng() * keys.length)] ?? keys[0]!;
  // Coin flip: either drop the field, or replace it with a type-mismatched value.
  if (rng() < 0.5) {
    delete obj[key];
  } else {
    const v = obj[key];
    obj[key] = typeof v === 'string' ? 12345 : 'unexpected-string';
  }
  return JSON.stringify(obj);
}
