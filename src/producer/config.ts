/**
 * Producer — configuration type and CLI argument parsing.
 *
 * Zero-dependency arg parsing (no yargs/commander) to honour the "zero new
 * runtime dependencies" constraint (§5). Uses Node's built-in `util.parseArgs`.
 */

import { parseArgs } from 'node:util';
import type { PacingMode } from './pacing.js';
import type { ErrorKind, ErrorSchedule } from './errors.js';
import { makeRng } from './errors.js';

export interface ControlHeaderConfig {
  readonly version?: string; // TamaDB-Version
  readonly ttl?: string; // TamaDB-TTL
  readonly expireAt?: string; // TamaDB-Expire-At
  readonly schema?: string; // TamaDB-Schema
}

export interface ProducerConfig {
  readonly file: string;
  readonly baseUrl: string;
  readonly documentType: string;
  readonly channelId?: string;
  readonly concurrency: number;
  readonly pacing: PacingMode;
  readonly errorSchedule: ErrorSchedule;
  readonly errorKind: ErrorKind;
  readonly controlHeaders: ControlHeaderConfig;
  /** Skip lines that are not valid JSON objects instead of sending them (R4). */
  readonly skipInvalidInput: boolean;
  /** Optional path to write a machine-readable JSON summary (R18). */
  readonly jsonOut?: string;
  /** Maximum lines to send (0 = all). Handy for smoke tests. */
  readonly limit: number;
  /** Per-message request timeout (ms). */
  readonly timeoutMs: number;
}

const HELP = `
TamaDB Producer Emulator — push a JSONL file to a running TamaDB over HTTP.

USAGE:
  tsx scripts/producer-emulator/src/producer/index.ts --file <path> --type <documentType> [options]

REQUIRED:
  --file <path>            JSONL file, one document per line.
  --type <documentType>    Target Document_Type (URL path segment).

TARGET:
  --url <baseUrl>          TamaDB base URL. Default: http://localhost:3000
  --channel <channelId>    Optional channel id (adds /{channelId} to the path).

PACING (choose at most one):
  --delay <ms>             Fixed inter-message delay.
  --rate <n>               Target messages/second.
  --ramp <s>:<e>:<sec>     Ramp rate from s to e msg/s over <sec> seconds.
  --burst <n> --gap <ms>   Send n back-to-back, then pause <gap> ms; repeat.

CONCURRENCY:
  --concurrency <n>        Number of parallel virtual producers. Default: 1.

CONTROL HEADERS (optional, attached to every message):
  --h-version <int>        TamaDB-Version (positive integer >= 1)
  --h-ttl <int|infinite>   TamaDB-TTL (non-negative seconds, or "infinite")
  --h-expire-at <iso>      TamaDB-Expire-At: ISO-8601 / RFC 3339 date-time WITH an
                           explicit timezone (trailing Z, or +/-HH:MM offset).
                           e.g. 2026-12-31T23:59:59Z  (a value without a timezone
                           is ambiguous and interpreted in the server's local time)
  --h-schema <ref>         TamaDB-Schema (schema reference string)

ERROR INJECTION:
  --error-kind <kind>      transport-malformed | schema-invalid | control-header-invalid
                           Default: transport-malformed.
  --error-every <n>        Corrupt every Nth message.
  --error-rate <pct>       Each message has pct% chance of corruption.
  --error-after <k>        Inject once after k consecutive good messages.
  --error-burst <b>[:<at>] Corrupt a contiguous block of b messages (starting at index at, default 0).
  --seed <int>             Seed for random error injection (reproducibility).

INPUT:
  --skip-invalid-input     Skip non-JSON-object lines instead of sending them.
  --limit <n>              Send at most n messages (0 = all). Default: 0.

OUTPUT:
  --json <path>            Also write a machine-readable JSON summary.
  --timeout <ms>           Per-request timeout. Default: 30000.
  --help                   Show this help.

NOTES:
  * Sends each line as the BARE JSON body (no wrapper) with Content-Type: application/json.
  * Assumes TamaDB is already configured (schemas/types/channels). Pure producer.
  * The bare-body wire format lives on branch feature/ingestion-bare-wire-format;
    run the server from that branch until it merges to main.
`;

export function printHelp(): void {
  process.stdout.write(HELP);
}

/** Parse process argv into a ProducerConfig, or throw an Error with a message. */
export function parseConfig(argv: readonly string[]): ProducerConfig | 'help' {
  const { values } = parseArgs({
    args: [...argv],
    allowPositionals: false,
    options: {
      file: { type: 'string' },
      type: { type: 'string' },
      url: { type: 'string' },
      channel: { type: 'string' },
      delay: { type: 'string' },
      rate: { type: 'string' },
      ramp: { type: 'string' },
      burst: { type: 'string' },
      gap: { type: 'string' },
      concurrency: { type: 'string' },
      'h-version': { type: 'string' },
      'h-ttl': { type: 'string' },
      'h-expire-at': { type: 'string' },
      'h-schema': { type: 'string' },
      'error-kind': { type: 'string' },
      'error-every': { type: 'string' },
      'error-rate': { type: 'string' },
      'error-after': { type: 'string' },
      'error-burst': { type: 'string' },
      seed: { type: 'string' },
      'skip-invalid-input': { type: 'boolean' },
      limit: { type: 'string' },
      json: { type: 'string' },
      timeout: { type: 'string' },
      help: { type: 'boolean' },
    },
  });

  if (values.help) return 'help';

  const file = requireStr(values.file, '--file');
  const documentType = requireStr(values.type, '--type');
  const baseUrl = (values.url ?? 'http://localhost:3000').replace(/\/+$/, '');

  const pacing = parsePacing(values);
  const errorSchedule = parseErrorSchedule(values);
  const errorKind = parseErrorKind(values['error-kind']);

  const controlHeaders: ControlHeaderConfig = {
    ...(values['h-version'] !== undefined ? { version: values['h-version'] } : {}),
    ...(values['h-ttl'] !== undefined ? { ttl: values['h-ttl'] } : {}),
    ...(values['h-expire-at'] !== undefined ? { expireAt: values['h-expire-at'] } : {}),
    ...(values['h-schema'] !== undefined ? { schema: values['h-schema'] } : {}),
  };

  return {
    file,
    baseUrl,
    documentType,
    ...(values.channel !== undefined ? { channelId: values.channel } : {}),
    concurrency: parsePositiveInt(values.concurrency, 1, '--concurrency'),
    pacing,
    errorSchedule,
    errorKind,
    controlHeaders,
    skipInvalidInput: values['skip-invalid-input'] ?? false,
    ...(values.json !== undefined ? { jsonOut: values.json } : {}),
    limit: parseNonNegInt(values.limit, 0, '--limit'),
    timeoutMs: parsePositiveInt(values.timeout, 30000, '--timeout'),
  };
}

type Values = Record<string, string | boolean | undefined>;

function parsePacing(v: Values): PacingMode {
  const chosen = ['delay', 'rate', 'ramp', 'burst'].filter((k) => v[k] !== undefined);
  if (chosen.length > 1) {
    throw new Error(`Pacing modes are mutually exclusive; got: ${chosen.map((c) => '--' + c).join(', ')}`);
  }
  if (v.delay !== undefined) return { kind: 'delay', ms: parseNonNegInt(v.delay, 0, '--delay') };
  if (v.rate !== undefined) return { kind: 'rate', perSec: parsePositiveNumber(v.rate, '--rate') };
  if (v.ramp !== undefined) {
    const m = String(v.ramp).split(':');
    if (m.length !== 3) throw new Error('--ramp expects <start>:<end>:<seconds>');
    return {
      kind: 'ramp',
      startPerSec: parsePositiveNumber(m[0], '--ramp start'),
      endPerSec: parsePositiveNumber(m[1], '--ramp end'),
      overMs: parsePositiveNumber(m[2], '--ramp seconds') * 1000,
    };
  }
  if (v.burst !== undefined) {
    return {
      kind: 'burst',
      n: parsePositiveInt(v.burst, 1, '--burst'),
      gapMs: parseNonNegInt(v.gap, 0, '--gap'),
    };
  }
  return { kind: 'none' };
}

function parseErrorSchedule(v: Values): ErrorSchedule {
  const chosen = ['error-every', 'error-rate', 'error-after', 'error-burst'].filter((k) => v[k] !== undefined);
  if (chosen.length > 1) {
    throw new Error(`Error schedules are mutually exclusive; got: ${chosen.map((c) => '--' + c).join(', ')}`);
  }
  const seed = v.seed !== undefined ? parseNonNegInt(v.seed, 0, '--seed') : 0x9e3779b9;

  if (v['error-every'] !== undefined) return { kind: 'every', n: parsePositiveInt(v['error-every'], 1, '--error-every') };
  if (v['error-rate'] !== undefined) {
    return { kind: 'rate', pct: parsePositiveNumber(v['error-rate'], '--error-rate'), rng: makeRng(seed) };
  }
  if (v['error-after'] !== undefined) return { kind: 'after', k: parseNonNegInt(v['error-after'], 0, '--error-after') };
  if (v['error-burst'] !== undefined) {
    const parts = String(v['error-burst']).split(':');
    return {
      kind: 'burst',
      b: parsePositiveInt(parts[0], 1, '--error-burst'),
      at: parts[1] !== undefined ? parseNonNegInt(parts[1], 0, '--error-burst at') : 0,
    };
  }
  return { kind: 'none' };
}

function parseErrorKind(raw: string | boolean | undefined): ErrorKind {
  if (raw === undefined) return 'transport-malformed';
  const s = String(raw);
  if (s === 'transport-malformed' || s === 'schema-invalid' || s === 'control-header-invalid') return s;
  throw new Error(`--error-kind must be one of: transport-malformed, schema-invalid, control-header-invalid (got "${s}")`);
}

// --- small numeric parse helpers ---

function requireStr(v: string | boolean | undefined, flag: string): string {
  if (v === undefined || v === '' || typeof v !== 'string') throw new Error(`${flag} is required`);
  return v;
}

function parsePositiveInt(v: string | boolean | undefined, dflt: number, flag: string): number {
  if (v === undefined) return dflt;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 1) throw new Error(`${flag} must be a positive integer`);
  return n;
}

function parseNonNegInt(v: string | boolean | undefined, dflt: number, flag: string): number {
  if (v === undefined) return dflt;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 0) throw new Error(`${flag} must be a non-negative integer`);
  return n;
}

function parsePositiveNumber(v: string | boolean | undefined, flag: string): number {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) throw new Error(`${flag} must be a positive number`);
  return n;
}
