/**
 * Consumer — configuration type and CLI argument parsing.
 *
 * Zero-dependency arg parsing via Node's built-in `util.parseArgs` (same
 * discipline as the producer: no yargs/commander, no new runtime deps).
 *
 * The sink is a PURE RECEIVER by design: it does NOT register the subscription.
 * The operator pre-creates a TamaDB subscription whose `callbackUrl` points at
 * this sink (consistent with the producer being a pure sender). So there is no
 * TamaDB base URL / subscription flag here — only how to listen, how to sink,
 * and how to misbehave.
 */

import { parseArgs } from 'node:util';
import { type FaultConfig, type FaultAction, makeFaultSchedule, NO_FAULTS } from './faults.js';
import type { SinkMode } from './sinks.js';

export interface ConsumerConfig {
  readonly host: string;
  readonly port: number;
  /** Path the sink listens on for deliveries. Default '/'. */
  readonly path: string;
  readonly sinkMode: SinkMode;
  /** Output file (save-jsonl) or directory (file-per-delivery). */
  readonly outputPath?: string;
  /**
   * Persist `{ headers, body }` (the TamaDB-* delivery headers plus the body)
   * instead of the bare body. Off by default so a saved JSONL stays diffable
   * against the producer's input.
   */
  readonly saveContext: boolean;
  /** Print each delivery to stdout as it arrives. */
  readonly print: boolean;
  /**
   * Treat a delivery with no TamaDB-* headers as a contract warning. Off by
   * default because from TamaDB 0.8.0 headers are sent only when a subscription
   * opts in (`protocolMetadata: true`). Turn it on against a 0.7.0 server, or
   * when testing an opted-in subscription.
   */
  readonly requireHeaders: boolean;
  readonly faults: FaultConfig;
  /** Auto-stop and print the summary after N idle ms with no deliveries (0 = run until Ctrl-C). */
  readonly idleStopMs: number;
  /** Optional path to write a machine-readable JSON summary on shutdown. */
  readonly jsonOut?: string;
}

const HELP = `
TamaDB Consumer / Webhook-Sink Emulator — receive TamaDB webhook deliveries.

The OPERATOR pre-creates a TamaDB subscription pointing its callbackUrl at this
sink. This tool does not register anything; it only receives.

USAGE:
  tamadb-sink [options]

LISTEN:
  --host <host>            Bind host. Default: 127.0.0.1
  --port <n>               Bind port. Default: 4000
  --path <path>            Path to accept deliveries on. Default: /

SINK MODE:
  --sink <mode>            void | save-jsonl | file-per-delivery. Default: void.
  --out <path>             Output file (save-jsonl) or directory (file-per-delivery).
  --save-context           Save { headers, body } (the TamaDB-* delivery headers
                           plus the body) instead of the bare body. Default: off,
                           so a saved JSONL stays diffable against producer input.
                           Only acknowledged (2xx) attempts are saved either way.

WATCH:
  --print                  Print each delivery as it arrives: time, path, the
                           status the sink returned, the TamaDB-* headers (if
                           any) and the body. Works with any sink mode. Printing
                           every delivery slows the sink under heavy load.

CONTRACT:
  --require-headers        Warn when a delivery has no TamaDB-* headers. Use it
                           against TamaDB 0.7.0 (headers always sent) or for a
                           subscription with protocolMetadata: true. Default:
                           off, because from 0.8.0 headers are opt-in.

FAULT INJECTION (exercise TamaDB retry / fire-and-forget / backoff):
  --fault <action>         non-2xx | delay | drop. Default: non-2xx.
  --fault-every <n>        Fault every Nth delivery.
  --fault-rate <pct>       Each delivery has pct% chance of a fault.
  --fault-status <code>    Status returned for the non-2xx action. Default: 500.
  --fault-delay <ms>       Hold time for the delay action. Default: 1000.
  --seed <int>             Seed for random fault scheduling (reproducibility).

LIFECYCLE / OUTPUT:
  --idle-stop <ms>         Auto-stop after this many idle ms with no deliveries
                           (0 = run until Ctrl-C). Default: 0. Keep it above
                           TamaDB's retry backoff (1/2/4/8/16 s) when testing
                           faults, or the sink exits before the retries arrive.
  --json <path>            Write a machine-readable JSON summary on shutdown.
  --help                   Show this help.

NOTES:
  * Works with the TamaDB 0.7.0 and 0.8.0 webhook contracts. The body is the
    bare document by default (payloadFormat "raw"); "envelope" is opt-in per
    subscription. Both shapes are detected per delivery, and the envelope's
    time key may be "timestamp" (0.7.0) or "enqueuedAt" (0.8.0).
  * TamaDB-Subscription-Id / -Document-Type / -D-Id / -Enqueued-At (and -Path
    for node-scoped subscriptions) are sent on every delivery in 0.7.0, and from
    0.8.0 only when the subscription sets protocolMetadata: true. When present,
    context and latency come from them; envelope bodies are cross-checked.
  * A raw delivery without headers carries no context: it counts as
    "unattributed", with no latency sample and no retry/duplicate classification.
  * Always responds 200 unless a fault is injected for that delivery. A non-2xx
    or dropped connection is a failed attempt: under "retry" TamaDB re-sends the
    same body and headers (best-effort, in-memory queue, backoff 1/2/4/8/16 s).
  * TamaDB in Docker, sink on the host: use callbackUrl
    http://host.docker.internal:<port>/...  and bind the sink with --host 0.0.0.0.
`;

export function printConsumerHelp(): void {
  process.stdout.write(HELP);
}

export function parseConsumerConfig(argv: readonly string[]): ConsumerConfig | 'help' {
  const { values } = parseArgs({
    args: [...argv],
    allowPositionals: false,
    options: {
      host: { type: 'string' },
      port: { type: 'string' },
      path: { type: 'string' },
      sink: { type: 'string' },
      out: { type: 'string' },
      'save-context': { type: 'boolean' },
      print: { type: 'boolean' },
      'require-headers': { type: 'boolean' },
      fault: { type: 'string' },
      'fault-every': { type: 'string' },
      'fault-rate': { type: 'string' },
      'fault-status': { type: 'string' },
      'fault-delay': { type: 'string' },
      seed: { type: 'string' },
      'idle-stop': { type: 'string' },
      json: { type: 'string' },
      help: { type: 'boolean' },
    },
  });

  if (values.help) return 'help';

  const sinkMode = parseSinkMode(values.sink);
  const faults = parseFaults(values);

  const cfg: ConsumerConfig = {
    host: values.host ?? '127.0.0.1',
    port: parsePort(values.port, 4000),
    path: normalisePath(values.path ?? '/'),
    sinkMode,
    ...(values.out !== undefined ? { outputPath: values.out } : {}),
    saveContext: values['save-context'] ?? false,
    print: values.print ?? false,
    requireHeaders: values['require-headers'] ?? false,
    faults,
    idleStopMs: parseNonNegInt(values['idle-stop'], 0, '--idle-stop'),
    ...(values.json !== undefined ? { jsonOut: values.json } : {}),
  };

  if ((sinkMode === 'save-jsonl' || sinkMode === 'file-per-delivery') && cfg.outputPath === undefined) {
    throw new Error(`--sink ${sinkMode} requires --out <path>`);
  }

  return cfg;
}

function parseSinkMode(raw: string | undefined): SinkMode {
  if (raw === undefined) return 'void';
  if (raw === 'void' || raw === 'save-jsonl' || raw === 'file-per-delivery') return raw;
  throw new Error(`--sink must be one of: void, save-jsonl, file-per-delivery (got "${raw}")`);
}

function parseFaults(v: Record<string, string | boolean | undefined>): FaultConfig {
  const action = v.fault !== undefined ? parseFaultAction(String(v.fault)) : 'non-2xx';
  const every = v['fault-every'] !== undefined ? parsePositiveInt(v['fault-every'], 1, '--fault-every') : undefined;
  const pct = v['fault-rate'] !== undefined ? parsePositiveNumber(v['fault-rate'], '--fault-rate') : undefined;
  const seed = v.seed !== undefined ? parseNonNegInt(v.seed, 0, '--seed') : 0x9e3779b9;
  const schedule = makeFaultSchedule(every, pct, seed);

  if (schedule.kind === 'none' && (v.fault !== undefined || v['fault-status'] !== undefined || v['fault-delay'] !== undefined)) {
    // Fault parameters given without a schedule: nothing would ever fault.
    // Keep it permissive but default to a sane schedule only if an action was set.
  }

  return {
    schedule,
    action,
    status: v['fault-status'] !== undefined ? parsePositiveInt(v['fault-status'], 500, '--fault-status') : 500,
    delayMs: v['fault-delay'] !== undefined ? parseNonNegInt(v['fault-delay'], 1000, '--fault-delay') : 1000,
  };
}

function parseFaultAction(raw: string): FaultAction {
  if (raw === 'non-2xx' || raw === 'delay' || raw === 'drop') return raw;
  throw new Error(`--fault must be one of: non-2xx, delay, drop (got "${raw}")`);
}

// --- small parse helpers (mirrors producer/config.ts) ---

function normalisePath(p: string): string {
  if (!p.startsWith('/')) return '/' + p;
  return p.length > 1 ? p.replace(/\/+$/, '') : p;
}

function parsePort(v: string | undefined, dflt: number): number {
  if (v === undefined) return dflt;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 1 || n > 65535) throw new Error('--port must be an integer in 1..65535');
  return n;
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
