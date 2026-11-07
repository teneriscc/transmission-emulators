/**
 * Shared core — run summary formatting (human-readable + machine-readable JSON).
 *
 * Pure functions only: given a Metrics instance and run metadata, produce a
 * string (human) or a plain object (JSON). No I/O here — the caller decides
 * where to print/write, keeping this reentrant and consumer-agnostic.
 *
 * This module serves BOTH emulators. The producer summary (buildSummaryJson /
 * formatSummaryHuman + RunMeta) is unchanged from the original producer tool.
 * The consumer sink has its own metrics/summary shape — see
 * src/consumer/metrics.ts — because its observations (end-to-end latency,
 * gaps/duplicates, per-type tallies) differ from send-side metrics.
 */

import { latencyStats, type LatencyStats, type Metrics } from './metrics.js';

export interface RunMeta {
  readonly target: string;
  readonly documentType: string;
  readonly channelId?: string;
  readonly startedAt: string;
  readonly finishedAt: string;
  readonly durationMs: number;
  readonly concurrency: number;
}

export interface RunSummaryJson {
  readonly meta: RunMeta;
  readonly totals: {
    readonly sent: number;
    readonly transportFailures: number;
    readonly injectedErrors: number;
    readonly quarantineRefs: number;
    readonly achievedRatePerSec: number;
  };
  readonly statusCodes: Record<string, number>;
  readonly tamadbCodes: Record<string, number>;
  readonly latencyMs: LatencyStats;
}

export function buildSummaryJson(metrics: Metrics, meta: RunMeta): RunSummaryJson {
  const stats = latencyStats(metrics.latencies);
  const achievedRatePerSec = meta.durationMs > 0 ? (metrics.totalSent / meta.durationMs) * 1000 : 0;
  return {
    meta,
    totals: {
      sent: metrics.totalSent,
      transportFailures: metrics.transportFailures,
      injectedErrors: metrics.injectedErrors,
      quarantineRefs: metrics.quarantineRefs,
      achievedRatePerSec: round(achievedRatePerSec, 2),
    },
    statusCodes: mapToObject(metrics.statusCodes),
    tamadbCodes: mapToObject(metrics.tamadbCodes),
    latencyMs: stats,
  };
}

export function formatSummaryHuman(metrics: Metrics, meta: RunMeta): string {
  const j = buildSummaryJson(metrics, meta);
  const lines: string[] = [];
  const target = meta.channelId
    ? `${meta.target}/api/v1/ingest/${meta.documentType}/${meta.channelId}`
    : `${meta.target}/api/v1/ingest/${meta.documentType}`;

  lines.push('');
  lines.push('=== Producer Emulator — Run Summary ===');
  lines.push(`Target        : ${target}`);
  lines.push(`Duration      : ${(meta.durationMs / 1000).toFixed(2)} s  (concurrency ${meta.concurrency})`);
  lines.push(`Sent          : ${j.totals.sent}  (~${j.totals.achievedRatePerSec} msg/s)`);
  lines.push(`Injected errs : ${j.totals.injectedErrors}`);
  lines.push(`Quarantined   : ${j.totals.quarantineRefs}`);
  if (j.totals.transportFailures > 0) {
    lines.push(`Transport fail: ${j.totals.transportFailures}  (could not reach server)`);
  }

  lines.push('');
  lines.push('HTTP status codes:');
  for (const [code, n] of sortEntries(j.statusCodes)) {
    const label = code === '0' ? '  (no response)' : '';
    lines.push(`  ${code}${label} : ${n}`);
  }

  if (Object.keys(j.tamadbCodes).length > 0) {
    lines.push('');
    lines.push('TamaDB error codes:');
    for (const [code, n] of sortEntries(j.tamadbCodes)) {
      lines.push(`  ${code} : ${n}`);
    }
  }

  const l = j.latencyMs;
  lines.push('');
  lines.push('Latency (ms):');
  if (l.count === 0) {
    lines.push('  (no samples)');
  } else {
    lines.push(`  min ${fmt(l.min)}  mean ${fmt(l.mean)}  p50 ${fmt(l.p50)}  p95 ${fmt(l.p95)}  p99 ${fmt(l.p99)}  max ${fmt(l.max)}`);
  }
  lines.push('=======================================');
  return lines.join('\n');
}

// ─── Shared formatting helpers (used by producer + consumer summaries) ───────

export function mapToObject<K extends string | number>(m: Map<K, number>): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [k, v] of m) out[String(k)] = v;
  return out;
}

export function sortEntries(o: Record<string, number>): Array<[string, number]> {
  return Object.entries(o).sort((a, b) => a[0].localeCompare(b[0], undefined, { numeric: true }));
}

export function round(n: number, dp: number): number {
  const f = 10 ** dp;
  return Math.round(n * f) / f;
}

export function fmt(n: number): string {
  return n.toFixed(1);
}
