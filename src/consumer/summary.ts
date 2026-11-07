/**
 * Consumer — run summary formatting (human + machine-readable JSON).
 *
 * Pure functions: given a ConsumerMetrics + run metadata, produce a string or a
 * plain object. No I/O (the caller decides where to print/write).
 */

import type { LatencyStats } from '../core/metrics.js';
import { fmt, mapToObject, round, sortEntries } from '../core/summary.js';
import type { ConsumerMetrics } from './metrics.js';
import type { SinkMode } from './sinks.js';

export interface ConsumerRunMeta {
  readonly listenUrl: string;
  readonly sinkMode: SinkMode;
  readonly outputPath?: string;
  readonly startedAt: string;
  readonly finishedAt: string;
  readonly durationMs: number;
}

export interface ConsumerSummaryJson {
  readonly meta: ConsumerRunMeta;
  readonly totals: {
    readonly received: number;
    readonly envelope: number;
    readonly raw: number;
    readonly faultsInjected: number;
    readonly duplicates: number;
    readonly achievedRatePerSec: number;
  };
  readonly respondedStatus: Record<string, number>;
  readonly byDocumentType: Record<string, number>;
  readonly byDId: Record<string, number>;
  readonly e2eLatencyMs: LatencyStats;
}

export function buildConsumerSummaryJson(metrics: ConsumerMetrics, meta: ConsumerRunMeta): ConsumerSummaryJson {
  const achievedRatePerSec = meta.durationMs > 0 ? (metrics.totalReceived / meta.durationMs) * 1000 : 0;
  return {
    meta,
    totals: {
      received: metrics.totalReceived,
      envelope: metrics.envelopeCount,
      raw: metrics.rawCount,
      faultsInjected: metrics.faultsInjected,
      duplicates: metrics.duplicates,
      achievedRatePerSec: round(achievedRatePerSec, 2),
    },
    respondedStatus: mapToObject(metrics.respondedStatus),
    byDocumentType: mapToObject(metrics.byDocumentType),
    byDId: mapToObject(metrics.byDId),
    e2eLatencyMs: metrics.e2eStats(),
  };
}

export function formatConsumerSummaryHuman(metrics: ConsumerMetrics, meta: ConsumerRunMeta): string {
  const j = buildConsumerSummaryJson(metrics, meta);
  const lines: string[] = [];

  lines.push('');
  lines.push('=== Consumer / Webhook-Sink Emulator — Run Summary ===');
  lines.push(`Listening     : ${meta.listenUrl}`);
  lines.push(`Sink mode     : ${meta.sinkMode}${meta.outputPath ? `  (${meta.outputPath})` : ''}`);
  lines.push(`Duration      : ${(meta.durationMs / 1000).toFixed(2)} s`);
  lines.push(`Received      : ${j.totals.received}  (~${j.totals.achievedRatePerSec} msg/s)`);
  lines.push(`Format        : envelope ${j.totals.envelope}  /  raw ${j.totals.raw}`);
  lines.push(`Duplicates    : ${j.totals.duplicates}`);
  if (j.totals.faultsInjected > 0) {
    lines.push(`Faults inject : ${j.totals.faultsInjected}`);
  }

  lines.push('');
  lines.push('Responded HTTP status:');
  for (const [code, n] of sortEntries(j.respondedStatus)) {
    lines.push(`  ${code} : ${n}`);
  }

  if (Object.keys(j.byDocumentType).length > 0) {
    lines.push('');
    lines.push('By Document_Type:');
    for (const [t, n] of sortEntries(j.byDocumentType)) {
      lines.push(`  ${t} : ${n}`);
    }
  }

  const distinctDIds = Object.keys(j.byDId).length;
  lines.push('');
  lines.push(`Distinct D-Ids: ${distinctDIds}`);

  const l = j.e2eLatencyMs;
  lines.push('');
  lines.push('End-to-end latency (ms, envelope deliveries only):');
  if (l.count === 0) {
    lines.push('  (no samples — raw deliveries carry no server timestamp)');
  } else {
    lines.push(`  min ${fmt(l.min)}  mean ${fmt(l.mean)}  p50 ${fmt(l.p50)}  p95 ${fmt(l.p95)}  p99 ${fmt(l.p99)}  max ${fmt(l.max)}`);
  }
  lines.push('======================================================');
  return lines.join('\n');
}
