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
    /** Delivery ATTEMPTS received (a retry counts again). */
    readonly received: number;
    readonly envelope: number;
    readonly raw: number;
    readonly faultsInjected: number;
    readonly duplicates: number;
    readonly redeliveries: number;
    readonly missingHeaderDeliveries: number;
    readonly contextMismatches: number;
    readonly achievedRatePerSec: number;
  };
  readonly respondedStatus: Record<string, number>;
  readonly byDocumentType: Record<string, number>;
  readonly bySubscription: Record<string, number>;
  readonly byDId: Record<string, number>;
  readonly enqueueToReceiveMs: LatencyStats;
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
      redeliveries: metrics.redeliveries,
      missingHeaderDeliveries: metrics.missingHeaderDeliveries,
      contextMismatches: metrics.contextMismatches,
      achievedRatePerSec: round(achievedRatePerSec, 2),
    },
    respondedStatus: mapToObject(metrics.respondedStatus),
    byDocumentType: mapToObject(metrics.byDocumentType),
    bySubscription: mapToObject(metrics.bySubscription),
    byDId: mapToObject(metrics.byDId),
    enqueueToReceiveMs: metrics.latencyStats(),
  };
}

export function formatConsumerSummaryHuman(metrics: ConsumerMetrics, meta: ConsumerRunMeta): string {
  const j = buildConsumerSummaryJson(metrics, meta);
  const t = j.totals;
  const lines: string[] = [];

  lines.push('');
  lines.push('=== Consumer / Webhook-Sink Emulator — Run Summary ===');
  lines.push(`Listening     : ${meta.listenUrl}`);
  lines.push(`Sink mode     : ${meta.sinkMode}${meta.outputPath ? `  (${meta.outputPath})` : ''}`);
  lines.push(`Duration      : ${(meta.durationMs / 1000).toFixed(2)} s`);
  lines.push(`Received      : ${t.received} attempts  (~${t.achievedRatePerSec} /s)`);
  lines.push(`Body format   : raw ${t.raw}  /  envelope ${t.envelope}`);
  lines.push(`Redeliveries  : ${t.redeliveries}  (repeat after a failed attempt — expected under retry)`);
  lines.push(`Duplicates    : ${t.duplicates}  (repeat after an acknowledged attempt)`);
  if (t.faultsInjected > 0) {
    lines.push(`Faults inject : ${t.faultsInjected}`);
  }

  // Contract conformance (TamaDB 0.7.0): surfaced only when something is off.
  if (t.missingHeaderDeliveries > 0 || t.contextMismatches > 0) {
    lines.push('');
    lines.push('CONTRACT WARNINGS:');
    if (t.missingHeaderDeliveries > 0) {
      lines.push(`  ${t.missingHeaderDeliveries} deliveries lacked required TamaDB-* headers (pre-0.7.0 server?)`);
    }
    if (t.contextMismatches > 0) {
      lines.push(`  ${t.contextMismatches} envelope deliveries disagreed with their headers`);
    }
  }

  lines.push('');
  lines.push('Responded HTTP status:');
  for (const [code, n] of sortEntries(j.respondedStatus)) {
    lines.push(`  ${code} : ${n}`);
  }

  if (Object.keys(j.byDocumentType).length > 0) {
    lines.push('');
    lines.push('By Document_Type:');
    for (const [k, n] of sortEntries(j.byDocumentType)) lines.push(`  ${k} : ${n}`);
  }

  if (Object.keys(j.bySubscription).length > 0) {
    lines.push('');
    lines.push('By subscription:');
    for (const [k, n] of sortEntries(j.bySubscription)) lines.push(`  ${k} : ${n}`);
  }

  lines.push('');
  lines.push(`Distinct D-Ids: ${Object.keys(j.byDId).length}`);

  const l = j.enqueueToReceiveMs;
  lines.push('');
  lines.push('Enqueue→receive latency (ms, from TamaDB-Enqueued-At):');
  if (l.count === 0) {
    lines.push('  (no samples — no usable TamaDB-Enqueued-At)');
  } else {
    lines.push(`  min ${fmt(l.min)}  mean ${fmt(l.mean)}  p50 ${fmt(l.p50)}  p95 ${fmt(l.p95)}  p99 ${fmt(l.p99)}  max ${fmt(l.max)}`);
  }
  lines.push('======================================================');
  return lines.join('\n');
}
