/**
 * Consumer — delivery metrics (brief §8: "Delivery metrics").
 *
 * Concurrency discipline mirrors the producer's core/metrics.ts: there is NO
 * shared mutable state beyond a single instance. The server owns ONE
 * ConsumerMetrics instance and records into it from its request handler. Node
 * runs JS single-threaded per process, so a single owning instance is race-free
 * within a process; across processes (multiple sink daemons / VMs) each has its
 * own instance, exactly as with the producer. If a future version fans delivery
 * handling across worker_threads, keep one instance per worker and merge with
 * `mergeConsumerMetrics` (a pure function returning a NEW instance).
 *
 * What it does NOT do: it does not attempt true cross-run gap detection (which
 * would need a known expected key space). Duplicate detection is per-run, by
 * (dId, serverTimestamp) when both are present on an envelope.
 */

import { latencyStats, type LatencyStats } from '../core/metrics.js';
import type { DeliveryFormat, NormalisedDelivery, WireDId } from './delivery.js';
import { dIdKey } from './delivery.js';

/** One observed delivery outcome, as seen by the sink. */
export interface DeliveryObservation {
  readonly format: DeliveryFormat;
  readonly documentType?: string;
  readonly dId?: WireDId;
  /**
   * End-to-end latency (ms): received wall-clock minus the envelope's server
   * timestamp. Only computable for envelope deliveries carrying `timestamp`.
   */
  readonly e2eLatencyMs?: number;
  /** HTTP status the sink RETURNED for this delivery (200, or an injected fault). */
  readonly respondedStatus: number;
  /** True if the sink deliberately injected a fault (non-2xx / delay / drop). */
  readonly faultInjected: boolean;
  /** Dedup key: present only when both dId and serverTimestamp were available. */
  readonly dedupKey?: string;
}

export class ConsumerMetrics {
  totalReceived = 0;
  envelopeCount = 0;
  rawCount = 0;
  faultsInjected = 0;
  /** Deliveries whose dedupKey was seen earlier this run. */
  duplicates = 0;

  /** HTTP status the sink returned -> count. */
  readonly respondedStatus = new Map<number, number>();
  /** Document_Type -> count (envelope deliveries only; raw -> "(unknown)"). */
  readonly byDocumentType = new Map<string, number>();
  /** D-Id (wire-form key) -> count. */
  readonly byDId = new Map<string, number>();

  /** End-to-end latencies (ms) for envelope deliveries that carried a timestamp. */
  readonly e2eLatencies: number[] = [];

  /** Seen dedup keys this run (bounded only by distinct deliveries). */
  private readonly seen = new Set<string>();

  record(o: DeliveryObservation): void {
    this.totalReceived += 1;
    if (o.format === 'envelope') this.envelopeCount += 1;
    else this.rawCount += 1;
    if (o.faultInjected) this.faultsInjected += 1;

    bump(this.respondedStatus, o.respondedStatus);
    bump(this.byDocumentType, o.documentType ?? '(unknown)');
    bump(this.byDId, dIdKey(o.dId));

    if (o.e2eLatencyMs !== undefined && Number.isFinite(o.e2eLatencyMs)) {
      this.e2eLatencies.push(o.e2eLatencyMs);
    }

    if (o.dedupKey !== undefined) {
      if (this.seen.has(o.dedupKey)) this.duplicates += 1;
      else this.seen.add(o.dedupKey);
    }
  }

  e2eStats(): LatencyStats {
    return latencyStats(this.e2eLatencies);
  }
}

function bump<K>(map: Map<K, number>, key: K): void {
  map.set(key, (map.get(key) ?? 0) + 1);
}

/**
 * Build a DeliveryObservation from a normalised delivery + sink response facts.
 * `receivedAtMs` is the sink's wall-clock receive time (Date.now()).
 */
export function observe(
  d: NormalisedDelivery,
  receivedAtMs: number,
  respondedStatus: number,
  faultInjected: boolean,
): DeliveryObservation {
  let e2eLatencyMs: number | undefined;
  if (d.serverTimestamp !== undefined) {
    const produced = Date.parse(d.serverTimestamp);
    if (!Number.isNaN(produced)) e2eLatencyMs = receivedAtMs - produced;
  }

  let dedupKey: string | undefined;
  if (d.dId !== undefined && d.serverTimestamp !== undefined) {
    dedupKey = `${dIdKey(d.dId)}@${d.serverTimestamp}`;
  }

  return {
    format: d.format,
    ...(d.documentType !== undefined ? { documentType: d.documentType } : {}),
    ...(d.dId !== undefined ? { dId: d.dId } : {}),
    ...(e2eLatencyMs !== undefined ? { e2eLatencyMs } : {}),
    respondedStatus,
    faultInjected,
    ...(dedupKey !== undefined ? { dedupKey } : {}),
  };
}

/** Merge many per-worker ConsumerMetrics into one (pure; new instance). */
export function mergeConsumerMetrics(parts: readonly ConsumerMetrics[]): ConsumerMetrics {
  const out = new ConsumerMetrics();
  for (const m of parts) {
    out.totalReceived += m.totalReceived;
    out.envelopeCount += m.envelopeCount;
    out.rawCount += m.rawCount;
    out.faultsInjected += m.faultsInjected;
    out.duplicates += m.duplicates;
    for (const [k, v] of m.respondedStatus) out.respondedStatus.set(k, (out.respondedStatus.get(k) ?? 0) + v);
    for (const [k, v] of m.byDocumentType) out.byDocumentType.set(k, (out.byDocumentType.get(k) ?? 0) + v);
    for (const [k, v] of m.byDId) out.byDId.set(k, (out.byDId.get(k) ?? 0) + v);
    out.e2eLatencies.push(...m.e2eLatencies);
  }
  return out;
}
