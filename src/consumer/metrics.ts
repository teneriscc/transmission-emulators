/**
 * Consumer — delivery metrics.
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
 * Retry awareness: a retry re-sends the SAME body and headers, so the
 * enqueued-at value is identical across attempts of one job. That makes
 * (subscription, D-Id, enqueuedAt) a stable job key:
 *   - seen again after a FAILED attempt  -> a redelivery (expected under retry)
 *   - seen again after an ACKNOWLEDGED one -> a duplicate (suspect)
 * Limits: enqueuedAt has millisecond precision, so two distinct writes to the
 * same D-Id within one millisecond would share a key and read as a duplicate.
 * A raw delivery with no TamaDB-* headers carries no subscription, D-Id or
 * enqueued-at at all, so it has no job key and cannot be classified either way.
 */

import { latencyStats, type LatencyStats } from '../core/metrics.js';
import type { DeliveryFormat, NormalisedDelivery, ProtocolMetadataState, WireDId } from './delivery.js';
import { dIdKey } from './delivery.js';

/** One observed delivery attempt, as seen by the sink. */
export interface DeliveryObservation {
  readonly format: DeliveryFormat;
  readonly subscriptionId?: string | undefined;
  readonly documentType?: string | undefined;
  readonly dId?: WireDId | undefined;
  /**
   * Latency (ms) from TamaDB-Enqueued-At to the sink receiving the request.
   * Includes TamaDB's pump pickup delay and, on a retry, the backoff wait
   * (enqueuedAt does not change between attempts). Subject to clock skew
   * between the TamaDB host/VM and this machine.
   */
  readonly enqueueToReceiveMs?: number | undefined;
  /** HTTP status the sink RETURNED (200, or an injected fault; 0 = dropped). */
  readonly respondedStatus: number;
  /** True if the sink deliberately injected a fault (non-2xx / delay / drop). */
  readonly faultInjected: boolean;
  /** Job key; present only when subscription, D-Id and enqueuedAt are all known. */
  readonly jobKey?: string | undefined;
  /** Whether the TamaDB-* headers were present, absent or partial. */
  readonly protocolMetadata: ProtocolMetadataState;
  /**
   * Required headers missing or unusable, counted only when that is a contract
   * problem: always for "partial", and for "absent" only with --require-headers.
   */
  readonly missingHeaderCount: number;
  /** Envelope-vs-header disagreements. */
  readonly mismatchCount: number;
}

export class ConsumerMetrics {
  totalReceived = 0;
  envelopeCount = 0;
  rawCount = 0;
  faultsInjected = 0;
  /** Repeat of a job the sink had already acknowledged (suspect). */
  duplicates = 0;
  /** Repeat of a job whose earlier attempt(s) all failed (expected for retry). */
  redeliveries = 0;
  /** Deliveries with a header problem (see DeliveryObservation.missingHeaderCount). */
  missingHeaderDeliveries = 0;
  /** Deliveries that carried every required TamaDB-* header. */
  headersPresent = 0;
  /** Deliveries with no TamaDB-* headers (normal from 0.8.0 unless opted in). */
  headersAbsent = 0;
  /** Deliveries with some but not all required headers. */
  headersPartial = 0;
  /** Deliveries whose Document_Type could not be determined (raw with no headers). */
  unattributed = 0;
  /** Envelope deliveries whose body disagreed with the headers. */
  contextMismatches = 0;

  /** HTTP status the sink returned -> count. */
  readonly respondedStatus = new Map<number, number>();
  /** Document_Type -> count. */
  readonly byDocumentType = new Map<string, number>();
  /** D-Id (wire-form key) -> count. */
  readonly byDId = new Map<string, number>();
  /** Subscription id -> count (useful when several subscriptions share a sink). */
  readonly bySubscription = new Map<string, number>();

  /** Enqueue→receive latencies (ms). */
  readonly latencies: number[] = [];

  /** jobKey -> whether any earlier attempt was acknowledged (2xx). */
  private readonly jobs = new Map<string, boolean>();

  record(o: DeliveryObservation): void {
    this.totalReceived += 1;
    if (o.format === 'envelope') this.envelopeCount += 1;
    else this.rawCount += 1;
    if (o.faultInjected) this.faultsInjected += 1;
    if (o.missingHeaderCount > 0) this.missingHeaderDeliveries += 1;
    if (o.mismatchCount > 0) this.contextMismatches += 1;
    if (o.protocolMetadata === 'present') this.headersPresent += 1;
    else if (o.protocolMetadata === 'absent') this.headersAbsent += 1;
    else this.headersPartial += 1;
    if (o.documentType === undefined) this.unattributed += 1;

    bump(this.respondedStatus, o.respondedStatus);
    bump(this.byDocumentType, o.documentType ?? '(unknown)');
    bump(this.byDId, dIdKey(o.dId));
    bump(this.bySubscription, o.subscriptionId ?? '(unknown)');

    if (o.enqueueToReceiveMs !== undefined && Number.isFinite(o.enqueueToReceiveMs)) {
      this.latencies.push(o.enqueueToReceiveMs);
    }

    const acked = o.respondedStatus >= 200 && o.respondedStatus < 300;
    if (o.jobKey !== undefined) {
      const priorAck = this.jobs.get(o.jobKey);
      if (priorAck === undefined) {
        this.jobs.set(o.jobKey, acked);
      } else {
        if (priorAck) this.duplicates += 1;
        else this.redeliveries += 1;
        this.jobs.set(o.jobKey, priorAck || acked);
      }
    }
  }

  latencyStats(): LatencyStats {
    return latencyStats(this.latencies);
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
  requireHeaders: boolean = false,
): DeliveryObservation {
  const c = d.context;

  let enqueueToReceiveMs: number | undefined;
  if (c.enqueuedAt !== undefined) {
    const enqueued = Date.parse(c.enqueuedAt);
    if (!Number.isNaN(enqueued)) enqueueToReceiveMs = receivedAtMs - enqueued;
  }

  let jobKey: string | undefined;
  if (c.subscriptionId !== undefined && c.dId !== undefined && c.enqueuedAt !== undefined) {
    jobKey = `${c.subscriptionId}|${dIdKey(c.dId)}|${c.enqueuedAt}`;
  }

  return {
    format: d.format,
    subscriptionId: c.subscriptionId,
    documentType: c.documentType,
    dId: c.dId,
    enqueueToReceiveMs,
    respondedStatus,
    faultInjected,
    jobKey,
    protocolMetadata: d.protocolMetadata,
    missingHeaderCount:
      d.protocolMetadata === 'partial' || (requireHeaders && d.protocolMetadata === 'absent')
        ? d.missingHeaders.length
        : 0,
    mismatchCount: d.mismatches.length,
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
    out.redeliveries += m.redeliveries;
    out.missingHeaderDeliveries += m.missingHeaderDeliveries;
    out.contextMismatches += m.contextMismatches;
    out.headersPresent += m.headersPresent;
    out.headersAbsent += m.headersAbsent;
    out.headersPartial += m.headersPartial;
    out.unattributed += m.unattributed;
    for (const [k, v] of m.respondedStatus) out.respondedStatus.set(k, (out.respondedStatus.get(k) ?? 0) + v);
    for (const [k, v] of m.byDocumentType) out.byDocumentType.set(k, (out.byDocumentType.get(k) ?? 0) + v);
    for (const [k, v] of m.byDId) out.byDId.set(k, (out.byDId.get(k) ?? 0) + v);
    for (const [k, v] of m.bySubscription) out.bySubscription.set(k, (out.bySubscription.get(k) ?? 0) + v);
    out.latencies.push(...m.latencies);
  }
  return out;
}
