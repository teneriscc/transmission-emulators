/**
 * Shared core — metrics accumulation and latency statistics.
 *
 * Concurrency-safety by design (addresses the "same code pushing and consuming
 * simultaneously could conflict" concern): there is NO shared mutable state
 * here. Each virtual producer creates its own `Metrics` instance and mutates
 * only that instance. Instances are combined at the end via `mergeMetrics`,
 * which is a pure function returning a NEW instance. Nothing is written to a
 * shared object concurrently, so parallel producers cannot corrupt each other.
 *
 * Node.js runs JS on a single thread, so within one process there is no true
 * data race; the per-instance discipline additionally keeps the module correct
 * if a future version uses worker_threads.
 */

/** A single observed outcome of one send attempt. */
export interface SendOutcome {
  /** Wall-clock latency of the HTTP round-trip in milliseconds. */
  readonly latencyMs: number;
  /** HTTP status code, or 0 if the request never got a response (transport failure). */
  readonly httpStatus: number;
  /** TamaDB error `code` from the response body, if the response was an error. */
  readonly tamadbCode?: string;
  /** Whether TamaDB returned a quarantineRef for this message. */
  readonly quarantined: boolean;
  /** Whether the emulator deliberately injected an error into this message. */
  readonly injectedError: boolean;
  /** True if the request could not reach the server at all (network/DNS/refused). */
  readonly transportFailure: boolean;
}

export class Metrics {
  totalSent = 0;
  transportFailures = 0;
  injectedErrors = 0;
  quarantineRefs = 0;

  /** httpStatus -> count. Status 0 means "no response" (transport failure). */
  readonly statusCodes = new Map<number, number>();
  /** TamaDB error code (e.g. VERSION_CONFLICT) -> count. */
  readonly tamadbCodes = new Map<string, number>();

  /** All observed latencies (ms), kept for percentile computation. */
  readonly latencies: number[] = [];

  record(o: SendOutcome): void {
    this.totalSent += 1;
    this.latencies.push(o.latencyMs);
    bump(this.statusCodes, o.httpStatus);
    if (o.tamadbCode) bump(this.tamadbCodes, o.tamadbCode);
    if (o.quarantined) this.quarantineRefs += 1;
    if (o.injectedError) this.injectedErrors += 1;
    if (o.transportFailure) this.transportFailures += 1;
  }
}

function bump<K>(map: Map<K, number>, key: K): void {
  map.set(key, (map.get(key) ?? 0) + 1);
}

/** Merge many per-producer Metrics into a single run-level Metrics (pure). */
export function mergeMetrics(parts: readonly Metrics[]): Metrics {
  const out = new Metrics();
  for (const m of parts) {
    out.totalSent += m.totalSent;
    out.transportFailures += m.transportFailures;
    out.injectedErrors += m.injectedErrors;
    out.quarantineRefs += m.quarantineRefs;
    for (const [k, v] of m.statusCodes) out.statusCodes.set(k, (out.statusCodes.get(k) ?? 0) + v);
    for (const [k, v] of m.tamadbCodes) out.tamadbCodes.set(k, (out.tamadbCodes.get(k) ?? 0) + v);
    out.latencies.push(...m.latencies);
  }
  return out;
}

export interface LatencyStats {
  readonly count: number;
  readonly min: number;
  readonly mean: number;
  readonly p50: number;
  readonly p95: number;
  readonly p99: number;
  readonly max: number;
}

/** Compute latency statistics (min/mean/p50/p95/p99/max) from raw samples. */
export function latencyStats(samplesInput: readonly number[]): LatencyStats {
  if (samplesInput.length === 0) {
    return { count: 0, min: 0, mean: 0, p50: 0, p95: 0, p99: 0, max: 0 };
  }
  const samples = [...samplesInput].sort((a, b) => a - b);
  const n = samples.length;
  const sum = samples.reduce((acc, v) => acc + v, 0);
  return {
    count: n,
    min: samples[0]!,
    mean: sum / n,
    p50: percentile(samples, 0.5),
    p95: percentile(samples, 0.95),
    p99: percentile(samples, 0.99),
    max: samples[n - 1]!,
  };
}

/** Nearest-rank percentile over an already-sorted array. */
function percentile(sorted: readonly number[], q: number): number {
  const n = sorted.length;
  if (n === 0) return 0;
  const rank = Math.ceil(q * n);
  const idx = Math.min(Math.max(rank - 1, 0), n - 1);
  return sorted[idx]!;
}
