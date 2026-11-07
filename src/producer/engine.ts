/**
 * Producer — run engine.
 *
 * Loads the JSONL, partitions work across N virtual producers (R9), and drives
 * each producer's send loop with its own Pacer, ErrorScheduler and Metrics
 * (per-instance state; merged at the end — see core/metrics.ts for the
 * concurrency rationale).
 *
 * Work partitioning: lines are round-robin assigned to producers by index
 * (producer p handles lines where index % concurrency === p). This spreads the
 * dataset evenly without loading it all into memory more than once per producer.
 * Each producer independently streams the file and skips lines not assigned to
 * it, so there is no shared cursor and no locking.
 */

import { readJsonlLines, isJsonObject } from '../core/jsonl.js';
import { Metrics, mergeMetrics } from '../core/metrics.js';
import { buildSummaryJson, formatSummaryHuman, type RunMeta } from '../core/summary.js';
import { makePacer, sleep } from './pacing.js';
import { ErrorScheduler, corrupt, makeRng } from './errors.js';
import { sendOne } from './sender.js';
import type { ProducerConfig } from './config.js';

export interface RunResult {
  readonly metrics: Metrics;
  readonly meta: RunMeta;
  readonly human: string;
  readonly json: ReturnType<typeof buildSummaryJson>;
  /** Overall exit code: non-zero if the server was unreachable for ALL sends. */
  readonly exitCode: number;
}

function ingestUrl(cfg: ProducerConfig): string {
  const path = cfg.channelId
    ? `/api/v1/ingest/${encodeURIComponent(cfg.documentType)}/${encodeURIComponent(cfg.channelId)}`
    : `/api/v1/ingest/${encodeURIComponent(cfg.documentType)}`;
  return `${cfg.baseUrl}${path}`;
}

/** One virtual producer: streams its assigned share and sends each line. */
async function runProducer(
  producerIndex: number,
  cfg: ProducerConfig,
  url: string,
  startClock: number,
): Promise<Metrics> {
  const metrics = new Metrics();
  const pacer = makePacer(cfg.pacing);
  const scheduler = new ErrorScheduler(cfg.errorSchedule);
  // Deterministic-but-distinct RNG per producer for error-kind randomness.
  const rng = makeRng(0xabcdef ^ (producerIndex + 1));

  let localIndex = 0; // index among THIS producer's messages (drives pacing)
  let globalIndex = -1; // index across the whole file (for round-robin assignment)
  let sent = 0;

  for await (const line of readJsonlLines(cfg.file)) {
    globalIndex += 1;
    if (globalIndex % cfg.concurrency !== producerIndex) continue; // not ours

    // Respect --limit across the WHOLE run: each producer stops at its share.
    if (cfg.limit > 0 && sent >= perProducerLimit(cfg.limit, cfg.concurrency, producerIndex)) break;

    // Optionally skip lines that are not valid JSON objects (R4).
    if (cfg.skipInvalidInput && !isJsonObject(line.text)) continue;

    // Decide corruption for this message.
    const inject = scheduler.shouldCorrupt(localIndex);
    let body = line.text;
    let headerOverrides: Record<string, string> | undefined;
    if (inject) {
      const c = corrupt(line.text, cfg.errorKind, rng);
      body = c.body;
      headerOverrides = c.headerOverrides;
    }

    // Pace: wait until this message is due.
    const elapsed = performance.now() - startClock;
    const wait = pacer.delayBeforeMs(localIndex, elapsed);
    if (wait > 0) await sleep(wait);

    const outcome = await sendOne({
      url,
      body,
      baseHeaders: cfg.controlHeaders,
      ...(headerOverrides ? { headerOverrides } : {}),
      injectedError: inject,
      timeoutMs: cfg.timeoutMs,
    });
    metrics.record(outcome);

    localIndex += 1;
    sent += 1;
  }

  return metrics;
}

/** Split a global --limit as evenly as possible across producers. */
function perProducerLimit(limit: number, concurrency: number, producerIndex: number): number {
  const base = Math.floor(limit / concurrency);
  const remainder = limit % concurrency;
  return base + (producerIndex < remainder ? 1 : 0);
}

export async function run(cfg: ProducerConfig): Promise<RunResult> {
  const url = ingestUrl(cfg);
  const startedAt = new Date().toISOString();
  const startClock = performance.now();

  const producers = Array.from({ length: cfg.concurrency }, (_, p) => runProducer(p, cfg, url, startClock));
  const parts = await Promise.all(producers);

  const durationMs = performance.now() - startClock;
  const finishedAt = new Date().toISOString();
  const metrics = mergeMetrics(parts);

  const meta: RunMeta = {
    target: cfg.baseUrl,
    documentType: cfg.documentType,
    ...(cfg.channelId !== undefined ? { channelId: cfg.channelId } : {}),
    startedAt,
    finishedAt,
    durationMs,
    concurrency: cfg.concurrency,
  };

  const human = formatSummaryHuman(metrics, meta);
  const json = buildSummaryJson(metrics, meta);

  // Exit non-zero only if we sent at least one message and EVERY send failed at
  // the transport layer (server unreachable). Individual TamaDB rejections are
  // normal, counted outcomes and do not fail the run (R19).
  const allTransportFailed = metrics.totalSent > 0 && metrics.transportFailures === metrics.totalSent;
  const exitCode = metrics.totalSent === 0 ? 0 : allTransportFailed ? 1 : 0;

  return { metrics, meta, human, json, exitCode };
}
