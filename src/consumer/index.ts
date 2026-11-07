/**
 * TamaDB Consumer / Webhook-Sink Emulator — CLI entry point.
 *
 * Starts an HTTP sink that receives TamaDB webhook deliveries (the operator
 * pre-creates a subscription pointing its callbackUrl here), normalises the
 * envelope/raw payload format, persists via the chosen sink mode, optionally
 * injects faults, and prints a run summary on shutdown.
 *
 * Run:  tsx src/consumer/index.ts [options]
 * Help: tsx src/consumer/index.ts --help
 */

import { writeFile } from 'node:fs/promises';
import { parseConsumerConfig, printConsumerHelp } from './config.js';
import { startSink } from './server.js';
import { buildConsumerSummaryJson, formatConsumerSummaryHuman, type ConsumerRunMeta } from './summary.js';

async function main(): Promise<number> {
  let cfg;
  try {
    const parsed = parseConsumerConfig(process.argv.slice(2));
    if (parsed === 'help') {
      printConsumerHelp();
      return 0;
    }
    cfg = parsed;
  } catch (err) {
    process.stderr.write(`\nArgument error: ${(err as Error).message}\n\nRun with --help for usage.\n`);
    return 2;
  }

  const startedAt = new Date().toISOString();
  const startClock = performance.now();

  const sink = await startSink(cfg);
  process.stdout.write(`Consumer sink listening on ${sink.listenUrl}  (sink: ${cfg.sinkMode})\n`);
  process.stdout.write('Point a TamaDB subscription callbackUrl here. Ctrl-C to stop.\n');

  // Resolve when either a signal or an idle-stop timeout asks us to finish.
  await new Promise<void>((resolve) => {
    let done = false;
    const finish = (): void => {
      if (done) return;
      done = true;
      resolve();
    };
    sink.setIdleHandler(() => {
      process.stdout.write(`\nIdle for ${cfg.idleStopMs}ms with no deliveries — stopping.\n`);
      finish();
    });
    process.once('SIGINT', finish);
    process.once('SIGTERM', finish);
  });

  await sink.close();

  const meta: ConsumerRunMeta = {
    listenUrl: sink.listenUrl,
    sinkMode: cfg.sinkMode,
    ...(cfg.outputPath !== undefined ? { outputPath: cfg.outputPath } : {}),
    startedAt,
    finishedAt: new Date().toISOString(),
    durationMs: performance.now() - startClock,
  };

  process.stdout.write(formatConsumerSummaryHuman(sink.metrics, meta) + '\n');

  if (cfg.jsonOut) {
    await writeFile(cfg.jsonOut, JSON.stringify(buildConsumerSummaryJson(sink.metrics, meta), null, 2), 'utf8');
    process.stdout.write(`\nJSON summary written to ${cfg.jsonOut}\n`);
  }

  return 0;
}

main()
  .then((code) => process.exit(code))
  .catch((err: unknown) => {
    process.stderr.write(`\nUnexpected error: ${err instanceof Error ? err.stack ?? err.message : String(err)}\n`);
    process.exit(3);
  });
