#!/usr/bin/env node
/**
 * TamaDB Producer Emulator — CLI entry point.
 *
 * Reads a JSONL file and pushes each line as the bare JSON body to a running
 * TamaDB ingestion endpoint, with configurable pacing, concurrency, control
 * headers and deliberate error injection. Prints a run summary.
 *
 * Run:  tamadb-producer --file <path> --type <documentType> [options]
 * Help: tamadb-producer --help
 */

import { writeFile } from 'node:fs/promises';
import { parseConfig, printHelp } from './config.js';
import { run } from './engine.js';

async function main(): Promise<number> {
  let cfg;
  try {
    const parsed = parseConfig(process.argv.slice(2));
    if (parsed === 'help') {
      printHelp();
      return 0;
    }
    cfg = parsed;
  } catch (err) {
    process.stderr.write(`\nArgument error: ${(err as Error).message}\n\nRun with --help for usage.\n`);
    return 2;
  }

  const result = await run(cfg);

  // Human-readable summary to stdout.
  process.stdout.write(result.human + '\n');

  // Optional machine-readable JSON summary.
  if (cfg.jsonOut) {
    await writeFile(cfg.jsonOut, JSON.stringify(result.json, null, 2), 'utf8');
    process.stdout.write(`\nJSON summary written to ${cfg.jsonOut}\n`);
  }

  if (result.exitCode !== 0) {
    process.stderr.write('\nRun failed: could not reach the server (all sends failed at transport layer).\n');
  }
  return result.exitCode;
}

main()
  .then((code) => process.exit(code))
  .catch((err: unknown) => {
    process.stderr.write(`\nUnexpected error: ${err instanceof Error ? err.stack ?? err.message : String(err)}\n`);
    process.exit(3);
  });
