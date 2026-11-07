/**
 * Consumer — fault injection (brief §8: "Fault modes").
 *
 * Lets the sink deliberately misbehave so TamaDB's delivery behaviour can be
 * observed: `retry` vs `fire-and-forget`, backoff, and give-up. Three fault
 * actions, scheduled the same way the producer schedules error injection
 * (every Nth / percentage with a seed) for symmetry and reproducibility:
 *
 *   non-2xx : respond with a configurable error status (default 500) so a
 *             `retry` subscription re-delivers and a `fire-and-forget` one does not.
 *   delay   : hold the response for N ms (exercise timeouts / slow-consumer paths).
 *   drop    : abruptly destroy the socket with no response (connection reset).
 *
 * Per-run, single owning instance (the server holds one). Reuses the producer's
 * seeded RNG so a seed reproduces the exact fault pattern.
 */

import { makeRng } from '../producer/errors.js';

export type FaultAction = 'non-2xx' | 'delay' | 'drop';

export type FaultSchedule =
  | { kind: 'none' }
  | { kind: 'every'; n: number }
  | { kind: 'rate'; pct: number; rng: () => number };

export interface FaultConfig {
  readonly schedule: FaultSchedule;
  readonly action: FaultAction;
  /** Status returned for the `non-2xx` action. */
  readonly status: number;
  /** Milliseconds held before responding for the `delay` action. */
  readonly delayMs: number;
}

export const NO_FAULTS: FaultConfig = {
  schedule: { kind: 'none' },
  action: 'non-2xx',
  status: 500,
  delayMs: 0,
};

/**
 * Decides whether delivery number `index` (0-based) should be faulted.
 * Single owning instance; no shared state.
 */
export class FaultInjector {
  constructor(private readonly cfg: FaultConfig) {}

  get action(): FaultAction {
    return this.cfg.action;
  }
  get status(): number {
    return this.cfg.status;
  }
  get delayMs(): number {
    return this.cfg.delayMs;
  }

  shouldFault(index: number): boolean {
    const s = this.cfg.schedule;
    switch (s.kind) {
      case 'none':
        return false;
      case 'every':
        return s.n > 0 && (index + 1) % s.n === 0;
      case 'rate':
        return s.rng() * 100 < s.pct;
    }
  }
}

/** Build a FaultSchedule from CLI-ish values (shared seed semantics with producer). */
export function makeFaultSchedule(
  every: number | undefined,
  pct: number | undefined,
  seed: number,
): FaultSchedule {
  if (every !== undefined && pct !== undefined) {
    throw new Error('--fault-every and --fault-rate are mutually exclusive');
  }
  if (every !== undefined) return { kind: 'every', n: every };
  if (pct !== undefined) return { kind: 'rate', pct, rng: makeRng(seed) };
  return { kind: 'none' };
}
