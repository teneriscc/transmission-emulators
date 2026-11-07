/**
 * Producer — pacing (brief §4.2 R5–R7) and bursting (§4.3 R8).
 *
 * A Pacer decides how long to wait before sending message number `i` (0-based).
 * All time is derived from a run-relative clock (`elapsedMs`) so ramp and rate
 * modes are stable regardless of how long individual sends take.
 *
 * Per-producer instance: each virtual producer owns its own Pacer, no shared state.
 */

export type PacingMode =
  | { kind: 'none' } // as fast as possible
  | { kind: 'delay'; ms: number } // fixed inter-message delay (R5)
  | { kind: 'rate'; perSec: number } // target msgs/sec (R6)
  | { kind: 'ramp'; startPerSec: number; endPerSec: number; overMs: number } // (R7)
  | { kind: 'burst'; n: number; gapMs: number }; // N back-to-back then pause (R8)

export interface Pacer {
  /**
   * Return the number of milliseconds to sleep BEFORE sending message `index`.
   * `elapsedMs` is milliseconds since the producer started.
   */
  delayBeforeMs(index: number, elapsedMs: number): number;
}

export function makePacer(mode: PacingMode): Pacer {
  switch (mode.kind) {
    case 'none':
      return { delayBeforeMs: () => 0 };

    case 'delay':
      return { delayBeforeMs: (index) => (index === 0 ? 0 : Math.max(0, mode.ms)) };

    case 'rate': {
      const interval = mode.perSec > 0 ? 1000 / mode.perSec : 0;
      // Target absolute send time for message `index`, catch-up aware.
      return {
        delayBeforeMs: (index, elapsedMs) => {
          const targetTime = index * interval;
          return Math.max(0, targetTime - elapsedMs);
        },
      };
    }

    case 'ramp': {
      // Linear ramp of instantaneous rate from start to end over `overMs`.
      // We integrate rate over time to find the target send time per index.
      return {
        delayBeforeMs: (index, elapsedMs) => {
          const targetTime = rampTargetTimeMs(index, mode.startPerSec, mode.endPerSec, mode.overMs);
          return Math.max(0, targetTime - elapsedMs);
        },
      };
    }

    case 'burst': {
      const n = Math.max(1, mode.n);
      return {
        delayBeforeMs: (index) => {
          // Pause AFTER each block of n messages: message n, 2n, ... waits gapMs.
          if (index === 0) return 0;
          return index % n === 0 ? Math.max(0, mode.gapMs) : 0;
        },
      };
    }
  }
}

/**
 * For a linear rate ramp r(t) = r0 + (r1 - r0) * (t / T), the cumulative number
 * of messages sent by time t is the integral:
 *   N(t) = r0*t + (r1 - r0) * t^2 / (2T)   (for t <= T; linear at r1 after T)
 * We invert this to find the time at which the (index+1)-th message is due.
 * Rates are per-second; we convert to per-ms internally.
 */
function rampTargetTimeMs(index: number, startPerSec: number, endPerSec: number, overMs: number): number {
  const r0 = startPerSec / 1000; // per ms
  const r1 = endPerSec / 1000; // per ms
  const T = Math.max(1, overMs);
  const target = index; // cumulative count needed before sending message `index`

  // Messages due within the ramp window: solve N(t) = target for t in [0, T].
  const nAtT = r0 * T + ((r1 - r0) * T) / 2;
  if (target <= nAtT) {
    // Solve (a/2) t^2 + r0 t - target = 0, where a = (r1 - r0)/T.
    const a = (r1 - r0) / T;
    if (Math.abs(a) < 1e-12) {
      // Constant rate degenerate case.
      return r0 > 0 ? target / r0 : 0;
    }
    const A = a / 2;
    const disc = r0 * r0 + 4 * A * target;
    const t = (-r0 + Math.sqrt(Math.max(0, disc))) / (2 * A);
    return t;
  }
  // Past the ramp: constant rate r1 after time T.
  const remaining = target - nAtT;
  return T + (r1 > 0 ? remaining / r1 : 0);
}

/** Promise-based sleep. */
export function sleep(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, ms));
}
