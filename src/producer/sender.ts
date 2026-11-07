/**
 * Producer — HTTP sender (brief §4.1 R1–R3).
 *
 * Sends ONE message as the bare JSON body with Content-Type: application/json,
 * plus optional TamaDB-* control headers. Uses Node's built-in fetch (no deps).
 *
 * Distinguishes two failure classes (R19):
 *   - transportFailure: the request never reached / got a response from the
 *     server (DNS, connection refused, timeout). This is a run-level problem.
 *   - a TamaDB error response (4xx/5xx with a `code`): a normal, counted outcome.
 */

import type { ControlHeaderConfig } from './config.js';
import type { SendOutcome } from '../core/metrics.js';

export interface SendInput {
  readonly url: string;
  readonly body: string;
  readonly baseHeaders: ControlHeaderConfig;
  /** Overriding/extra raw headers (e.g. injected invalid control headers). */
  readonly headerOverrides?: Record<string, string>;
  readonly injectedError: boolean;
  readonly timeoutMs: number;
}

/** Build the request headers from base control-header config + overrides. */
function buildHeaders(base: ControlHeaderConfig, overrides?: Record<string, string>): Record<string, string> {
  const h: Record<string, string> = { 'content-type': 'application/json' };
  if (base.version !== undefined) h['TamaDB-Version'] = base.version;
  if (base.ttl !== undefined) h['TamaDB-TTL'] = base.ttl;
  if (base.expireAt !== undefined) h['TamaDB-Expire-At'] = base.expireAt;
  if (base.schema !== undefined) h['TamaDB-Schema'] = base.schema;
  if (overrides) {
    for (const [k, v] of Object.entries(overrides)) h[k] = v;
  }
  return h;
}

export async function sendOne(input: SendInput): Promise<SendOutcome> {
  const headers = buildHeaders(input.baseHeaders, input.headerOverrides);
  const start = performance.now();

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), input.timeoutMs);

  try {
    const res = await fetch(input.url, {
      method: 'POST',
      headers,
      body: input.body,
      signal: controller.signal,
    });
    const latencyMs = performance.now() - start;

    let tamadbCode: string | undefined;
    let quarantined = false;

    // Parse the response body for TamaDB's code / quarantineRef. Errors are
    // JSON { code, message, quarantineRef? }; successes are { dId, versionNo }.
    try {
      const json = (await res.json()) as Record<string, unknown>;
      if (typeof json['code'] === 'string') tamadbCode = json['code'];
      if (typeof json['quarantineRef'] === 'string') quarantined = true;
    } catch {
      // Non-JSON body (unexpected) — leave code undefined.
    }

    return {
      latencyMs,
      httpStatus: res.status,
      ...(tamadbCode !== undefined ? { tamadbCode } : {}),
      quarantined,
      injectedError: input.injectedError,
      transportFailure: false,
    };
  } catch {
    // fetch threw: network failure, DNS, connection refused, or abort/timeout.
    const latencyMs = performance.now() - start;
    return {
      latencyMs,
      httpStatus: 0,
      quarantined: false,
      injectedError: input.injectedError,
      transportFailure: true,
    };
  } finally {
    clearTimeout(timer);
  }
}
