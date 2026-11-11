/**
 * Consumer — delivery contract handling (TamaDB 0.7.0 webhook contract).
 *
 * BODY. A subscription's `payloadFormat` decides the body shape:
 *
 *   raw (DEFAULT since 0.7.0): the bare projected document, no wrapper keys.
 *   envelope (opt-in):         { subscriptionId, dId, documentType, path?,
 *                                document, timestamp }
 *
 * Subscriptions created before 0.7.0 keep the format they were created with, so
 * a long-lived TamaDB can legitimately deliver BOTH shapes at once. The sink
 * detects which arrived, per delivery.
 *
 * HEADERS. Every delivery, in BOTH formats, carries the delivery context:
 *
 *   TamaDB-Subscription-Id   the subscription that fired
 *   TamaDB-Document-Type     percent-encoded Document_Type
 *   TamaDB-D-Id              percent-encoded parts joined by ":" (split on ":"
 *                            first, THEN decodeURIComponent each part)
 *   TamaDB-Enqueued-At       ISO-8601 UTC, ms precision; when the snapshot was
 *                            captured (equals the envelope `timestamp`)
 *   TamaDB-Path              ONLY for a node-scoped subscription
 *
 * Node lowercases incoming header names, so lookups here use lowercase names.
 *
 * The headers are the authoritative context source. For an envelope the body
 * repeats the same facts; the sink uses the headers first, falls back to the
 * body (e.g. a pre-0.7.0 server that sends no headers), and cross-checks the
 * two so a contract regression shows up as a counted mismatch.
 *
 * Retries re-send the SAME body and headers, so `TamaDB-Enqueued-At` is stable
 * across attempts of one job.
 */

export type DeliveryFormat = 'envelope' | 'raw';

/** D-Id in wire form: a bare string for a single key, an array for a composed key. */
export type WireDId = string | string[];

/** Composed-D-Id delimiter used in the TamaDB-D-Id header (TamaDB default). */
export const D_ID_DELIMITER = ':';

/** Canonical header names (display form). Lookup lowercases them. */
export const DELIVERY_HEADERS = {
  subscriptionId: 'TamaDB-Subscription-Id',
  documentType: 'TamaDB-Document-Type',
  dId: 'TamaDB-D-Id',
  enqueuedAt: 'TamaDB-Enqueued-At',
  path: 'TamaDB-Path',
} as const;

/** Header map as Node presents it (lowercased names). */
export type HeaderMap = Readonly<Record<string, string | string[] | undefined>>;

/** The delivery context: who/what/when a delivery is about. */
export interface DeliveryContext {
  readonly subscriptionId?: string | undefined;
  readonly documentType?: string | undefined;
  readonly dId?: WireDId | undefined;
  /** Node path; present only for node-scoped subscriptions. */
  readonly path?: string | undefined;
  /** ISO-8601 UTC. From TamaDB-Enqueued-At, or the envelope `timestamp`. */
  readonly enqueuedAt?: string | undefined;
}

/** A received delivery, normalised across envelope/raw. */
export interface NormalisedDelivery {
  readonly format: DeliveryFormat;
  /** The document itself (envelope.document, or the whole body when raw). */
  readonly document: unknown;
  /** Effective context: headers first, envelope body as fallback. */
  readonly context: DeliveryContext;
  /**
   * Required delivery headers that were absent or unusable. Non-empty means the
   * server did not honour the 0.7.0 contract (or is a pre-0.7.0 server).
   * `TamaDB-Path` is never "required" (node-scoped subscriptions only).
   */
  readonly missingHeaders: readonly string[];
  /**
   * Fields where an envelope body disagrees with the headers (e.g. `timestamp`
   * vs TamaDB-Enqueued-At). Always empty for raw deliveries.
   */
  readonly mismatches: readonly string[];
  /** The exact parsed body as received (or raw text if it was not JSON). */
  readonly rawBody: unknown;
}

// ─── Headers ────────────────────────────────────────────────────────────────

/** First value of a header, or undefined when absent/empty. `name` is lowercased here. */
function headerValue(headers: HeaderMap, name: string): string | undefined {
  const v = headers[name.toLowerCase()];
  const s = Array.isArray(v) ? v[0] : v;
  return s === undefined || s === '' ? undefined : s;
}

/** decodeURIComponent that never throws: a malformed escape is returned as-is. */
function safeDecode(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

/**
 * Parse a TamaDB-D-Id header value: split on the delimiter FIRST (an encoded
 * delimiter inside a part is %3A, so the split is unambiguous), then decode
 * each part. One part -> string; several -> string[].
 */
export function parseDIdHeader(value: string, delimiter: string = D_ID_DELIMITER): WireDId {
  const parts = value.split(delimiter).map(safeDecode);
  return parts.length === 1 ? parts[0]! : parts;
}

/**
 * Read the delivery context out of the request headers, and report which of the
 * four always-present headers were missing or unusable.
 */
export function parseDeliveryHeaders(
  headers: HeaderMap,
  delimiter: string = D_ID_DELIMITER,
): { context: DeliveryContext; missing: string[] } {
  const subscriptionId = headerValue(headers, DELIVERY_HEADERS.subscriptionId);
  const rawType = headerValue(headers, DELIVERY_HEADERS.documentType);
  const rawDId = headerValue(headers, DELIVERY_HEADERS.dId);
  const rawPath = headerValue(headers, DELIVERY_HEADERS.path);
  const rawAt = headerValue(headers, DELIVERY_HEADERS.enqueuedAt);

  const missing: string[] = [];
  if (subscriptionId === undefined) missing.push(DELIVERY_HEADERS.subscriptionId);
  if (rawType === undefined) missing.push(DELIVERY_HEADERS.documentType);
  if (rawDId === undefined) missing.push(DELIVERY_HEADERS.dId);

  let enqueuedAt: string | undefined;
  if (rawAt === undefined) {
    missing.push(DELIVERY_HEADERS.enqueuedAt);
  } else if (Number.isNaN(Date.parse(rawAt))) {
    missing.push(`${DELIVERY_HEADERS.enqueuedAt} (unparseable)`);
  } else {
    enqueuedAt = rawAt;
  }

  return {
    context: {
      subscriptionId,
      documentType: rawType !== undefined ? safeDecode(rawType) : undefined,
      dId: rawDId !== undefined ? parseDIdHeader(rawDId, delimiter) : undefined,
      path: rawPath !== undefined ? safeDecode(rawPath) : undefined,
      enqueuedAt,
    },
    missing,
  };
}

/** All `TamaDB-*` request headers, verbatim (lowercased names), for save sinks. */
export function pickTamaDbHeaders(headers: HeaderMap): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) {
    if (v !== undefined && k.toLowerCase().startsWith('tamadb-')) {
      out[k] = Array.isArray(v) ? v.join(', ') : v;
    }
  }
  return out;
}

// ─── Body ───────────────────────────────────────────────────────────────────

function isObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

/**
 * Is this body a TamaDB envelope? Requires ALL five always-present envelope
 * keys (`path` is optional and not required). With raw now the default, a
 * business document must never be mistaken for an envelope, so the test is
 * deliberately strict. Matching on keys (not on values) means an envelope whose
 * values contradict the headers is still recognised, and then reported as a
 * mismatch rather than silently reclassified as raw.
 */
function isEnvelopeBody(o: Record<string, unknown>): boolean {
  return (
    'document' in o && 'subscriptionId' in o && 'documentType' in o && 'dId' in o && 'timestamp' in o
  );
}

function toWireDId(v: unknown): WireDId | undefined {
  if (typeof v === 'string') return v;
  if (Array.isArray(v) && v.every((p) => typeof p === 'string')) return v as string[];
  return undefined;
}

function str(v: unknown): string | undefined {
  return typeof v === 'string' ? v : undefined;
}

/**
 * Normalise a delivery. Pure and side-effect-free.
 *
 * @param body    the parsed JSON body (or the raw text if it was not JSON)
 * @param headers the request headers (Node-lowercased)
 */
export function normaliseDelivery(body: unknown, headers: HeaderMap = {}): NormalisedDelivery {
  const { context: fromHeaders, missing } = parseDeliveryHeaders(headers);

  if (!(isObject(body) && isEnvelopeBody(body))) {
    // Raw: the body IS the document; all context comes from the headers.
    return {
      format: 'raw',
      document: body,
      context: fromHeaders,
      missingHeaders: missing,
      mismatches: [],
      rawBody: body,
    };
  }

  const fromBody: DeliveryContext = {
    subscriptionId: str(body['subscriptionId']),
    documentType: str(body['documentType']),
    dId: toWireDId(body['dId']),
    path: str(body['path']),
    enqueuedAt: str(body['timestamp']),
  };

  const context: DeliveryContext = {
    subscriptionId: fromHeaders.subscriptionId ?? fromBody.subscriptionId,
    documentType: fromHeaders.documentType ?? fromBody.documentType,
    dId: fromHeaders.dId ?? fromBody.dId,
    path: fromHeaders.path ?? fromBody.path,
    enqueuedAt: fromHeaders.enqueuedAt ?? fromBody.enqueuedAt,
  };

  // Cross-check only when the server sent headers at all; against a pre-0.7.0
  // server (no headers) there is nothing to compare, and `missingHeaders`
  // already reports it.
  const headersPresent = Object.values(fromHeaders).some((v) => v !== undefined);
  const mismatches = headersPresent ? compareContexts(fromHeaders, fromBody) : [];

  return {
    format: 'envelope',
    document: body['document'],
    context,
    missingHeaders: missing,
    mismatches,
    rawBody: body,
  };
}

/** Fields where header-derived and body-derived context disagree. */
function compareContexts(h: DeliveryContext, b: DeliveryContext): string[] {
  const bad: string[] = [];
  if (h.subscriptionId !== undefined && b.subscriptionId !== undefined && h.subscriptionId !== b.subscriptionId) {
    bad.push('subscriptionId');
  }
  if (h.documentType !== undefined && b.documentType !== undefined && h.documentType !== b.documentType) {
    bad.push('documentType');
  }
  if (h.dId !== undefined && b.dId !== undefined && dIdKey(h.dId) !== dIdKey(b.dId)) {
    bad.push('dId');
  }
  // 0.7.0: the envelope `timestamp` equals the TamaDB-Enqueued-At header.
  if (h.enqueuedAt !== undefined && b.enqueuedAt !== undefined && h.enqueuedAt !== b.enqueuedAt) {
    bad.push('timestamp!=TamaDB-Enqueued-At');
  }
  // TamaDB-Path and envelope `path` are both present for a node-scoped
  // subscription and both absent otherwise, so presence must agree too.
  if (h.path !== b.path) bad.push('path');
  return bad;
}

/** A stable string key for a D-Id, for per-D-Id tallies and duplicate tracking. */
export function dIdKey(dId: WireDId | undefined): string {
  if (dId === undefined) return '(unknown)';
  return Array.isArray(dId) ? dId.join('\u0001') : dId;
}
