/**
 * Consumer — delivery contract handling (TamaDB 0.7.0 and 0.8.0 webhook contracts).
 *
 * TamaDB configures delivery metadata on two independent layers. The sink reads
 * both and assumes neither.
 *
 * BODY (payload layer). A subscription's `payloadFormat` decides the shape:
 *
 *   raw (the default since 0.7.0): the bare projected document, no wrapper keys.
 *   envelope (opt-in):             { subscriptionId, dId, documentType, path?,
 *                                    document, enqueuedAt }
 *
 * The envelope's time key is `timestamp` in TamaDB 0.7.0 and `enqueuedAt` from
 * 0.8.0. Both are accepted. Subscriptions keep the format they were created
 * with, so one TamaDB can deliver both shapes at once; the sink detects the
 * shape per delivery.
 *
 * HEADERS (protocol layer). When sent, the delivery context travels as:
 *
 *   TamaDB-Subscription-Id   the subscription that fired
 *   TamaDB-Document-Type     percent-encoded Document_Type
 *   TamaDB-D-Id              percent-encoded parts joined by ":" (split on ":"
 *                            first, THEN decodeURIComponent each part)
 *   TamaDB-Enqueued-At       ISO-8601 UTC, ms precision; when the snapshot was
 *                            captured (equals the envelope's time key)
 *   TamaDB-Path              ONLY for a node-scoped subscription
 *
 * TamaDB 0.7.0 sends these on every delivery. From 0.8.0 they are sent only
 * when the subscription sets `protocolMetadata: true`. The sink cannot see the
 * subscription's settings, so a delivery with NO TamaDB-* headers is treated as
 * normal ("absent"). A delivery with SOME but not all required headers is a
 * contract violation in either version ("partial"). Callers that know headers
 * must be present (a 0.7.0 server, or an opted-in subscription) can demand them.
 *
 * Node lowercases incoming header names, so lookups here use lowercase names.
 *
 * Context comes from the headers first and from an envelope body as fallback,
 * and the two are cross-checked when both are present. Retries re-send the SAME
 * body and headers, so the enqueued-at value is stable across attempts.
 */

export type DeliveryFormat = 'envelope' | 'raw';

/** D-Id in wire form: a bare string for a single key, an array for a composed key. */
export type WireDId = string | string[];

/**
 * Whether the delivery carried TamaDB's protocol metadata:
 *   present  every required header present and usable
 *   absent   no TamaDB-* delivery header at all (normal from 0.8.0 unless opted in)
 *   partial  some present, some missing or unusable (a contract violation)
 */
export type ProtocolMetadataState = 'present' | 'absent' | 'partial';

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
  /** ISO-8601 UTC. From TamaDB-Enqueued-At, or the envelope's time key. */
  readonly enqueuedAt?: string | undefined;
}

/** A received delivery, normalised across envelope/raw. */
export interface NormalisedDelivery {
  readonly format: DeliveryFormat;
  /** The document itself (envelope.document, or the whole body when raw). */
  readonly document: unknown;
  /** Effective context: headers first, envelope body as fallback. */
  readonly context: DeliveryContext;
  /** Whether TamaDB's protocol metadata (the TamaDB-* headers) was sent. */
  readonly protocolMetadata: ProtocolMetadataState;
  /**
   * Required delivery headers that were absent or unusable. Lists all four when
   * `protocolMetadata` is "absent"; whether that matters is the caller's call.
   * `TamaDB-Path` is never required (node-scoped subscriptions only).
   */
  readonly missingHeaders: readonly string[];
  /**
   * Fields where an envelope body disagrees with the headers (e.g. its time key
   * vs TamaDB-Enqueued-At). Always empty for raw deliveries, and when no
   * headers were sent.
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
 * Read the delivery context out of the request headers. Reports which of the
 * four required headers were missing or unusable, and the overall state.
 */
export function parseDeliveryHeaders(
  headers: HeaderMap,
  delimiter: string = D_ID_DELIMITER,
): { context: DeliveryContext; missing: string[]; state: ProtocolMetadataState } {
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

  const anySent = [subscriptionId, rawType, rawDId, rawPath, rawAt].some((v) => v !== undefined);
  const state: ProtocolMetadataState = !anySent ? 'absent' : missing.length === 0 ? 'present' : 'partial';

  return {
    context: {
      subscriptionId,
      documentType: rawType !== undefined ? safeDecode(rawType) : undefined,
      dId: rawDId !== undefined ? parseDIdHeader(rawDId, delimiter) : undefined,
      path: rawPath !== undefined ? safeDecode(rawPath) : undefined,
      enqueuedAt,
    },
    missing,
    state,
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
 * Is this body a TamaDB envelope? Requires the four always-present keys plus a
 * time key (`enqueuedAt` from 0.8.0, `timestamp` in 0.7.0); `path` is optional.
 * With raw the default, a business document must never be mistaken for an
 * envelope, so the test is deliberately strict. It matches on keys, not values,
 * so an envelope that contradicts its headers is still recognised and then
 * reported as a mismatch rather than silently reclassified as raw.
 */
function isEnvelopeBody(o: Record<string, unknown>): boolean {
  return (
    'document' in o &&
    'subscriptionId' in o &&
    'documentType' in o &&
    'dId' in o &&
    ('enqueuedAt' in o || 'timestamp' in o)
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
  const { context: fromHeaders, missing, state } = parseDeliveryHeaders(headers);

  if (!(isObject(body) && isEnvelopeBody(body))) {
    // Raw: the body IS the document; any context comes from the headers.
    return {
      format: 'raw',
      document: body,
      context: fromHeaders,
      protocolMetadata: state,
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
    enqueuedAt: str(body['enqueuedAt']) ?? str(body['timestamp']),
  };

  const context: DeliveryContext = {
    subscriptionId: fromHeaders.subscriptionId ?? fromBody.subscriptionId,
    documentType: fromHeaders.documentType ?? fromBody.documentType,
    dId: fromHeaders.dId ?? fromBody.dId,
    path: fromHeaders.path ?? fromBody.path,
    enqueuedAt: fromHeaders.enqueuedAt ?? fromBody.enqueuedAt,
  };

  // Cross-check only when headers were sent; with none there is nothing to compare.
  const mismatches = state === 'absent' ? [] : compareContexts(fromHeaders, fromBody);

  return {
    format: 'envelope',
    document: body['document'],
    context,
    protocolMetadata: state,
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
  // The envelope's time key equals the TamaDB-Enqueued-At header.
  if (h.enqueuedAt !== undefined && b.enqueuedAt !== undefined && h.enqueuedAt !== b.enqueuedAt) {
    bad.push('enqueuedAt!=TamaDB-Enqueued-At');
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
