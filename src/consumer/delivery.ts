/**
 * Consumer — payload-format awareness (brief §8, TamaDB Req 35.5b).
 *
 * TamaDB delivers a webhook in one of two shapes, chosen PER SUBSCRIPTION by
 * whoever created the subscription (`payloadFormat: 'envelope' | 'raw'`,
 * default 'envelope'; see TamaDB src/subscription/subscription-manager.ts):
 *
 *   envelope: {
 *     subscriptionId: string,
 *     dId: string | string[],          // wire form: bare string | array
 *     documentType: string,
 *     document: <the projected document>,
 *     timestamp: string,               // server-side ISO-8601 at format time
 *     path?: string                    // present only for path-scoped subs
 *   }
 *
 *   raw:      <the projected document>  // the document itself, top-level
 *
 * Since (per the agreed design) the OPERATOR pre-creates the subscription, the
 * operator controls which shape this sink receives — and a tester will create
 * both. So the sink must detect which arrived and normalise to one internal
 * record. The envelope metadata is also what the delivery metrics attribute by
 * (Document_Type / D-Id) and what provides the produced-side timestamp for
 * end-to-end latency; a raw delivery degrades those gracefully to "unknown".
 *
 * Detection is structural and deliberately conservative: a body is treated as
 * an envelope only when it is a JSON object carrying the envelope's marker keys
 * (`document` present, plus `subscriptionId` and `documentType`). Anything else
 * — including a raw document that happens to contain a `document` field — is
 * treated as raw unless it also carries the subscription markers. This avoids
 * mis-reading a business document that merely has a `document` property.
 */

export type DeliveryFormat = 'envelope' | 'raw';

/** D-Id in wire form, as TamaDB serialises it in the envelope. */
export type WireDId = string | string[];

/** A received delivery, normalised across envelope/raw. */
export interface NormalisedDelivery {
  /** Which shape arrived. */
  readonly format: DeliveryFormat;
  /** The document itself (envelope.document, or the raw body). */
  readonly document: unknown;
  /** Subscription id (envelope only). */
  readonly subscriptionId?: string;
  /** Document_Type (envelope only). */
  readonly documentType?: string;
  /** D-Id in wire form (envelope only). */
  readonly dId?: WireDId;
  /** Node path, present only for path-scoped subscriptions (envelope only). */
  readonly path?: string;
  /**
   * Server-side timestamp from the envelope (ISO-8601), if present. Used as the
   * "produced" reference for end-to-end latency. Absent for raw deliveries.
   */
  readonly serverTimestamp?: string;
  /** The exact parsed JSON body as received, retained for save sinks. */
  readonly rawBody: unknown;
}

/** True when the parsed body is a non-null, non-array object. */
function isObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

/**
 * Does this object look like a TamaDB envelope? Requires the document slot plus
 * the two always-present envelope markers. `timestamp`/`dId` are expected too
 * but we key on the structural markers that a business document is least likely
 * to collide with.
 */
function looksLikeEnvelope(o: Record<string, unknown>): boolean {
  return 'document' in o && 'subscriptionId' in o && 'documentType' in o;
}

/**
 * Normalise a parsed delivery body into a NormalisedDelivery. Pure and
 * side-effect-free (safe to call from any sink / worker without cross-talk).
 */
export function normaliseDelivery(body: unknown): NormalisedDelivery {
  if (isObject(body) && looksLikeEnvelope(body)) {
    const subscriptionId = typeof body['subscriptionId'] === 'string' ? body['subscriptionId'] : undefined;
    const documentType = typeof body['documentType'] === 'string' ? body['documentType'] : undefined;
    const serverTimestamp = typeof body['timestamp'] === 'string' ? body['timestamp'] : undefined;
    const path = typeof body['path'] === 'string' ? body['path'] : undefined;
    const dId = toWireDId(body['dId']);

    return {
      format: 'envelope',
      document: body['document'],
      ...(subscriptionId !== undefined ? { subscriptionId } : {}),
      ...(documentType !== undefined ? { documentType } : {}),
      ...(dId !== undefined ? { dId } : {}),
      ...(path !== undefined ? { path } : {}),
      ...(serverTimestamp !== undefined ? { serverTimestamp } : {}),
      rawBody: body,
    };
  }

  // Raw delivery: the body IS the document. No attributable metadata.
  return { format: 'raw', document: body, rawBody: body };
}

/** Coerce the envelope's dId field to wire form (string | string[]) or undefined. */
function toWireDId(v: unknown): WireDId | undefined {
  if (typeof v === 'string') return v;
  if (Array.isArray(v) && v.every((p) => typeof p === 'string')) return v as string[];
  return undefined;
}

/** A stable string key for a D-Id, for per-D-Id tallies and gap/dup tracking. */
export function dIdKey(dId: WireDId | undefined): string {
  if (dId === undefined) return '(unknown)';
  return Array.isArray(dId) ? dId.join('\u0001') : dId;
}
