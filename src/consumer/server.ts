/**
 * Consumer — HTTP receiver.
 *
 * A zero-dependency HTTP server (Node built-in `http`) that receives TamaDB
 * webhook deliveries on a configurable path, normalises the payload format,
 * records metrics, persists via the chosen sink, and — unless a fault is
 * injected for that delivery — responds 200.
 *
 * The server owns ONE ConsumerMetrics, ONE Sink and ONE FaultInjector instance
 * (no shared mutable state beyond these owned singletons); Node's single-thread
 * model makes recording race-free within the process. Across processes (several
 * sink daemons / VMs) each has its own set — the same model as the producer.
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { normaliseDelivery } from './delivery.js';
import { ConsumerMetrics, observe } from './metrics.js';
import { createSink, type Sink } from './sinks.js';
import { FaultInjector } from './faults.js';
import type { ConsumerConfig } from './config.js';

export interface RunningSink {
  readonly server: Server;
  readonly metrics: ConsumerMetrics;
  /** The listen URL actually bound. */
  readonly listenUrl: string;
  /**
   * Register a callback fired when no delivery has arrived for `idleStopMs`
   * (only active when `--idle-stop` > 0). Used by the CLI to auto-stop.
   */
  setIdleHandler(fn: () => void): void;
  /** Stop accepting, flush the sink, and resolve when fully closed. */
  close(): Promise<void>;
}

export async function startSink(cfg: ConsumerConfig): Promise<RunningSink> {
  const metrics = new ConsumerMetrics();
  const sink: Sink = createSink(cfg.sinkMode, cfg.outputPath);
  const faults = new FaultInjector(cfg.faults);

  let deliveryIndex = -1; // 0-based across the run, for fault scheduling + seq

  const server = createServer((req, res) => {
    void handleRequest(req, res);
  });

  async function handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
    // Only accept POST on the configured path; everything else is 404 and is
    // NOT counted as a delivery (keeps metrics honest if something probes us).
    const url = req.url ?? '/';
    const pathOnly = url.split('?')[0] ?? '/';
    if (req.method !== 'POST' || pathOnly !== cfg.path) {
      res.writeHead(404, { 'content-type': 'application/json' });
      res.end('{"error":"not found"}');
      return;
    }

    const body = await readBody(req);
    const receivedAtMs = Date.now();
    const seq = (deliveryIndex += 1) + 1;

    // Decide fault for this delivery up-front (index is 0-based).
    const faulted = faults.shouldFault(deliveryIndex);

    // Parse + normalise. A non-JSON body is still a counted delivery; it
    // normalises to a raw delivery whose document is the raw text.
    let parsed: unknown;
    try {
      parsed = JSON.parse(body);
    } catch {
      parsed = body; // keep the raw text; normaliseDelivery treats it as raw
    }
    const delivery = normaliseDelivery(parsed);

    // Persist (void sink is a no-op). Do this before responding so save modes
    // reflect what we acknowledged.
    await sink.write({ seq, body: delivery.rawBody, receivedAtMs });

    // Apply the fault (or respond 200).
    const respondedStatus = await respond(res, faulted, faults);
    metrics.record(observe(delivery, receivedAtMs, respondedStatus, faulted));
    touchActivity();
  }

  // ─── Idle auto-stop (optional) ─────────────────────────────────────────
  let idleTimer: NodeJS.Timeout | undefined;
  let onIdle: (() => void) | undefined;
  function touchActivity(): void {
    if (cfg.idleStopMs <= 0) return;
    if (idleTimer) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => onIdle?.(), cfg.idleStopMs);
    idleTimer.unref();
  }

  await new Promise<void>((resolve) => server.listen(cfg.port, cfg.host, resolve));
  const listenUrl = `http://${cfg.host}:${cfg.port}${cfg.path}`;

  const running: RunningSink = {
    server,
    metrics,
    listenUrl,
    setIdleHandler: (fn: () => void) => {
      onIdle = fn;
    },
    close: () =>
      new Promise<void>((resolve, reject) => {
        if (idleTimer) clearTimeout(idleTimer);
        server.close(async (err) => {
          try {
            await sink.close();
          } catch (e) {
            return reject(e);
          }
          if (err) return reject(err);
          resolve();
        });
      }),
  };

  return running;
}

/** Collect the full request body as a UTF-8 string. */
function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

/**
 * Respond to a delivery. Returns the HTTP status recorded for metrics; for the
 * `drop` action it destroys the socket and records status 0 (no response).
 */
async function respond(res: ServerResponse, faulted: boolean, faults: FaultInjector): Promise<number> {
  if (!faulted) {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end('{"ok":true}');
    return 200;
  }

  switch (faults.action) {
    case 'non-2xx':
      res.writeHead(faults.status, { 'content-type': 'application/json' });
      res.end(`{"ok":false,"injected":true,"status":${faults.status}}`);
      return faults.status;

    case 'delay':
      await sleep(faults.delayMs);
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{"ok":true,"delayed":true}');
      return 200;

    case 'drop':
      // Abruptly reset the connection: no HTTP response at all.
      res.socket?.destroy();
      return 0;
  }
}

function sleep(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, ms));
}
