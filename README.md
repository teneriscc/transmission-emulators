# Transmission Emulators

Standalone dev/test tooling for the TamaDB transmission layer (and, later,
PamaDB). Two complementary command-line tools that exercise a **running** server
over real HTTP:

- **Producer emulator** (`src/producer/`) — reads a JSONL file and pushes each
  record to a TamaDB ingestion endpoint, with configurable pacing, concurrency,
  control headers, and deliberate error injection.
- **Consumer / webhook-sink emulator** (`src/consumer/`) — an HTTP endpoint that
  receives TamaDB's webhook deliveries so the producer → TamaDB → consumer path
  can be exercised and measured end to end.

They share a small, side-effect-free core (`src/core/`): JSONL I/O, metric
accumulation, and latency statistics.

> **This is development tooling, not a product.** Neither tool is shipped or run
> in CI by default. Both talk to a server the operator runs separately. This
> repo was extracted from the TamaDB repo once the consumer sink gave the
> emulator set a second reason to exist (the recorded trigger for extraction).

## Why two separate tools, and why they can share a core

Each tool runs as its own OS process. Sharing `src/core/` is **source reuse**,
not runtime coupling: every process loads its own private copy, and the core
holds **no module-level mutable state** — each producer/consumer owns its own
`Metrics`/`ConsumerMetrics` instance and merges with a pure function. So any
number of producers and consumers can run in parallel (many-to-many across
VMs/containers, or co-located daemons) with no cross-talk and no shared-core
penalty. The only genuinely shared resources are the TamaDB server itself and
host CPU/NIC/disk when co-locating — independent of the code sharing.

## Requirements

- Node.js ≥ 22 (built-in `fetch`, `http`, `AbortController`, `util.parseArgs`).
- `tsx` + `typescript` (devDependencies). **Zero runtime dependencies.**

```bash
npm install
```

---

## Producer

Pushes a JSONL file to a running TamaDB over HTTP. Assumes TamaDB is already
configured (schemas / Document_Types / channels); it is a **pure producer** and
registers nothing. Each line is sent as the **bare JSON body** (no wrapper) with
optional `TamaDB-*` control headers.

```bash
npm run producer -- \
  --file data/orders.jsonl \
  --type orders \
  --url http://localhost:3000 \
  --delay 10
```

Full options:

```bash
npm run producer -- --help
```

Highlights: pacing (`--delay` / `--rate` / `--ramp` / `--burst`), concurrency
(`--concurrency`), control headers (`--h-version` / `--h-ttl` / `--h-expire-at`
/ `--h-schema`), error injection (`--error-every` / `--error-rate` /
`--error-after` / `--error-burst` × `--error-kind`), and a human + JSON run
summary (status-code / TamaDB-code tallies, quarantine counts, latency
percentiles).

---

## Consumer / Webhook-Sink

Receives TamaDB webhook deliveries. **The operator pre-creates the TamaDB
subscription** whose `callbackUrl` points at this sink — consistent with the
producer being a pure sender, the sink is a pure receiver and registers nothing.

```bash
# Void sink (discard, measure only) — the simplest mode
npm run consumer -- --port 4000

# Save every delivery as one JSON line (round-trip diffing vs producer input)
npm run consumer -- --port 4000 --sink save-jsonl --out out/received.jsonl

# One .json file per delivery (inspect payloads; stress high-volume file output)
npm run consumer -- --port 4000 --sink file-per-delivery --out out/deliveries
```

Full options:

```bash
npm run consumer -- --help
```

### Webhook contract (TamaDB 0.7.0+)

The sink targets the **0.7.0 webhook contract** (a breaking change from 0.6.x).

**Body** — chosen **per subscription** by `options.payloadFormat`:

- **raw** (the default for new subscriptions) — the bare projected document, no
  wrapper keys.
- **envelope** (opt-in) — `{ subscriptionId, dId, documentType, path?, document, timestamp }`.

Subscriptions created before 0.7.0 keep the format they were created with, so one
TamaDB can deliver both shapes at once. The sink detects the shape per delivery.
(An envelope is recognised by all five of its always-present keys, so a business
document that merely has a `document` field is not mistaken for one.)

**Headers** — on **every** delivery, in both formats:

| Header | Value |
|--------|-------|
| `TamaDB-Subscription-Id` | the subscription that fired |
| `TamaDB-Document-Type` | percent-encoded Document_Type |
| `TamaDB-D-Id` | percent-encoded parts joined by `:` (split on `:` first, then decode each part) |
| `TamaDB-Enqueued-At` | ISO-8601 UTC, ms precision; equals the envelope `timestamp` |
| `TamaDB-Path` | only for a node-scoped subscription |

Context therefore comes from the headers for **both** formats: per-Document_Type,
per-subscription and per-D-Id tallies, and latency, all work for raw deliveries.
An envelope body is cross-checked against its headers.

**What the summary reports**

- *Enqueue→receive latency* — from `TamaDB-Enqueued-At` to arrival. It includes
  TamaDB's pump pickup delay and, on a retry, the backoff wait (the timestamp does
  not change between attempts). It also assumes the TamaDB host/VM clock and this
  machine's clock agree; with TamaDB in a VM or container, drift can skew it.
- *Redeliveries vs duplicates* — a retry re-sends the same body and headers, so a
  job is identified by (subscription, D-Id, enqueued-at). A repeat after a
  **failed** attempt is a *redelivery* (expected under `retry`); a repeat after an
  **acknowledged** one is a *duplicate* (suspect). Millisecond precision means two
  distinct writes to one D-Id in the same millisecond would read as a duplicate.
- *Contract warnings* — shown only when a delivery lacks required `TamaDB-*`
  headers (e.g. a pre-0.7.0 server) or an envelope disagrees with its headers.

**Delivery semantics to keep in mind:** delivery is best-effort and TamaDB's queue
is in-memory (lost on restart). A non-2xx response or a dropped connection is a
failed attempt; under `retry` TamaDB backs off 1/2/4/8/16 s and re-sends the same
body and headers.

**Saving.** By default a saved file holds the bare body, so a raw-format JSONL
diffs directly against the producer's input. Only **acknowledged** (2xx)
attempts are saved, so each delivered job appears once. Because raw bodies no
longer carry Document_Type / D-Id / path, add `--save-context` to save
`{ "headers": {...}, "body": ... }` instead.

**TamaDB in Docker, sink on the host:** bind the sink with `--host 0.0.0.0` and
use `http://host.docker.internal:<port>/` as the subscription `callbackUrl`
(image `teneriscc/tamadb:0.7.0`).

### Fault injection (exercise TamaDB retry / fire-and-forget / backoff)

```bash
# Return 500 on every 5th delivery
npm run consumer -- --port 4000 --fault non-2xx --fault-every 5 --fault-status 500

# Add a 2s delay to ~10% of deliveries (seeded, reproducible)
npm run consumer -- --port 4000 --fault delay --fault-rate 10 --fault-delay 2000 --seed 42

# Drop the connection (no response) on every 3rd delivery
npm run consumer -- --port 4000 --fault drop --fault-every 3
```

### Lifecycle

The sink runs until `Ctrl-C`, or until `--idle-stop <ms>` elapses with no
deliveries (handy for scripted runs). When testing faults, keep `--idle-stop`
above TamaDB's retry backoff (up to 16 s), or the sink exits before the retries
arrive. On shutdown it prints a run summary and, with `--json <path>`, writes a
machine-readable summary.

---

## End-to-end loop (the whole point)

1. Start TamaDB (0.7.0+ — the consumer targets the 0.7.0 webhook contract).
2. Configure schemas / Document_Types / channels in TamaDB.
3. Start the consumer sink: `npm run consumer -- --port 4000 --sink save-jsonl --out out/received.jsonl`.
4. Create a TamaDB subscription whose `callbackUrl` is `http://<sink-host>:4000/`.
5. Run the producer to push traffic: `npm run producer -- --file ... --type ...`.
6. Compare producer-sent vs consumer-received; read both run summaries.

## Typecheck

```bash
npm run typecheck
```

## Layout

```
src/
  core/        Shared, reentrant, side-effect-free helpers
    jsonl.ts     Streaming JSONL reader
    metrics.ts   Send-side metric accumulator + latency stats + pure merge
    summary.ts   Producer summary + shared formatting helpers
  producer/    Pure sender (lifted from the original producer emulator)
    config.ts, pacing.ts, errors.ts, sender.ts, engine.ts, index.ts
  consumer/    Pure receiver (webhook sink)
    delivery.ts  0.7.0 contract: TamaDB-* headers + raw/envelope body detection
    metrics.ts   delivery metrics (latency, per-type/subscription/D-Id,
                 redeliveries vs duplicates, contract conformance)
    summary.ts   consumer run summary (human + JSON)
    sinks.ts     void / save-jsonl / file-per-delivery
    faults.ts    non-2xx / delay / drop fault injection
    config.ts    CLI parsing
    server.ts    HTTP receiver (Node built-in http)
    index.ts     CLI entry point

docs/
  producer-emulator-requirements.md   Original producer brief (§8 = consumer spec)
```

## Data & safety

- Reads JSONL datasets from the private, git-ignored `test_cases/` (never
  committed). Run outputs go to the git-ignored `out/`.
- Neither tool transmits project code or secrets anywhere; the producer only
  POSTs the operator-provided JSONL to the operator-provided URL, and the sink
  only receives what the operator's subscription sends it.
- Defaults are public-safe: `http://localhost:3000` (producer target),
  `127.0.0.1:4000` (sink bind). No private hostnames or local paths baked in.
