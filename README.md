# Transmission Emulators

Two small command-line tools for exercising a **running** [TamaDB](https://tamadb.cc)
over real HTTP:

- **`tamadb-producer`** reads a JSONL file and pushes each record to a TamaDB
  ingestion endpoint, with configurable pacing, concurrency, control headers and
  deliberate error injection.
- **`tamadb-sink`** is a webhook receiver. It accepts TamaDB's subscription
  deliveries so the producer → TamaDB → consumer path can be exercised and
  measured end to end.

Both are development and test tools. They talk to a server you run yourself,
have **zero runtime dependencies**, and need Node.js 22 or later.

## Install

The package is distributed as a tarball attached to each
[GitHub Release](https://github.com/teneriscc/transmission-emulators/releases).
It is not on the npm registry. Install the compiled package straight from the
release:

```bash
npm install -g https://github.com/teneriscc/transmission-emulators/releases/download/v0.2.0/transmission-emulators-0.2.0.tgz
tamadb-producer --help
tamadb-sink --help
```

Or run without installing:

```bash
npx -p https://github.com/teneriscc/transmission-emulators/releases/download/v0.2.0/transmission-emulators-0.2.0.tgz tamadb-sink --help
```

For another version, replace `0.2.0` in both places in the URL. To remove a
global install: `npm uninstall -g transmission-emulators`.

## Compatibility

| Tool | Needs TamaDB | Why |
|------|--------------|-----|
| `tamadb-producer` | 0.3.0 or later | Sends the bare-body ingestion format with `TamaDB-*` control headers. |
| `tamadb-sink` | 0.7.0 or later | Handles the 0.7.0 and 0.8.0 webhook contracts: raw body by default, `TamaDB-*` delivery headers always sent (0.7.0) or opt-in (0.8.0). Tested live against 0.7.0; 0.8.0 support follows TamaDB's planned 0.8.0 contract and is not yet tested against a 0.8.0 server. |

---

## Producer

Pushes a JSONL file to a running TamaDB. It assumes TamaDB is already configured
(schemas, Document_Types, channels). It is a **pure producer** and registers
nothing. Each line is sent as the **bare JSON body**, with optional `TamaDB-*`
control headers.

```bash
tamadb-producer \
  --file data/orders.jsonl \
  --type orders \
  --url http://localhost:3000 \
  --delay 10
```

Highlights (see `tamadb-producer --help` for everything):

- **Pacing:** `--delay`, `--rate`, `--ramp`, `--burst`.
- **Concurrency:** `--concurrency` virtual producers.
- **Control headers:** `--h-version`, `--h-ttl`, `--h-expire-at`, `--h-schema`.
- **Error injection:** `--error-every`, `--error-rate`, `--error-after`,
  `--error-burst`, combined with `--error-kind`
  (`transport-malformed`, `schema-invalid`, `control-header-invalid`).
- **Output:** a human summary (status-code and TamaDB-code tallies, quarantine
  counts, latency percentiles) and `--json <path>` for a machine-readable one.

Exit codes: `0` ran to completion (TamaDB rejecting individual messages is a
counted outcome, not a failure), `1` the server was unreachable, `2` bad
arguments, `3` unexpected error.

---

## Sink

Receives TamaDB webhook deliveries. **You create the TamaDB subscription**, with
a `callbackUrl` pointing at the sink. The sink is a pure receiver and registers
nothing.

```bash
# Discard deliveries, measure only (the simplest mode)
tamadb-sink --port 4000

# Save every acknowledged delivery as one JSON line
tamadb-sink --port 4000 --sink save-jsonl --out out/received.jsonl

# One .json file per delivery
tamadb-sink --port 4000 --sink file-per-delivery --out out/deliveries

# Watch each delivery arrive (works with any sink mode)
tamadb-sink --port 4000 --print
```

With `--print`, each delivery is written to the terminal as it arrives: the time,
the path, the status the sink returned, the `TamaDB-*` headers (if any) and the
body exactly as received. Printing every delivery slows the sink under heavy
load, so leave it off for load tests.

### Webhook contract (TamaDB 0.7.0 and 0.8.0)

TamaDB configures delivery metadata on two independent layers, and the sink
assumes neither.

**Body.** Chosen per subscription by `options.payloadFormat`:

- **raw** (the default for new subscriptions): the bare projected document, no
  wrapper keys.
- **envelope** (opt-in): `{ subscriptionId, dId, documentType, path?, document, enqueuedAt }`.
  In TamaDB 0.7.0 the time key is `timestamp`; from 0.8.0 it is `enqueuedAt`. The
  sink accepts either.

Subscriptions keep the format they were created with, so one TamaDB can deliver
both shapes at once. The sink detects the shape per delivery. An envelope is
recognised only when its four always-present keys and a time key are all there,
so a business document that merely has a `document` field is not mistaken for one.

**Headers.** Sent on every delivery in TamaDB 0.7.0. From 0.8.0, sent only when
the subscription sets `protocolMetadata: true`. When sent, in both formats:

| Header | Value |
|--------|-------|
| `TamaDB-Subscription-Id` | the subscription that fired |
| `TamaDB-Document-Type` | percent-encoded Document_Type |
| `TamaDB-D-Id` | percent-encoded parts joined by `:` (split on `:` first, then decode each part) |
| `TamaDB-Enqueued-At` | ISO-8601 UTC, ms precision; equals the envelope's time key |
| `TamaDB-Path` | only for a node-scoped subscription |

When the headers are sent, context comes from them for **both** formats, so
per-Document_Type, per-subscription and per-D-Id tallies and latency all work for
raw deliveries, and an envelope body is cross-checked against its headers.

**Without headers** (the 0.8.0 default), an envelope still carries its own context.
A raw delivery carries none: the sink counts it as *unattributed*, with no
latency sample and no redelivery/duplicate tracking. Set `protocolMetadata: true`
on the subscription if you need that for raw deliveries.

**Missing headers are not an error by default**, because the sink cannot see a
subscription's settings and so cannot tell an opt-out from a fault. Use
`--require-headers` when headers must be there (a 0.7.0 server, or an opted-in
subscription); then a delivery without them is a contract warning.

**Delivery semantics.** Delivery is best-effort and TamaDB's queue is in-memory
(lost on restart). A non-2xx response or a dropped connection is a failed attempt.
Under `retry`, TamaDB backs off 1/2/4/8/16 s and re-sends the same body and
headers.

### What the summary reports

- **TamaDB headers**: how many deliveries carried the `TamaDB-*` headers
  (present), none (absent) or only some (partial), and how many were
  unattributed.
- **Enqueue→receive latency**, from `TamaDB-Enqueued-At` (or the envelope's time
  key) to arrival. It includes
  TamaDB's pump pickup delay and, on a retry, the backoff wait (the timestamp does
  not change between attempts). It assumes the TamaDB host and this machine agree
  on the time; with TamaDB in a VM or container, clock drift can skew it.
- **Redeliveries vs duplicates.** A retry re-sends the same body and headers, so a
  job is identified by (subscription, D-Id, enqueued-at). A repeat after a
  **failed** attempt is a *redelivery* (expected under `retry`). A repeat after an
  **acknowledged** one is a *duplicate* (suspect). Millisecond precision means two
  distinct writes to one D-Id in the same millisecond would read as a duplicate.
- **Contract warnings**, shown only when something is off: a delivery with some
  but not all required `TamaDB-*` headers, an envelope that disagrees with its
  headers, or (with `--require-headers`) a delivery with no headers at all.

### Saving

By default a saved file holds the bare body, so a raw-format JSONL diffs directly
against the producer's input. Only **acknowledged** (2xx) attempts are saved, so
each delivered job appears once even when faults were injected. Failed attempts
still count in the summary. Raw bodies don't carry Document_Type, D-Id or path, so
add `--save-context` to save `{ "headers": {...}, "body": ... }` instead (the
headers object is empty when TamaDB sent none).

### Fault injection

Exercise TamaDB's retry and backoff behaviour:

```bash
# Return 503 on every 5th delivery
tamadb-sink --port 4000 --fault non-2xx --fault-every 5 --fault-status 503

# Hold ~10% of responses for 2 s (seeded, reproducible)
tamadb-sink --port 4000 --fault delay --fault-rate 10 --fault-delay 2000 --seed 42

# Drop the connection (no response) on every 3rd delivery
tamadb-sink --port 4000 --fault drop --fault-every 3
```

### Lifecycle

The sink runs until `Ctrl-C`, or until `--idle-stop <ms>` passes with no
deliveries (handy for scripts). When testing faults, keep `--idle-stop` above
TamaDB's retry backoff (up to 16 s), or the sink exits before the retries arrive.
On shutdown it prints a summary and, with `--json <path>`, writes a
machine-readable one.

### TamaDB in Docker, sink on the host

Bind the sink to all interfaces (`--host 0.0.0.0`; the default `127.0.0.1` is not
reachable from a container) and use `http://host.docker.internal:<port>/` as the
subscription's `callbackUrl`.

---

## End-to-end loop

1. Start TamaDB (0.7.0 or later) and configure its schemas, Document_Types and channels.
2. Start the sink: `tamadb-sink --port 4000 --sink save-jsonl --out out/received.jsonl`.
3. Create a TamaDB subscription whose `callbackUrl` is `http://<sink-host>:4000/`.
4. Run the producer: `tamadb-producer --file data/orders.jsonl --type orders`.
5. Compare what was sent with what was received, and read both summaries.

## Running several at once

Each process is independent: it owns its own metrics and shares no state with any
other. You can run many producers and sinks in parallel, on one machine or across
VMs and containers. The shared resources are TamaDB itself and, when co-located,
the host's CPU, network and disk. The sink's `file-per-delivery` mode is the
heaviest on the filesystem.

## Develop from source

```bash
npm install
npm run producer -- --help      # runs src/ through tsx
npm run sink -- --help
npm run typecheck
npm run build                   # emits dist/, which is what the package ships
```

```
src/
  core/       shared helpers: JSONL reader, metrics, latency stats, summary formatting
  producer/   the producer: config, pacing, error injection, sender, engine, CLI entry
  consumer/   the sink: delivery contract, metrics, sink modes, faults, server, CLI entry
```

## Safety

- The producer only POSTs the JSONL you give it to the URL you give it. The sink
  only receives what your subscription sends it. Nothing is sent anywhere else.
- Defaults are local: the producer targets `http://localhost:3000` and the sink
  binds `127.0.0.1:4000`.
- Keep your test datasets out of version control. Run outputs under `out/` are
  git-ignored.

## License

Apache-2.0. See [LICENSE](LICENSE).
