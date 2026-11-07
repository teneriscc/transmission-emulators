/**
 * Consumer — sink modes (brief §8).
 *
 *   void              accept and discard; measure throughput/latency only.
 *   save-jsonl        append each delivery as one JSON line to a .jsonl file
 *                     (mirrors the producer's input format → round-trip diffing).
 *   file-per-delivery write each delivery to its own .json file (inspect
 *                     individual payloads; stress high-volume file output).
 *
 * A Sink receives the PARSED body (or the raw text when the body was not JSON)
 * and persists it per mode. Sinks are the one place in the consumer that do
 * disk I/O; under high load the file-per-delivery mode is the deliberate
 * filesystem-stress mode (one small file per message). save-jsonl uses a single
 * append stream to keep the common high-throughput path cheap.
 *
 * Each Sink instance owns its own handles/counter — no shared mutable state, so
 * it stays safe if a future version runs multiple sinks.
 */

import { createWriteStream, type WriteStream } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { once } from 'node:events';

export type SinkMode = 'void' | 'save-jsonl' | 'file-per-delivery';

/** What the server hands a sink for each accepted delivery. */
export interface SinkRecord {
  /** 1-based sequence number assigned by the server on receipt. */
  readonly seq: number;
  /** Parsed JSON body, or the raw string if the body was not valid JSON. */
  readonly body: unknown;
  /** Receive wall-clock (ms) for filename/ordering. */
  readonly receivedAtMs: number;
}

export interface Sink {
  /** Persist one delivery. Must not throw for ordinary I/O backpressure. */
  write(rec: SinkRecord): Promise<void>;
  /** Flush and release resources. */
  close(): Promise<void>;
}

export function createSink(mode: SinkMode, outputPath?: string): Sink {
  switch (mode) {
    case 'void':
      return new VoidSink();
    case 'save-jsonl':
      if (!outputPath) throw new Error('save-jsonl sink requires --out <file.jsonl>');
      return new JsonlSink(outputPath);
    case 'file-per-delivery':
      if (!outputPath) throw new Error('file-per-delivery sink requires --out <directory>');
      return new FilePerDeliverySink(outputPath);
  }
}

/** Discard everything. The simplest, lowest-overhead mode. */
class VoidSink implements Sink {
  async write(): Promise<void> {
    /* intentionally empty */
  }
  async close(): Promise<void> {
    /* nothing to flush */
  }
}

/** Append one JSON line per delivery to a single file via a buffered stream. */
class JsonlSink implements Sink {
  private stream: WriteStream | undefined;

  constructor(private readonly filePath: string) {}

  private ensureStream(): WriteStream {
    if (!this.stream) {
      this.stream = createWriteStream(this.filePath, { flags: 'a', encoding: 'utf8' });
    }
    return this.stream;
  }

  async write(rec: SinkRecord): Promise<void> {
    const stream = this.ensureStream();
    const line = JSON.stringify(rec.body) + '\n';
    // Honour backpressure: if the buffer is full, wait for 'drain'.
    if (!stream.write(line)) {
      await once(stream, 'drain');
    }
  }

  async close(): Promise<void> {
    if (!this.stream) return;
    const s = this.stream;
    this.stream = undefined;
    await new Promise<void>((resolve, reject) => {
      s.end((err?: NodeJS.ErrnoException | null) => (err ? reject(err) : resolve()));
    });
  }
}

/** One .json file per delivery. The high-volume filesystem-stress mode. */
class FilePerDeliverySink implements Sink {
  private dirReady = false;

  constructor(private readonly dir: string) {}

  private async ensureDir(): Promise<void> {
    if (!this.dirReady) {
      await mkdir(this.dir, { recursive: true });
      this.dirReady = true;
    }
  }

  async write(rec: SinkRecord): Promise<void> {
    await this.ensureDir();
    // Zero-padded sequence keeps lexical order == arrival order.
    const name = `delivery-${String(rec.seq).padStart(10, '0')}.json`;
    await writeFile(join(this.dir, name), JSON.stringify(rec.body, null, 2), 'utf8');
  }

  async close(): Promise<void> {
    /* each write is self-contained; nothing to flush */
  }
}
