/** A bounded serializer buffer. Generators yield before the charged slack can be exhausted. */
import { Slice, type SlicedJob } from "../slices.js";
import { afterYield, yieldTask } from "../pipeline.js";
import { JsonOutput } from "../kernels/json-output.js";
import type { JsonBlob } from "./json-blobs.js";
import { JsonBlobs } from "./json-blobs.js";
export const JSON_FRAGMENT = 65536;
const SLACK = 8192;
class FragmentSlice extends Slice {
  pending: (() => boolean) | null = null;
  override due(): boolean {
    return this.pending?.() === true || super.due();
  }
}
export class JsonStream extends JsonOutput {
  readonly #buffer: Uint8Array;
  #filled = 0;
  #blob: JsonBlob;
  #closed = false;
  constructor(
    readonly log: JsonBlobs,
    readonly clockSlice: Slice,
    readonly check: () => void,
    readonly checkLength?: (length: number) => void,
  ) {
    const slice = new FragmentSlice(clockSlice.ms, clockSlice.clock);
    super(null, slice);
    slice.pending = () => this.pending;
    if (!log.io.pool.allocate(JSON_FRAGMENT + SLACK))
      throw new Error("JSON stream was not admitted");
    this.#buffer = new Uint8Array(JSON_FRAGMENT + SLACK);
    this.#blob = log.empty();
  }
  get pending(): boolean {
    return this.#filled >= JSON_FRAGMENT;
  }
  override put(byte: number): void {
    this.checkLength?.(this.position + 1);
    if (this.#filled >= this.#buffer.length)
      throw new Error("JSON serializer exceeded its yield bound");
    this.#buffer[this.#filled++] = byte;
    this.position++;
  }
  override *copy(bytes: Uint8Array): SlicedJob<void> {
    for (let at = 0; at < bytes.length;) {
      const n = Math.min(bytes.length - at, JSON_FRAGMENT - this.#filled);
      if (n > 0) {
        this.checkLength?.(this.position + n);
        this.#buffer.set(bytes.subarray(at, at + n), this.#filled);
        at += n;
        this.#filled += n;
        this.position += n;
      }
      if (this.#filled >= JSON_FRAGMENT || this.slice.due()) yield;
    }
  }
  async flush(): Promise<void> {
    this.check();
    if (this.#filled === 0) return;
    this.#blob = await this.log.append(this.#blob, this.#buffer.subarray(0, this.#filled));
    this.#filled = 0;
    this.check();
  }
  async run<T>(job: SlicedJob<T>): Promise<T> {
    let complete = false;
    try {
      for (;;) {
        this.check();
        const next = job.next();
        if (next.done) {
          complete = true;
          return next.value;
        }
        if (this.#filled >= JSON_FRAGMENT) await this.flush();
        if (next.value !== undefined) await next.value;
        else if (this.clockSlice.due()) {
          await yieldTask();
          afterYield(this.clockSlice.clock);
        }
      }
    } finally {
      if (!complete) job.return(undefined as T);
    }
  }
  async finish(): Promise<JsonBlob> {
    await this.flush();
    return this.log.seal(this.#blob);
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.log.io.pool.free(JSON_FRAGMENT + SLACK);
  }
}
