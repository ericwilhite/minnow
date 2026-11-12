/** Charged encoded payload stays owned until extent append settlement, then boundary publication. */
import type { MaybePromise } from "../access/key-index.js";
import type { RunRef } from "../format/manifest.js";
import type { ExtentBuilder, ExtentWriter } from "../storage/extents.js";
import { RunBuilder, runRef, type RunSummary } from "./runs.js";

/** Bounded encoded payload; append settlement is the storage contract's consumption point. */
export class RunSink {
  #target: ExtentBuilder | null = null;
  constructor(
    readonly builder: RunBuilder,
    readonly writer: ExtentWriter,
    readonly estimate: number,
  ) {}
  drain(force = false): MaybePromise<void> {
    const bytes = this.builder.queuedBytes;
    if (bytes === 0 || (!force && bytes < 256 * 1024)) return;
    const parts = this.builder.takeParts();
    const target = (this.#target ??= this.writer.open(Math.max(this.estimate, bytes)));
    const missing = target.length + bytes - target.capacity;
    if (missing > 0) target.extend(Math.max(missing, 256 * 1024));
    return target.append(parts).then(() => {
      for (const part of parts) this.builder.releasePart(part);
    });
  }
  async finish(outputs: RunSummary[]): Promise<RunRef | null> {
    const summary = this.builder.finish();
    if (summary === null) return null;
    outputs.push(summary); // publication owns boundary keys through success/abort/retry
    await this.drain(true);
    const sealed = await this.#target?.seal();
    return sealed ? runRef(summary, sealed.blobs) : null;
  }
}
