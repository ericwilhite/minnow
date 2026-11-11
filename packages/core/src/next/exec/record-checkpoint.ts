/** Pressure-only native records that outlive a producer's admitted buffers. The
 * detached descriptor holds files and types, never its writer's pool or grant. */
import type { SpillService } from "../spill/contract.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import { HashTape, TapeDirectory } from "./agg/hash-tape.js";
import { ColumnStore } from "./column-store.js";
import { PageWriter, SpillFiles } from "./spill-pages.js";
import { SpillIo } from "./spill-pool.js";

interface Writer {
  readonly io: SpillIo;
  readonly pages: PageWriter;
  readonly staging: ColumnStore;
}

export class RecordCheckpoint {
  readonly files: SpillFiles;
  readonly directory: TapeDirectory;
  readonly tape: HashTape;
  #writer: Writer | null = null;
  #sealed = false;
  #closing: Promise<void> | null = null;
  constructor(
    parent: SpillIo,
    service: SpillService,
    readonly types: readonly PhysicalType[],
  ) {
    this.files = new SpillFiles(service, parent.context.queryId, {
      allocate: () => false,
      free: () => {
        throw new Error("Unbuffered checkpoint freed a write cache");
      },
    });
    let pages: PageWriter | null = null,
      staging: ColumnStore | null = null,
      directory: TapeDirectory | null = null;
    try {
      pages = new PageWriter(parent.pool, 16 * 1024);
      const io = new SpillIo(
        parent.pool,
        this.files,
        pages,
        parent.context,
        parent.checkAborted,
        parent.pace,
      );
      directory = new TapeDirectory(io, parent.pool);
      staging = new ColumnStore(types, parent.pool, {
        chunkRows: 64,
        targetBytes: 16 * 1024,
        varlenBytes: 256,
      });
      this.directory = directory;
      this.tape = new HashTape(directory);
      this.#writer = { io, pages, staging };
    } catch (error) {
      staging?.close();
      directory?.detach();
      pages?.close();
      throw error;
    }
  }
  async write(columns: readonly Vector[], row: number, count = 1): Promise<void> {
    const writer = this.#writer;
    if (!writer || this.#sealed) throw new Error("Checkpoint writer is detached");
    await writer.io.stage(
      writer.staging,
      this.tape,
      { run: 0, partition: 0 },
      columns,
      null,
      row,
      count,
    );
  }
  #detachWriter(): void {
    const writer = this.#writer;
    this.#writer = null;
    if (!writer) return;
    writer.staging.close();
    this.directory.detach();
    writer.pages.close();
    writer.io.pool.settle();
  }
  async seal(): Promise<void> {
    const writer = this.#writer;
    if (!writer || this.#sealed) throw new Error("Checkpoint already detached");
    await writer.io.flush(writer.staging, this.tape, { run: 0, partition: 0 });
    writer.io.checkAborted();
    this.#sealed = true;
    this.#detachWriter();
  }
  async read(
    parent: SpillIo,
    visit: (columns: readonly FlatVector[], rows: number) => Promise<void>,
  ): Promise<void> {
    if (!this.#sealed || this.#writer) throw new Error("Checkpoint is not sealed");
    const pages = new PageWriter(parent.pool, 16 * 1024);
    let attached = false;
    try {
      // Read fences belong to the saved files' lifetime owner, not the new producer.
      const io = new SpillIo(
        parent.pool,
        this.files,
        pages,
        parent.context,
        parent.checkAborted,
        parent.pace,
      );
      this.directory.attach(io, parent.pool);
      attached = true;
      for await (const batch of this.tape.read(this.types)) {
        parent.checkAborted();
        await visit(batch.columns, batch.length);
      }
    } finally {
      if (attached) this.directory.detach();
      pages.close();
      parent.pool.settle();
    }
  }
  close(): Promise<void> {
    return (this.#closing ??= this.files.closeAll().finally(() => this.#detachWriter()));
  }
}
