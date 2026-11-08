/** R8 tickets live on disk. The queue retains handles for immutable A/B snapshots, never a
 * resident retired-offset index. Caller holds the workspace lock before the mutation barrier;
 * all methods use prepaid frames and perform no governor admission or caller wait.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { DATA_ALIGN, StoreCorruptionError, type DataRange } from "./contract.js";
import { openPairs, mergeExtents } from "./metadata-extents.js";
import { MetadataJournal } from "./metadata-journal.js";
import { pairPhase, type MetadataWork } from "./metadata-work.js";
import { disposeSnapshot } from "./extent-snapshot.js";

const TICKET_BYTES = 24;
export const RECLAIM_BATCH_RANGES = 256;
interface Ticket {
  readonly handle: number;
  readonly generation: number;
  readonly before: number;
  readonly after: number;
  readonly bytes: number;
  readonly next: number;
}
export class RetirementTickets {
  #head = 0;
  #tail = 0;
  #waiting = 0;
  #prepared: Ticket | null = null;
  constructor(readonly journal: MetadataJournal) {}
  get waitingBytes(): number {
    return this.#waiting;
  }
  /** All fallible ticket I/O is prepared before native root publication. Workspace serialization
   * keeps the provisional tail link invisible to a reclaimer until commit or abort.
   */
  async prepare(generation: number, before: number, after: number, bytes: number): Promise<void> {
    if (this.#prepared !== null) throw new TypeError("A ticket is already prepared");
    if (
      !Number.isSafeInteger(generation) ||
      generation < 0 ||
      !Number.isSafeInteger(bytes) ||
      bytes < 0
    )
      throw new RangeError("Invalid retirement ticket");
    if (bytes === 0) {
      this.#prepared = { handle: 0, generation, before, after, bytes, next: 0 };
      return;
    }
    const state = { handle: 0, keptBefore: false, keptAfter: false };
    try {
      await this.journal.workspace.run("spool", async () => {
        await this.journal.retain(before);
        state.keptBefore = true;
        await this.journal.retain(after);
        state.keptAfter = true;
        const writer = await this.journal.writer(1, generation);
        try {
          const value = new Uint8Array(TICKET_BYTES),
            view = new DataView(value.buffer);
          view.setFloat64(0, before, true);
          view.setFloat64(8, after, true);
          view.setFloat64(16, bytes, true);
          await writer.append(value);
          state.handle = await writer.finish();
          if (this.#tail !== 0) await this.journal.link(this.#tail, state.handle);
        } catch (error) {
          await writer.abort().catch(() => undefined);
          throw error;
        }
      });
      this.#prepared = { handle: state.handle, generation, before, after, bytes, next: 0 };
    } catch (error) {
      for (const owned of [
        state.handle,
        state.keptBefore ? before : 0,
        state.keptAfter ? after : 0,
      ])
        if (owned !== 0) await disposeSnapshot(this.journal, owned).catch(() => undefined);
      throw error;
    }
  }
  /** Installation is scalar-only: no allocating, native I/O or admission after durable publish. */
  commit(): void {
    const ticket = this.#prepared;
    if (ticket === null) throw new TypeError("No prepared retirement ticket");
    this.#prepared = null;
    if (ticket.handle === 0) return;
    if (this.#head === 0) this.#head = ticket.handle;
    this.#tail = ticket.handle;
    this.#waiting += ticket.bytes;
  }
  async abort(): Promise<void> {
    const ticket = this.#prepared;
    if (ticket === null) return;
    this.#prepared = null;
    if (ticket.handle === 0) return;
    await this.journal.workspace.run("spool", async () => {
      if (this.#tail !== 0) await this.journal.link(this.#tail, 0);
      await this.journal.release(ticket.handle);
      await this.journal.release(ticket.before);
      await this.journal.release(ticket.after);
    });
  }
  async #read(handle: number): Promise<Ticket> {
    return this.journal.workspace.run("spool", async () => {
      const descriptor = await this.journal.describe(handle);
      if (descriptor.length !== TICKET_BYTES)
        throw new StoreCorruptionError("Bad retirement ticket size");
      const cursor = await this.journal.cursor(handle, 1);
      try {
        const bytes = await cursor.next();
        if (bytes?.length !== TICKET_BYTES)
          throw new StoreCorruptionError("Short retirement ticket");
        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.length);
        const before = view.getFloat64(0, true),
          after = view.getFloat64(8, true),
          size = view.getFloat64(16, true);
        if (![before, after, size].every((n) => Number.isSafeInteger(n) && n > 0))
          throw new StoreCorruptionError("Bad retirement ticket coordinates");
        return {
          handle,
          generation: descriptor.tag,
          next: descriptor.next,
          before,
          after,
          bytes: size,
        };
      } finally {
        await cursor.close();
      }
    });
  }
  /** Generate the entire worklist before any native list allocation is released. A/B descriptors
   * and all their pages remain referenced until the final <=256-range release batch completes.
   */
  async reclaim(
    generation: number,
    oldestPin: number | null,
    release: (ranges: readonly DataRange[]) => Promise<void>,
    work?: TaskWork,
    io?: MetadataWork,
  ): Promise<void> {
    if (this.#prepared !== null) throw new TypeError("Reclaim during ticket preparation");
    while (this.#head !== 0) {
      const ticket = await this.#read(this.#head);
      if (generation < ticket.generation + 1 || (oldestPin ?? Infinity) < ticket.generation) return;
      let worklist = 0,
        failed = false;
      try {
        await pairPhase(this.journal, io, "merge", async (slots) => {
          worklist = await mergeExtents(
            this.journal,
            ticket.before,
            ticket.after,
            "difference",
            work,
            "exact",
            slots,
          );
        });
        await this.journal.workspace.run("release", async () => {
          const pairs = await openPairs(this.journal, worklist, 1, true);
          try {
            let more = await pairs.next(work),
              total = 0;
            while (more) {
              const ranges: DataRange[] = [];
              let bytes = 0;
              while (more && ranges.length < RECLAIM_BATCH_RANGES) {
                if (ranges.length % 128 === 0) {
                  const pending = pauseIfDue(work);
                  if (pending !== undefined) await pending;
                }
                const length = Math.ceil(pairs.length / DATA_ALIGN) * DATA_ALIGN;
                ranges.push({ offset: pairs.offset, length });
                bytes += length;
                more = await pairs.next(work);
              }
              if (total + bytes > ticket.bytes)
                throw new StoreCorruptionError("Retirement worklist exceeds ticket");
              await release(ranges);
              total += bytes;
              this.#waiting -= bytes;
            }
            if (total !== ticket.bytes)
              throw new StoreCorruptionError("Retirement worklist disagrees with ticket");
          } finally {
            await pairs.close();
          }
        });
        await this.journal.workspace.run("spool", async () => {
          await this.journal.release(ticket.before);
          await this.journal.release(ticket.after);
          await this.journal.release(ticket.handle);
        });
        this.#head = ticket.next;
        if (this.#head === 0) this.#tail = 0;
      } catch (error) {
        failed = true;
        throw error;
      } finally {
        if (worklist !== 0) {
          await discardWork(this.journal, worklist, failed);
        }
      }
    }
  }
  /** Audit only: under the same workspace lock, enumerate immutable protected roots into the
   * audit's own disk union. No queue or root-reference vector escapes this callback.
   */
  async forEachRoot(visit: (handle: number) => Promise<void>, work?: TaskWork): Promise<void> {
    let head = this.#head;
    while (head !== 0) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
      const ticket = await this.#read(head);
      await visit(ticket.before);
      await visit(ticket.after);
      head = ticket.next;
    }
  }
}

async function discardWork(journal: MetadataJournal, handle: number, failed: boolean) {
  try {
    await disposeSnapshot(journal, handle);
  } catch (error) {
    if (!failed) throw error;
  }
}
