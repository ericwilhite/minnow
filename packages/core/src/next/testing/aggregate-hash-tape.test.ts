/** A spilled group's pages retain their input order, using bounded directory memory.
 * Law: hash-tape.ts's forward-page contract and spill/contract.ts's checked page contract.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { FileScratch } from "../storage/file-store.js";
import { MemoryFile } from "../storage/memory-store.js";
import { Governor } from "../memory/governor.js";
import { SpillCorruptionError, type SpillFile, type SpillPageRef } from "../spill/contract.js";
import { MemorySpillFile, MemorySpillService } from "../spill/memory-spill.js";
import { INT, vectorOf } from "../exec/join-fixtures.js";
import { PageWriter, SpillFiles } from "../exec/spill-pages.js";
import { Charge, Pool, SpillIo } from "../exec/spill-pool.js";
import { HashTape, TapeDirectory } from "../exec/agg/hash-tape.js";

it.each(seedsFor("aggregate-hash-tape", [0xb4bb, 0xb4bc]))(
  "keeps page order and constant metadata, and cleans up failed reads (seed %i)",
  async (seed) => {
    const reach = createReach("hash tape", [
      "forward pages",
      "bounded references",
      "corruption",
      "cancel during read",
      "close between admissions",
      "zero charges",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 35, max: 90 }), async (count) => {
        reach.beginHistory();
        for (const mode of ["normal", "corrupt", "cancel", "detached"] as const) {
          const governor = new Governor(8 << 20),
            grant = await governor.admit({
              label: "tape I/O",
              minimumBytes: 2 << 20,
              spillable: false,
            }),
            pool = new Pool(new Charge(grant), 2 << 20),
            backend = new MemorySpillService(),
            controller = new AbortController();
          const created: Array<{ file: MemorySpillFile; refs: SpillPageRef[] }> = [];
          let suspend = false,
            entered!: () => void,
            resume!: () => void;
          const reading = new Promise<void>((resolve) => {
              entered = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              resume = resolve;
            });
          const files = new SpillFiles(
            {
              get bytesInUse() {
                return backend.bytesInUse;
              },
              create: async (queryId) => {
                const file = (await backend.create(queryId)) as MemorySpillFile,
                  refs: SpillPageRef[] = [];
                created.push({ file, refs });
                const wrapped: SpillFile = {
                  queryId,
                  get length() {
                    return file.length;
                  },
                  append: async (page) => {
                    const ref = await file.append(page);
                    refs.push(ref);
                    return ref;
                  },
                  read: async (ref) => {
                    if (suspend) {
                      entered();
                      await gate;
                    }
                    return file.read(ref);
                  },
                  readInto: async (ref, target) => {
                    target.set(await wrapped.read(ref));
                  },
                  close: () => file.close(),
                };
                return wrapped;
              },
              sweep: () => backend.sweep(),
            },
            1,
            {
              allocate: () => false,
              free: () => {
                throw new Error("unbuffered");
              },
            },
          );
          const pages = new PageWriter(pool),
            io = new SpillIo(
              pool,
              files,
              pages,
              { grant, queryId: 1, signal: controller.signal },
              () => controller.signal.throwIfAborted(),
            ),
            directory = new TapeDirectory(io, pool),
            tape = new HashTape(directory);
          try {
            for (let page = 0; page < count; page++) {
              const vector = vectorOf(INT, [page * 2, page * 2 + 1]);
              if (vector.form !== "flat") throw new Error("Tape fixture must be flat");
              const columns = [vector];
              const encoded = pages.encode(
                { queryId: 1, run: 1, partition: 0, sequence: page },
                columns,
                2,
              );
              if (!encoded || !tape.write) throw new Error("Unfunded tape fixture");
              await tape.write(encoded, 2);
              expect(tape.retainedPageRefs).toBe(1);
              expect(tape.pages).toHaveLength(0);
            }
            const payload = created[0];
            if (!payload) throw new Error("Missing payload file");
            await tape.seal();
            expect(payload.refs).toHaveLength(count); // Seal rewrites metadata only.
            const result: number[] = [];
            const read = async () => {
              for await (const batch of tape.read([INT])) {
                const vector = batch.columns[0];
                if (vector?.form !== "flat" || vector.data.layout !== "i32")
                  throw new Error("Tape shape");
                result.push(...vector.data.values.subarray(0, batch.length));
              }
            };
            if (mode === "detached") {
              directory.detach();
              pages.close();
              pool.close();
              expect(grant.allocated).toBe(0);
              grant.close();
              await tape.release();
              expect(backend.bytesInUse).toBe(0);
              reach.reach("close between admissions");
            } else if (mode === "corrupt") {
              const ref = payload.refs[0];
              if (!ref) throw new Error("No corruptible page");
              payload.file.corrupt(ref, 0);
              await expect(read()).rejects.toBeInstanceOf(SpillCorruptionError);
              reach.reach("corruption");
            } else if (mode === "cancel") {
              suspend = true;
              const failure = new Error("cancel tape read"),
                work = read();
              const rejected = expect(work).rejects.toBe(failure);
              await reading;
              controller.abort(failure);
              const cleanup = files.closeAll();
              resume();
              await rejected;
              await cleanup;
              reach.reach("cancel during read");
            } else {
              await read();
              expect(result).toEqual(Array.from({ length: count * 2 }, (_, i) => i));
              reach.reach("forward pages");
              reach.reach("bounded references");
            }
          } finally {
            resume();
            await files.closeAll();
            if (mode !== "detached") {
              directory.detach();
              pages.close();
              pool.close();
            }
            expect(grant.allocated).toBe(0);
            grant.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(backend.bytesInUse).toBe(0);
          reach.reach("zero charges");
        }
      }),
      { seed, numRuns: 5 },
    );
    reach.assertReached();
  },
  30000,
);

// Scratch offsets are physical, not logical append ordinals: releasing another file
// can make the next append use an earlier hole. No ordering is promised by SpillPageRef.
it.each(seedsFor("aggregate-hash-tape-reused-extents", [0xb4cf, 0xb4d0]))(
  "keeps tape order across backwards physical directory offsets (seed %i)",
  async (seed) => {
    const reach = createReach("hash tape extent reuse", [
      "earlier extent",
      "forward rows",
      "bounded refs",
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 24, max: 50 }), async (count) => {
        reach.beginHistory();
        const governor = new Governor(8 << 20),
          grant = await governor.admit({
            label: "physical tape",
            minimumBytes: 2 << 20,
            spillable: false,
          }),
          pool = new Pool(new Charge(grant), 2 << 20),
          pages = new PageWriter(pool),
          backend = new ScratchSpillService(
            new FileScratch(
              new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
              () => undefined,
            ),
          ),
          unbuffered = {
            allocate: () => false,
            free: () => {
              throw new Error("unbuffered");
            },
          };
        const hole = await backend.create(1, unbuffered);
        await hole.append(new Uint8Array(128 << 10));
        const offsets: number[] = [];
        let created = 0;
        const files = new SpillFiles(
            {
              get bytesInUse() {
                return backend.bytesInUse;
              },
              create: async (queryId, budget) => {
                const file = await backend.create(queryId, budget),
                  index = created++;
                return {
                  queryId,
                  get length() {
                    return file.length;
                  },
                  append: async (page) => {
                    const ref = await file.append(page);
                    if (index === 1) offsets.push(ref.offset);
                    return ref;
                  },
                  read: (ref) => file.read(ref),
                  readInto: (ref, target) => file.readInto(ref, target),
                  close: () => file.close(),
                };
              },
              sweep: () => backend.sweep(),
            },
            1,
            unbuffered,
          ),
          io = new SpillIo(pool, files, pages, { grant, queryId: 1 }, () => undefined),
          directory = new TapeDirectory(io, pool),
          tape = new HashTape(directory);
        let fence: SpillFile | null = null;
        try {
          for (let i = 0; i < count; i++) {
            const column = vectorOf(INT, [i]);
            if (column.form !== "flat") throw new Error("Expected flat tape input");
            const encoded = pages.encode(
              { queryId: 1, run: 0, partition: 0, sequence: i },
              [column],
              1,
            );
            if (!encoded || !tape.write) throw new Error("Unfunded physical tape input");
            await tape.write(encoded, 1);
            if (i === 0) {
              fence = await backend.create(1, unbuffered);
              await fence.append(new Uint8Array(4096));
              await hole.close();
            }
          }
          expect(offsets.some((offset, i) => i > 0 && offset < (offsets[i - 1] ?? 0))).toBe(true);
          reach.reach("earlier extent");
          const rows: number[] = [];
          for await (const batch of tape.read([INT])) {
            const vector = batch.columns[0];
            if (vector?.data.layout !== "i32") throw new Error("Expected integer tape output");
            rows.push(vector.data.values[0] ?? -1);
            expect(tape.retainedPageRefs).toBe(1);
            expect(tape.pages).toHaveLength(0);
          }
          expect(rows).toEqual(Array.from({ length: count }, (_, i) => i));
          reach.reach("forward rows");
          reach.reach("bounded refs");
        } finally {
          await files.closeAll();
          await hole.close();
          await fence?.close();
          await backend.close();
          directory.detach();
          pages.close();
          pool.close();
          expect(grant.allocated).toBe(0);
          grant.close();
        }
        expect(governor.usedBytes).toBe(0);
        expect(backend.bytesInUse).toBe(0);
        reach.reach("zero");
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  },
);
