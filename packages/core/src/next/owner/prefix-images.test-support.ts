/** Real native source/image fixture shared by payload and complete-input tests. */
import { Governor } from "../memory/governor.js";
import { expect } from "vitest";
import { ChunkReader } from "../page/reader.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { decimal } from "../exec/kernels/test-support.js";
import type { FlatVector } from "../vector/vector.js";
import {
  Harness,
  autocommit,
  rules,
  tableInput,
  column,
  INT,
  TEXT,
  batch,
  ints,
  texts,
} from "../write/test-support.js";
import { RegistryDirectory } from "./registry-directory.js";
import { RegistryFragments } from "./registry-fragments.js";
import { RegistryImages } from "./registry-images.js";
import {
  NativePrefixImages,
  nativePrefixImageBytes,
  nativePrefixSourceBytes,
} from "./prefix-images.js";
import { StatementImpl } from "../write/transaction.js";

export async function prefixFixture(
  rows: number,
  width: number,
  published: boolean,
  nullable: boolean,
  extraTable = false,
  capBytes = 256 * 1024 * 1024,
  nativeFunding = false,
  memtableRows = 256,
) {
  const h = new Harness("strict", "memory", memtableRows);
  Object.defineProperty(h, "governor", { value: new Governor(capBytes) });
  const path = await h.open(),
    type = decimal(38, 0);
  let tableId = 0,
    extraId = 0;
  await autocommit(path, async (s) => {
    tableId =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("p", [
            column("id", INT, false),
            column("n", type),
            column("payload", TEXT),
          ]),
        })
      ).id ?? 0;
  });
  if (extraTable)
    await autocommit(path, async (statement) => {
      extraId =
        (
          await statement.changeCatalog({
            kind: "createTable",
            table: tableInput("source", [
              column("id", INT, false),
              column("n", type),
              column("payload", TEXT),
            ]),
          })
        ).id ?? 0;
    });
  const coefficients = Array.from({ length: rows }, (_, i) =>
    i % 3 === 0 ? BigInt(i + 1) : i % 3 === 1 ? 2n ** 56n + BigInt(i) : 10n ** 30n + BigInt(i),
  );
  const valid = new Uint32Array(Math.ceil(rows / 32));
  for (let i = 0; i < rows; i++)
    if (!nullable || i % 3 !== 2) valid[i >>> 5] = (valid[i >>> 5] ?? 0) | (1 << (i & 31));
  const n: FlatVector = {
    form: "flat",
    type,
    length: rows,
    validity: nullable ? valid : null,
    data: {
      layout: "decimal",
      scale: 0,
      scales: null,
      specials: null,
      coefficients: { repr: "wide", values: coefficients },
    },
  };
  const payloads = Array.from({ length: rows }, (_, i) =>
    nullable && i % 4 === 1 ? null : "雪".repeat(width) + ":" + String(i),
  );
  await autocommit(path, (s) =>
    s
      .table(tableId)
      .insert(batch(ints(coefficients.map((_, i) => i + 1)), n, texts(payloads)), null),
  );
  if (published) {
    await path.recycleLog();
    await path.quiesce();
  }
  const tx = await path.begin(),
    statement = tx.statement({ rules });
  if (!(statement instanceof StatementImpl)) throw new Error("native statement");
  const table = statement.catalog.table(tableId);
  if (!table) throw new Error("native table");
  const root = await h.governor.admit({
      label: "prefix image root",
      minimumBytes: 4 * 1024 * 1024,
      spillable: false,
    }),
    source = await h.governor.admit({
      label: "prefix image source",
      minimumBytes: nativeFunding ? 0 : nativePrefixSourceBytes(table),
      spillable: false,
    }),
    stop = new AbortController(),
    spill = new MemorySpillService(),
    directory = new RegistryDirectory(root, spill, 1, stop.signal),
    fragments = new RegistryFragments(root, spill, 1, stop.signal, () => undefined);
  let capturing = false;
  let preparing: ((signal: AbortSignal) => Promise<void>) | null = null;
  const images = new RegistryImages({
    rootId: 73,
    catalog: statement.catalog,
    directory,
    fragments,
    charge: root,
    signal: stop.signal,
    checkpoint: () => undefined,
    prepare: (bytes) => {
      if (capturing) throw new Error("image capture requested progress while source borrowed");
      expect(root.held - root.allocated).toBeGreaterThanOrEqual(bytes);
    },
  });
  const before = root.allocated;
  const reader = new NativePrefixImages({
    rootId: 73,
    statement,
    tables: [tableId],
    images,
    reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
    charge: root,
    signal: stop.signal,
    checkpoint: () => undefined,
    abortRoot: (reason) => stop.abort(reason),
    prepareSource: async (_table, minimum, signal) => {
      signal.throwIfAborted();
      await preparing?.(signal);
      signal.throwIfAborted();
      expect(source.allocated).toBe(0);
      expect(source.loaned).toBe(0);
      if (nativeFunding) {
        source.release(source.held);
        for (;;) {
          const boundary = await statement.prepareNativeStep(root.held + minimum, signal, {
            includesProgress: true,
          });
          try {
            expect(
              await source.makeRoom?.(minimum, AbortSignal.any([signal, boundary.invalidated])),
            ).toBe(true);
            boundary.check();
            return source;
          } catch (error) {
            if (!boundary.invalidated.aborted || signal.aborted) throw error;
          } finally {
            boundary.release();
          }
        }
      }
      if (minimum > source.held) expect(await source.makeRoom?.(minimum, signal)).toBe(true);
      expect(minimum).toBeLessThanOrEqual(source.held);
      return source;
    },
  });
  expect(root.allocated - before).toBe(nativePrefixImageBytes(3));
  const pin = statement.readOwn(),
    ids: number[] = [];
  try {
    const view = pin.internalTable(tableId);
    for (const segment of view.segments)
      for (let i = 0; i < segment.rowCount; i++) ids.push(view.rowId(segment.index * 65536 + i));
  } finally {
    pin.release();
  }
  return {
    h,
    path,
    statement,
    stop,
    reader,
    images,
    root,
    source,
    ids,
    tableId,
    extraId,
    coefficients,
    payloads,
    fragments,
    directory,
    setPrepare(work: (signal: AbortSignal) => Promise<void>) {
      preparing = work;
    },
    setCapturing(value: boolean) {
      capturing = value;
    },
    async close() {
      await reader.close();
      await images.close();
      await fragments.close();
      await directory.close();
      expect(root.allocated).toBe(0);
      expect(source.allocated).toBe(0);
      expect(source.loaned).toBe(0);
      root.close();
      source.close();
      statement.rollback();
      tx.rollback();
      await h.close();
      expect(h.governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    },
  };
}
