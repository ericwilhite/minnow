/** PC2: supported outer gates make unknown nested tags corruption. A consuming operation must
 * refuse before publishing derived state; opening need not scan every stored data page. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, PAGE_CORRUPTION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { PageEncoding, type PageEntry } from "../format/manifest.js";
import * as snapshotCodec from "../format/snapshot.js";
import * as encoders from "../page/encode.js";
import { parsePage } from "../page/decode.js";
import * as directoryCodec from "../page/directory.js";
import * as writers from "../page/writer.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { SimLocks } from "../storage/sim-disk.js";
import type { FlatVector } from "../vector/vector.js";
import { exportPinnedSnapshot } from "../write/snapshot-export.js";
import { autocommit, batch, column, ints, tableInput, texts } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";

const kinds = ["plain", "constant", "bitmap", "for", "dictionary", "scaled", "gzip"] as const;
type Kind = (typeof kinds)[number];
const expected: Record<Kind, number> = {
  plain: PageEncoding.plain,
  constant: PageEncoding.constant,
  bitmap: PageEncoding.bitmap,
  for: PageEncoding.frameOfReference,
  dictionary: PageEncoding.dictionary,
  scaled: PageEncoding.scaledFloat,
  gzip: PageEncoding.plain,
};
type Mutation = "encoding" | "value tag" | "reserved" | "unused aux" | "directory flags";
function vector(kind: Kind, count: number, salt: number, sparse: boolean): FlatVector {
  const validity = sparse ? new Uint32Array(Math.ceil(count / 32)) : null;
  if (validity !== null)
    for (let row = 0; row < count; row++)
      if (row % 5 !== 0) validity[row >>> 5] = (validity[row >>> 5] ?? 0) | (1 << (row & 31));
  if (kind === "dictionary" || kind === "gzip")
    return texts(
      Array.from({ length: count }, (_, row) =>
        sparse && row % 5 === 0
          ? null
          : kind === "dictionary"
            ? `label${String((row + salt) % 7)}`
            : "compressible ".repeat(80) + String(row + salt),
      ),
    );
  if (kind === "bitmap")
    return {
      form: "flat",
      type: { kind: "bool" },
      length: count,
      validity,
      data: { layout: "bool", values: Uint8Array.from({ length: count }, (_, row) => row & 1) },
    };
  if (kind === "scaled")
    return {
      form: "flat",
      type: { kind: "float64" },
      length: count,
      validity,
      data: {
        layout: "f64",
        values: Float64Array.from({ length: count }, (_, row) => (row + salt) / 100),
      },
    };
  return ints(
    Array.from({ length: count }, (_, row) =>
      sparse && row % 5 === 0
        ? null
        : kind === "constant"
          ? salt
          : kind === "for"
            ? -salt - ((row * 17) % 19)
            : (row & 1 ? 0x70000000 : -0x70000000) + row + salt,
    ),
  );
}
function image(native: MemoryOpfs): string[] {
  return ["root-0", "root-1", "log"].map((name) =>
    Buffer.from(native.readFileBytes(name) ?? []).toString("base64"),
  );
}

async function fixture(
  kind: Kind,
  mutation: Mutation,
  unknown: number,
  bit: number,
  count: number,
  salt: number,
  sparse: boolean,
) {
  const core = new StorageCore(),
    native = new MemoryOpfs();
  Object.assign(core.harness, {
    backend: createOpfsBackend({
      name: "nested-corruption",
      directory: native.root,
      locks: new SimLocks(),
    }),
  });
  await core.open();
  const values = vector(kind, count, salt, sparse);
  await autocommit(core.harness.path, (statement) =>
    statement.changeCatalog({
      kind: "createTable",
      table: tableInput("t", [column("v", values.type)], { primaryKey: null }),
    }),
  );
  await autocommit(core.harness.path, (statement) =>
    statement.table(1).insert(batch(values), null),
  );
  const OriginalWriter = writers.GroupWriter;
  const configure = vi.spyOn(writers, "GroupWriter").mockImplementation(function (options) {
    return new OriginalWriter({
      ...options,
      rawPageBytes: 128 << 10,
      ...(kind === "gzip" ? { dictionaryEntries: 0, gzip: true } : {}),
    });
  });
  const method =
    kind === "bitmap"
      ? "encodeBoolPage"
      : kind === "scaled"
        ? "encodeFloatPage"
        : kind === "dictionary"
          ? "encodeDictionaryPage"
          : kind === "gzip"
            ? "encodeVarlenPage"
            : "encodeInt32Page";
  const original = encoders[method],
    encodeDirectory = directoryCodec.encodeDirectory;
  let damaged = 0;
  const directoryMutation = vi
    .spyOn(directoryCodec, "encodeDirectory")
    .mockImplementation((directory) => {
      const bytes = encodeDirectory(directory);
      if (mutation === "directory flags") {
        bytes[6] = (bytes[6] ?? 0) | 4;
        damaged++;
      }
      return bytes;
    });
  const encode = vi.spyOn(encoders, method).mockImplementation((...args: unknown[]) => {
    const page = Reflect.apply(original, undefined, args) as encoders.EncodedPage;
    expect(page.encoding, `actual stored source ${kind}`).toBe(expected[kind]);
    const entry: PageEntry = {
      offset: 0,
      length: page.bytes.length,
      checksum: 0,
      firstRow: 0,
      rowCount: page.rowCount,
      encoding: page.encoding,
      nullCount: page.nullCount,
      min: page.min,
      max: page.max,
    };
    // Validate the genuine encoder output before mutation. GroupWriter computes checksums
    // AFTER this hook (and after gzip), so refusal cannot be an unrepaired-CRC false positive.
    parsePage(page.bytes, entry, page.bytes[1] ?? 0);
    if (mutation === "encoding") page.bytes[0] = 7 + (unknown % 121);
    else if (mutation === "value tag") page.bytes[1] = 11 + (unknown % 245);
    else if (mutation === "reserved" || mutation === "unused aux")
      new DataView(page.bytes.buffer, page.bytes.byteOffset).setUint32(
        mutation === "reserved" ? 12 : 8,
        (1 << bit) >>> 0,
        true,
      );
    if (mutation !== "directory flags") damaged++;
    return page;
  });
  try {
    await core.harness.path.recycleLog();
    await core.harness.path.quiesce();
    expect(core.harness.path.failure).toBeNull();
    expect(damaged).toBeGreaterThan(0);
  } catch (error) {
    await core.close();
    throw error;
  } finally {
    encode.mockRestore();
    configure.mockRestore();
    directoryMutation.mockRestore();
  }
  // Directory corruption must first be consumed by the operation below, not by inspection.
  if (mutation === "directory flags") return { core, native };
  const grant = await core.harness.governor.admit({
    label: "inspect stored source",
    minimumBytes: 2 << 20,
    spillable: false,
  });
  const pin = core.harness.path.pin();
  try {
    const table = pin.table(1, { budget: grant });
    const segment = table.segments[0];
    expect(segment?.kind).toBe("group");
    if (segment?.kind !== "group") throw new Error("Corruption fixture was not persisted");
    await segment.root();
    const chunk = segment.chunk(1);
    if (chunk === null) throw new Error("Missing stored source column");
    const { ChunkReader } = await import("../page/reader.js");
    const reader = await ChunkReader.open(pin.pages, chunk, { budget: grant });
    try {
      expect(reader.directory.pages.length).toBeGreaterThan(0);
      const encoding = reader.directory.pages[0]?.encoding ?? 0;
      expect(encoding & 127).toBe(expected[kind]);
      expect((encoding & PageEncoding.gzip) !== 0).toBe(kind === "gzip");
    } finally {
      reader.close();
    }
  } finally {
    pin.release();
    expect(grant.allocated).toBe(0);
    grant.close();
  }
  return { core, native };
}

for (const operation of ["query", "export", "index rebuild"] as const)
  for (const seed of seedsFor("next-page-corruption", [0xb2a5, 0xb2a6]))
    it(`refuses checksummed nested corruption through ${operation} seed ${String(seed)}`, async () => {
      const reach = createReach(`page corruption ${operation}`, PAGE_CORRUPTION_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 64, max: 96 }),
          fc.integer({ min: 1, max: 100 }),
          fc.integer({ min: 0, max: 255 }),
          fc.integer({ min: 0, max: 31 }),
          async (count, salt, unknown, bit) => {
            reach.beginHistory();
            for (const kind of kinds)
              for (const mutation of [
                "encoding",
                "value tag",
                "reserved",
                "unused aux",
                "directory flags",
              ] as const)
                for (const sparse of [false, true]) {
                  // These layouts assign aux a value/length/exponent; it is not reserved there.
                  if (mutation === "unused aux" && ["dictionary", "scaled", "gzip"].includes(kind))
                    continue;
                  const { core, native } = await fixture(
                    kind,
                    mutation,
                    unknown,
                    bit,
                    count,
                    salt,
                    sparse,
                  );
                  try {
                    const before = image(native);
                    let failure: unknown;
                    try {
                      if (operation === "query") await core.execute("SELECT v FROM t");
                      else if (operation === "index rebuild")
                        await core.execute("CREATE INDEX ix ON t (v)");
                      else {
                        const pin = core.harness.path.pin(),
                          grant = await core.harness.governor.admit({
                            label: "corrupt export",
                            minimumBytes: 8 << 20,
                            spillable: false,
                          });
                        const footer = vi.spyOn(snapshotCodec, "encodeSnapshotFooter");
                        try {
                          await exportPinnedSnapshot(
                            pin,
                            new Uint8Array(16),
                            (async function* () {
                              yield Uint8Array.of(1);
                            })(),
                            { write: () => undefined },
                            grant,
                          );
                        } finally {
                          expect(footer).not.toHaveBeenCalled();
                          footer.mockRestore();
                          expect(pin.released).toBe(true);
                          expect(grant.allocated).toBe(0);
                          grant.close();
                        }
                      }
                    } catch (error) {
                      failure = error;
                    }
                    expect(failure).toMatchObject({ code: "MINNOW_PAGE_CORRUPT" });
                    expect((failure as Error).message).not.toMatch(/checksum/iu);
                    expect(image(native), "no root/WAL publication from the damaged page").toEqual(
                      before,
                    );
                    reach.reach(kind);
                    reach.reach(mutation);
                    reach.reach(sparse ? "sparse NULLs" : "all valid");
                    reach.reach("checksum-valid refusal");
                    reach.reach("root and WAL unchanged");
                    await core.harness.reopen();
                    expect((await core.execute("SELECT 1")).kind).toBe("rows");
                    await expect(core.execute("SELECT v FROM t")).rejects.toMatchObject({
                      code: "MINNOW_PAGE_CORRUPT",
                    });
                    reach.reach("reopenable");
                  } finally {
                    await core.close();
                  }
                  expect(core.harness.governor.usedBytes).toBe(0);
                  reach.reach("charges released");
                }
          },
        ),
        { seed, numRuns: 2 },
      );
      reach.assertReached();
    }, 240_000);
