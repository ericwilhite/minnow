/**
 * gzip is the default because most column shapes compress several times over. Some do not:
 * a column of unstructured doubles reaches about 1.07x, which costs a compression pass on
 * write and an inflate on every read to save nothing. The writer notices and stops paying.
 *
 * The codec is recorded per block, so this can only ever change how many bytes a block takes,
 * never what it holds — every case here reads its rows back and checks them.
 */
import { describe, expect, it } from "vitest";
import { inspectBlock } from "../block-format/index.js";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

/** Large enough to clear the 4 KiB floor where compression can begin repaying its CPU cost. */
const ROWS_PER_BLOCK = 16_384;
const BLOCKS = 4;
const TOTAL = ROWS_PER_BLOCK * BLOCKS;

function mulberry32(seed: number): () => number {
  let state = seed;
  return () => {
    state |= 0;
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

async function codecsPerBlock(
  store: MemoryBlockStore,
  columnName: string,
): Promise<{ codecs: string[]; storedBytes: number }> {
  const table = (await store.listTables()).find((candidate) => candidate.name === "t");
  const columnId = table?.columns.find((column) => column.name === columnName)?.id ?? "";
  const segments = await store.listSegments(table?.id);
  const codecs: string[] = [];
  let storedBytes = 0;
  for (const segment of segments) {
    for (const blockId of segment.columnBlockIds[columnId] ?? []) {
      const bytes = await store.getBlock(blockId);
      if (bytes === undefined) throw new Error(`missing block ${blockId}`);
      codecs.push(inspectBlock(bytes).compression);
      storedBytes += bytes.byteLength;
    }
  }
  return { codecs, storedBytes };
}

async function load(values: {
  incompressible: number[];
  compressible: number[];
}): Promise<{ store: MemoryBlockStore; database: MinnowDatabase }> {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, {
    compression: "gzip",
    rowsPerBlock: ROWS_PER_BLOCK,
  });
  await database.createTable({
    name: "t",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "noise", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  await database.insertBatch("t", {
    columns: {
      id: Array.from({ length: TOTAL }, (_, index) => index + 1),
      noise: values.incompressible,
      label: values.compressible.map((value) => `region-${String(value % 4)}`),
    },
  });
  return { store, database };
}

describe("adaptive block compression", () => {
  it("stops compressing a column gzip cannot compress, and keeps compressing one it can", async () => {
    const random = mulberry32(0x51ed);
    const noiseValues = Array.from({ length: TOTAL }, () => random() * 1e6);
    const { store, database } = await load({
      incompressible: noiseValues,
      compressible: Array.from({ length: TOTAL }, (_, index) => index),
    });
    const noise = await codecsPerBlock(store, "noise");
    const label = await codecsPerBlock(store, "label");

    // The first block is probed, found not worthwhile, and immediately re-encoded raw; every
    // block after it reuses that verdict.
    expect(noise.codecs).toHaveLength(BLOCKS);
    expect(noise.codecs[0]).toBe("raw");
    expect(new Set(noise.codecs.slice(1))).toEqual(new Set(["raw"]));
    // A column that compresses is untouched by any of this.
    expect(new Set(label.codecs)).toEqual(new Set(["gzip"]));

    // And the rows still read back exactly.
    const counted = await database.query("SELECT COUNT(*) AS n FROM t", { memoize: false });
    expect(counted.rows[0]?.n).toBe(TOTAL);
    const probes = [1, 20_000, TOTAL];
    const sampled = await database.query(
      `SELECT id, noise, label FROM t WHERE id IN (${probes.join(", ")}) ORDER BY id`,
      { memoize: false },
    );
    expect(sampled.rows.map((row) => Number(row.id))).toEqual(probes);
    for (const [index, row] of sampled.rows.entries()) {
      const source = (probes[index] ?? 0) - 1;
      expect(row.noise).toBe(noiseValues[source]);
      expect(row.label).toBe(`region-${String(source % 4)}`);
    }
  });

  it("never stores a block larger than the uncompressed form", async () => {
    const random = mulberry32(0x2b1f);
    const { store } = await load({
      incompressible: Array.from({ length: TOTAL }, () => random() * 1e6),
      compressible: Array.from({ length: TOTAL }, (_, index) => index),
    });
    const noise = await codecsPerBlock(store, "noise");
    // 8 bytes per double plus per-block envelope; gzip on this shape lands slightly above it.
    expect(noise.storedBytes).toBeLessThan(TOTAL * 8 + BLOCKS * 4096);
  });

  it("judges medium blocks instead of gziping them through a size loophole", async () => {
    const random = mulberry32(0x9182);
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { compression: "gzip", rowsPerBlock: 4_096 });
    await database.createTable({
      name: "t",
      columns: [{ name: "noise", type: "number" }],
    });
    await database.insertBatch("t", {
      columns: { noise: Array.from({ length: 4_096 }, () => random() * 1e6) },
    });
    expect((await codecsPerBlock(store, "noise")).codecs).toEqual(["raw"]);
    store.close();
  });

  it("applies the same adaptive decision to folded output and resumes mixed codecs", async () => {
    const random = mulberry32(0x71c4);
    const { store, database } = await load({
      incompressible: Array.from({ length: TOTAL }, () => random() * 1e6),
      compressible: Array.from({ length: TOTAL }, (_, index) => index),
    });
    await database.insert("t", {
      id: TOTAL + 1,
      noise: random() * 1e6,
      label: "region-0",
    });
    let progress = await database.compactTableStep("t", {
      maxBlocks: 1,
      targetBlockBytes: 512 * 1024,
      outputCompression: "gzip",
    });
    if (progress.jobId === null) throw new Error("Expected a persisted fold");
    const jobId = progress.jobId;
    while (progress.result === null) {
      progress = await database.resumeCompactionJob(jobId, { maxBlocks: 1 });
    }
    const job = await store.getCompactionJob(jobId);
    if (job?.outputSegmentId === null || job?.outputSegmentId === undefined) {
      throw new Error("Expected folded output");
    }
    const segment = await store.getSegment(job.outputSegmentId);
    const table = await store.getTableByName("t");
    if (segment === undefined || table === undefined) throw new Error("Expected folded records");
    const codecs = new Map<string, Set<string>>();
    for (const column of table.columns) {
      const columnCodecs = new Set<string>();
      for (const id of segment.columnBlockIds[column.id] ?? []) {
        const bytes = await store.getBlock(id);
        if (bytes === undefined) throw new Error(`Missing folded block ${id}`);
        columnCodecs.add(inspectBlock(bytes).compression);
      }
      codecs.set(column.name, columnCodecs);
    }
    expect(codecs.get("noise")).toEqual(new Set(["raw"]));
    expect(codecs.get("label")).toEqual(new Set(["gzip"]));
    expect(await database.query("SELECT COUNT(*) AS n FROM t", { memoize: false })).toMatchObject({
      rows: [{ n: TOTAL + 1 }],
    });
    store.close();
  });

  it("leaves an explicitly raw database alone", async () => {
    const random = mulberry32(0x7c31);
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      compression: "raw",
      rowsPerBlock: ROWS_PER_BLOCK,
    });
    await database.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "noise", type: "number" },
        { name: "label", type: "string" },
      ],
    });
    await database.insertBatch("t", {
      columns: {
        id: Array.from({ length: TOTAL }, (_, index) => index + 1),
        noise: Array.from({ length: TOTAL }, () => random() * 1e6),
        label: Array.from({ length: TOTAL }, (_, index) => `region-${String(index % 4)}`),
      },
    });
    expect(new Set((await codecsPerBlock(store, "label")).codecs)).toEqual(new Set(["raw"]));
  });
});
