/** Component and real ordinary-query guard for non-retaining export reads. Setup excluded. */
import { createHash } from "node:crypto";
import { PageCache } from "../../../packages/core/src/next/storage/page-cache.js";
import {
  countingBudget,
  countingMemory,
} from "../../../packages/core/src/next/storage/space-test-support.js";
import { xxHash32 } from "../../../packages/core/src/next/hash/xxhash32.js";
import type { StoreSession } from "../../../packages/core/src/next/storage/contract.js";
import type { BlobRef } from "../../../packages/core/src/next/storage/extents.js";
import { MemoryExtentSpace } from "../../../packages/core/src/next/page/memory-extents.js";
import { writeGroup } from "../../../packages/core/src/next/page/test-support.js";
import { Governor } from "../../../packages/core/src/next/memory/governor.js";
import { ProtocolClient } from "../../../packages/core/src/next/owner/client.js";
import { prepareOwnerExport } from "./owner-export.js";
import { StreamDictionaries } from "../../../packages/core/src/next/owner/frame.js";
import { typeOf, vectorOf } from "./wide-vectors.js";
import { wideSpec } from "./wide-data.js";
import { GroupWriter } from "../../../packages/core/src/next/page/writer.js";

import { pointProfile } from "./point-profile.js";

const clock = () => ({ cpu: process.cpuUsage(), wall: performance.now() });
const elapsed = (before: ReturnType<typeof clock>) => {
  const c = process.cpuUsage(before.cpu);
  return { cpuMs: (c.user + c.system) / 1000, wallMs: performance.now() - before.wall };
};
export async function readRetentionComponent() {
  const data = Uint8Array.from({ length: 8 << 20 }, (_, k) => (k * 17) % 251),
    memory = countingMemory(Infinity),
    budget = countingBudget(Infinity);
  let calls = 0;
  const cache = new PageCache({
    async readDataInto(at: number, out: Uint8Array) {
      calls++;
      out.set(data.subarray(at, at + out.length));
    },
  } as unknown as StoreSession);
  cache.attach(memory);
  const refs: BlobRef[] = Array.from({ length: 2048 }, (_, k) => ({
    extent: { offset: 0, length: data.length },
    offset: k * 4096,
    length: 2048,
    checksum: xxHash32(data.subarray(k * 4096, k * 4096 + 2048)),
  }));
  let cpuMs = 0,
    wallMs = 0,
    peakResident = 0,
    checksum = 0;
  for (let round = 0; round < 16; round++) {
    cache.evict(Infinity);
    const before = clock();
    for (let first = 0; first < refs.length; first += 256) {
      const lease = await cache.read(refs.slice(first, first + 256), { budget, retain: false });
      for (const bytes of lease.bytes) checksum = (checksum + xxHash32(bytes)) >>> 0;
      lease.release();
      peakResident = Math.max(peakResident, cache.stats.residentBytes);
    }
    const e = elapsed(before);
    cpuMs += e.cpuMs;
    wallMs += e.wallMs;
  }
  const exportResult = { cpuMs, wallMs, peakResident, checksum, calls };
  cache.evict(Infinity);
  const prime = await cache.read(refs, { budget });
  prime.release();
  const beforeCalls = calls,
    before = clock();
  for (let r = 0; r < 32; r++) {
    const hit = await cache.read(refs, { budget });
    hit.release();
  }
  const scan = { ...elapsed(before), backendReads: calls - beforeCalls };
  const profileDirectory = process.env.MINNOW_POINT_PROFILE_DIR;
  const finishProfile = profileDirectory ? await pointProfile(profileDirectory) : undefined;
  const pointStart = clock(),
    pointCalls = calls;
  for (let r = 0; r < 16384; r++) {
    const ref = refs[(r * 37) % refs.length];
    if (!ref) throw Error("point ref");
    const hit = await cache.read([ref], { budget });
    checksum ^= hit.bytes[0]?.[0] ?? 0;
    hit.release();
  }
  const point = { ...elapsed(pointStart), backendReads: calls - pointCalls, checksum };
  await finishProfile?.();
  cache.close();
  if (memory.used !== 0 || budget.used !== 0) throw Error("component charge leak");
  return { export: exportResult, warmScan: scan, warmPoint: point };
}
function digest(hash: ReturnType<typeof createHash>, value: unknown): void {
  if (ArrayBuffer.isView(value)) {
    hash.update(value.constructor.name);
    hash.update(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  } else if (value instanceof ArrayBuffer) hash.update(new Uint8Array(value));
  else if (Array.isArray(value)) {
    hash.update("[");
    for (const item of value) digest(hash, item);
    hash.update("]");
  } else if (value !== null && typeof value === "object") {
    for (const key of Object.keys(value).sort()) {
      hash.update(key);
      digest(hash, (value as Record<string, unknown>)[key]);
    }
  } else hash.update(typeof value + ":" + String(value));
}
export async function ordinaryAndExport() {
  const source = new MemoryExtentSpace(),
    spec = wideSpec(200),
    types = spec.map(typeOf),
    writer = new GroupWriter({
      columns: types.map((type, k) => ({ columnId: k + 1, type })),
      writer: source.beginWrite("wire differential"),
    });
  await writer.append(
    spec.map((c, k) => {
      const t = types[k];
      if (!t) throw Error("type");
      return vectorOf(c, t, 0, 4096, 4096);
    }),
    4096,
  );
  const made = await writer.finish();
  // A second group adds NULLs, gzip, a wide int64 and an out-of-line text value.
  const edge = await writeGroup(
    [
      {
        type: { kind: "int64" },
        values: Array.from({ length: 129 }, (_, k) => (1n << 60n) + BigInt(k)),
      },
      {
        type: { kind: "text" },
        values: Array.from({ length: 129 }, (_, k) =>
          k % 3 === 0 ? null : new TextEncoder().encode(k === 1 ? "水".repeat(40000) : `v${k % 5}`),
        ),
      },
    ],
    { gzip: true },
  );
  const cases = [
    { space: source, groups: [{ rows: 4096, chunks: made.columns }] },
    { space: edge.space, groups: [{ rows: 129, chunks: edge.group.columns }] },
  ];
  const results = [];
  for (const test of cases) {
    const governor = new Governor(512 << 20);
    let reads = 0;
    const cache = new PageCache({
      async readDataInto(at: number, out: Uint8Array) {
        reads++;
        out.set(test.space.bytes.subarray(at, at + out.length));
      },
    } as unknown as StoreSession);
    cache.attach(governor.registerCache(cache));
    const pages = {
      read: cache.read.bind(cache),
      peek: cache.peek.bind(cache),
      parsed: cache.parsed.bind(cache),
      attach: cache.attachParsed.bind(cache),
    };
    const channels = new MessageChannel(),
      client = new ProtocolClient(),
      actor = prepareOwnerExport(pages, test.groups, {
        governor,
        port: channels.port1,
        memory: () => ({ held: governor.usedBytes, resident: cache.stats.residentBytes }),
      });
    await client.connect(channels.port2);
    try {
      const hashes = [];
      for (let pass = 0; pass < 2; pass++) {
        const complete = actor.arm(),
          stream = client.exportStream("SELECT * FROM t"),
          hash = createHash("sha256");
        complete.catch(() => undefined);
        for await (const frame of stream.frames) {
          digest(hash, frame.payload);
          frame.release();
        }
        await stream.done;
        await complete;
        hashes.push(hash.digest("hex"));
      }
      const afterExportResident = cache.stats.residentBytes;
      const scan = [];
      for (let pass = 0; pass < 2; pass++) {
        const done = actor.arm(),
          cursor = client.cursor("SELECT * FROM t"),
          dict = new StreamDictionaries();
        done.catch(() => undefined);
        const before = clock(),
          beforeReads = reads;
        let rows = 0;
        for await (const frame of cursor.frames) {
          dict.apply(frame);
          rows += frame.rowCount;
        }
        await done;
        scan.push({
          ...elapsed(before),
          backendReads: reads - beforeReads,
          rows,
          resident: cache.stats.residentBytes,
        });
      }
      results.push({ hashes, afterExportResident, ordinary: scan });
    } finally {
      await actor.close();
      await client.close();
      channels.port1.close();
      channels.port2.close();
      cache.close();
    }
    if (governor.usedBytes !== 0) throw Error("API charge leak");
  }
  return results;
}

/** Actual write path, persisted groups and point-selection scan (keyLookup is not implemented); no fixture catalogue on this guard. */
export async function storedWarmQueries() {
  const { StorageCore } = await import("../../../packages/core/src/next/testing/storage-core.js");
  const core = await new StorageCore(64).open();
  const cases: { scan: unknown[]; point: unknown[] } = { scan: [], point: [] };
  try {
    await core.execute("CREATE TABLE cache_guard (id INTEGER PRIMARY KEY, payload TEXT)");
    await core.writeBatch(
      "insertBatch",
      "cache_guard",
      Array.from({ length: 2048 }, (_, id) => ({
        id,
        payload: `p${String(id % 17)}:${"x".repeat(37)}`,
      })),
    );
    await core.reopen();
    for (const kind of ["scan", "point"] as const)
      for (let pass = 0; pass < 2; pass++) {
        const reads = core.harness.space.stats.cache.platformReads,
          before = clock();
        let count = 0;
        for (let query = 0; query < (kind === "scan" ? 8 : 64); query++) {
          const result = await core.execute(
            kind === "scan"
              ? "SELECT * FROM cache_guard"
              : "SELECT * FROM cache_guard WHERE id + 0 = $1",
            kind === "scan" ? [] : [(query * 31) % 2048],
          );
          if (!("batches" in result)) throw Error("ordinary query did not return rows");
          const n = result.batches.reduce((total, batch) => total + batch.length, 0);
          if (n !== (kind === "scan" ? 2048 : 1)) throw Error("ordinary query values/row count");
          count += n;
        }
        cases[kind].push({
          ...elapsed(before),
          backendReads: core.harness.space.stats.cache.platformReads - reads,
          rows: count,
        });
      }
  } finally {
    await core.close();
  }
  if (core.harness.governor.usedBytes !== 0) throw Error("stored query charge leak");
  return cases;
}
