/** A timing fixture must preserve exact files, independently of the measured executor. */
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { MemoryBackend } from "../packages/core/dist/next/storage/memory-store.js";
import type { OwnerLease, SyncFile } from "../packages/core/dist/next/storage/contract.js";
import { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import { oracleRows } from "../packages/core/dist/next/testing/differential-adapter.js";
import { seedsFor, mulberry32 } from "../packages/core/src/testing/seeds.js";
import {
  createReach,
  NATIVE_BENCH_IMAGE_IO_REACH,
  NATIVE_BENCH_IMAGE_SQL_REACH,
} from "../packages/core/src/testing/reach.js";
import {
  captureNativeImage,
  captureNativeLease,
  restoreNativeImage,
  monitorNativeCapacity,
} from "./lib/native-storage-image.mts";

const hash = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
function slots(lease: OwnerLease): Record<string, SyncFile> {
  const raw = lease as OwnerLease & {
    files: {
      log: SyncFile;
      data: SyncFile;
      roots: [SyncFile, SyncFile];
      metadata: SyncFile;
      metadataIndex: SyncFile;
    };
  };
  return {
    log: raw.files.log,
    data: raw.files.data,
    root0: raw.files.roots[0],
    root1: raw.files.roots[1],
    metadata: raw.files.metadata,
    "metadata-index": raw.files.metadataIndex,
  };
}
for (const seed of seedsFor("native-bench-image-io", [0xb7c0, 0xb7c1]))
  it(`preserves image bytes and drains failed copies (${String(seed)})`, async () => {
    const reach = createReach(`native image IO ${String(seed)}`, NATIVE_BENCH_IMAGE_IO_REACH);
    reach.beginHistory();
    const directory = await mkdtemp(join(tmpdir(), "minnow-native-image-"));
    const source = new MemoryBackend(),
      restored = new MemoryBackend(),
      random = mulberry32(seed);
    const sourceCapacity = monitorNativeCapacity(source),
      targetCapacity = monitorNativeCapacity(restored);
    const expected = new Map<string, Uint8Array>();
    let released = 0;
    const acquire = restored.acquire.bind(restored);
    restored.acquire = async () => {
      const lease = await acquire(),
        release = lease.release.bind(lease);
      lease.release = async () => {
        released++;
        return release();
      };
      return lease;
    };
    try {
      const lease = await source.acquire();
      try {
        let i = 0;
        for (const [name, file] of Object.entries(slots(lease))) {
          const size = [0, 1, (1 << 20) - 1, 1 << 20, (1 << 20) + 17, 32][i++];
          if (size === undefined) throw new Error("missing image size");
          reach.reach(size === 0 ? "empty file" : size > 1 << 20 ? "chunk tail" : "one chunk");
          const bytes = Uint8Array.from({ length: size }, () => Math.floor(random() * 256));
          file.write(bytes, { at: 0 });
          if (name === "data") {
            file.truncate(2 << 20);
            file.truncate(size);
          }
          expected.set(name, bytes);
        }
        const capacities = sourceCapacity.stop();
        await captureNativeLease(lease, join(directory, "live"), "fixture", undefined, capacities);
        expect(lease.signal.aborted).toBe(false);
        reach.reach("borrowed owner live");
      } finally {
        await lease.release();
      }
      const image = join(directory, "image");
      const capacities = sourceCapacity.stop();
      await captureNativeImage(source, image, "fixture", undefined, capacities);
      for (const [name, bytes] of expected)
        expect(hash(await readFile(join(image, name + ".bin")))).toBe(hash(bytes));
      await restoreNativeImage(restored, image, "fixture");
      expect(targetCapacity.capacities).toEqual(capacities);
      reach.reach("capacity retained");
      expect(released).toBe(1);
      const verify = await restored.acquire();
      try {
        for (const [name, file] of Object.entries(slots(verify))) {
          const bytes = new Uint8Array(file.getSize());
          file.read(bytes, { at: 0 });
          const want = expected.get(name);
          if (want === undefined) throw new Error("missing source");
          expect(hash(bytes), name).toBe(hash(want));
        }
      } finally {
        await verify.release();
      }
      reach.reach("exact restore");
      await expect(captureNativeImage(source, image, "fixture")).rejects.toMatchObject({
        code: "EEXIST",
      });
      reach.reach("immutable image");
      const original = await readFile(join(image, "data.bin"));
      await writeFile(join(image, "data.bin"), Uint8Array.of((original[0] ?? 0) ^ 255));
      const before = released;
      await expect(restoreNativeImage(restored, image, "fixture")).rejects.toThrow("hash mismatch");
      expect(released).toBe(before + 1);
      reach.reach("corrupt refusal");
      await writeFile(join(image, "data.bin"), original);
      await restoreNativeImage(restored, image, "fixture");
      reach.reach("retry");
      const controller = new AbortController(),
        check = controller.signal.throwIfAborted.bind(controller.signal);
      let checks = 0;
      controller.signal.throwIfAborted = () => {
        if (++checks === 4) controller.abort(new Error("image cancel"));
        check();
      };
      await expect(
        captureNativeImage(source, join(directory, "cancelled"), "fixture", controller.signal),
      ).rejects.toThrow("image cancel");
      await expect(readFile(join(directory, "cancelled", "manifest.json"))).rejects.toMatchObject({
        code: "ENOENT",
      });
      const after = await source.acquire();
      await after.release();
      reach.reach("cancel cleanup");
      reach.assertReached();
    } finally {
      sourceCapacity.stop();
      targetCapacity.stop();
      await rm(directory, { recursive: true, force: true });
    }
  });

for (const seed of seedsFor("native-bench-image-sql", [0xb7c2, 0xb7c3]))
  it(`reopens actual encoded columns against PostgreSQL (${String(seed)})`, async () => {
    const reach = createReach(`native image SQL ${String(seed)}`, NATIVE_BENCH_IMAGE_SQL_REACH);
    reach.beginHistory();
    const directory = await mkdtemp(join(tmpdir(), "minnow-native-sql-image-"));
    const first = new StorageCore(4),
      second = new StorageCore(4),
      pg = new PGlite();
    const random = mulberry32(seed);
    const monitor = monitorNativeCapacity(first.harness.backend);
    let firstOpen = false,
      secondOpen = false;
    try {
      await first.open();
      firstOpen = true;
      await pg.waitReady;
      const ddl = "CREATE TABLE w(id INTEGER PRIMARY KEY,p TEXT,x DOUBLE PRECISION)";
      await first.execute(ddl);
      await pg.exec(ddl);
      const rows = Array.from({ length: 35 }, (_, id) => ({
        id,
        p: id % 7 === 0 ? null : (["a", "é", "😀"][id % 3] ?? "a"),
        x: id % 5 === 0 ? -0 : Math.floor(random() * 100) / 8,
      }));
      await first.writeBatch("insertBatch", "w", rows);
      for (const row of rows)
        await pg.query("INSERT INTO w VALUES($1,$2,$3::float8)", [
          row.id,
          row.p,
          Object.is(row.x, -0) ? "-0" : String(row.x),
        ]);
      const query = "SELECT id,p,x FROM w ORDER BY id";
      const expected = (await pg.query(query)).rows;
      await first.harness.path.quiesce();
      const capacities = monitor.stop();
      await captureNativeLease(
        monitor.lease,
        join(directory, "image"),
        "fixture",
        undefined,
        capacities,
      );
      expect(monitor.lease.signal.aborted).toBe(false);
      await restoreNativeImage(second.harness.backend, join(directory, "image"), "fixture");
      await second.open();
      secondOpen = true;
      const result = await second.execute(query);
      if (!("batches" in result)) throw new Error("expected query");
      expect(oracleRows(result)).toEqual(expected);
      for (const name of [
        "NULL validity",
        "dictionary text",
        "UTF8",
        "signed zero",
        "ordinary reopen",
      ] as const)
        reach.reach(name);
      await second.writeBatch("insertBatch", "w", [{ id: 35, p: "later", x: 1.5 }]);
      await pg.exec("INSERT INTO w VALUES(35,'later',1.5)");
      await second.harness.reopen();
      const later = await second.execute(query);
      if (!("batches" in later)) throw new Error("expected query");
      expect(oracleRows(later)).toEqual((await pg.query(query)).rows);
      reach.reach("continued writes");
      reach.assertReached();
    } finally {
      monitor.stop();
      if (firstOpen) await first.close();
      if (secondOpen) await second.close();
      await pg.close();
      expect(first.harness.governor.usedBytes).toBe(0);
      expect(second.harness.governor.usedBytes).toBe(0);
      await rm(directory, { recursive: true, force: true });
    }
  }, 30000);
