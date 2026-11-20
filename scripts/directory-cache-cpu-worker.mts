/** Native immutable-directory sizing component. Setup and complete row verification are
 * outside the process CPU span. The span includes every sizing grant and reader close;
 * it does not execute a SELECT, deliver frames, or measure browser storage.
 */
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
import { queryLifetimes } from "./lib/query-root-lifetimes.mts";
import type { ReadBound, ReadSource } from "../packages/core/dist/next/page/read-bounds.js";

export interface DirectoryComponentConfig {
  runtimeRoot: string;
  rows: number;
  columns: number;
  warmups: number;
  repeats: number;
}
export default async function createDirectoryComponent(
  context: CPUWorkerContext,
): Promise<CPUWorkerHarness> {
  const config = context.data as DirectoryComponentConfig;
  if (
    ![config.rows, config.columns, config.warmups, config.repeats].every(
      (value) => Number.isSafeInteger(value) && value > 0,
    ) ||
    config.columns > 200 ||
    config.rows > 1_000_000
  )
    throw new Error("Invalid native directory component policy");
  const load = (name: string) =>
    import(pathToFileURL(join(config.runtimeRoot, "packages/core/dist/next", name + ".js")).href);
  const { StorageCore } = (await load(
      "testing/storage-core",
    )) as typeof import("../packages/core/dist/next/testing/storage-core.js"),
    { oracleRows } = (await load(
      "testing/differential-adapter",
    )) as typeof import("../packages/core/dist/next/testing/differential-adapter.js"),
    { boundSource } = (await load(
      "page/read-bounds",
    )) as typeof import("../packages/core/dist/next/page/read-bounds.js"),
    { metadataCachesFor } = (await load(
      "format/metadata-reader",
    )) as typeof import("../packages/core/dist/next/format/metadata-reader.js"),
    { memoryScanBufferBytes } = (await load(
      "exec/table-scan",
    )) as typeof import("../packages/core/dist/next/exec/table-scan.js");
  const core = await new StorageCore(4096).open();
  try {
    const names = Array.from({ length: config.columns }, (_, at) => `c${String(at)}`),
      input = Array.from({ length: config.rows }, (_, id) =>
        Object.fromEntries(
          names.map((name, column) => [
            name,
            column === 0
              ? id
              : (id + column) % 17 === 0
                ? null
                : Math.imul(id + 1, 0x9e37_79b1 ^ column) % 1_000_003,
          ]),
        ),
      );
    await core.execute(
      `CREATE TABLE directory_component (${names
        .map((name, column) => `${name} INTEGER${column === 0 ? " PRIMARY KEY" : ""}`)
        .join(",")})`,
    );
    await core.writeBatch("insertBatch", "directory_component", input);
    await core.harness.path.recycleLog();
    await core.harness.path.quiesce();
    const table = core.harness.path.catalog.tableByName("directory_component");
    if (!table) throw new Error("Missing native directory component table");
    const types = table.columns.map((column) => column.type),
      source: ReadSource = {
        id: 1,
        tableId: table.id,
        columns: table.columns.map((column) => column.id),
        types,
        windowBytes: 0,
        stagingBytes: memoryScanBufferBytes(types),
        rows: null,
        ...(context.workload === "directory_size_once" ? { retain: false } : {}),
      };
    const fingerprint = createHash("sha256").update(JSON.stringify(input)).digest("hex");
    let expectedBound: ReadBound | undefined,
      warmed = false;
    async function once() {
      context.signal.throwIfAborted();
      const pin = core.harness.path.pin();
      try {
        return await boundSource(pin, source, core.harness.governor, context.signal, 8192);
      } finally {
        pin.release();
      }
    }
    return {
      revision: context.revision,
      identity: {
        runtimeRoot: config.runtimeRoot,
        rows: config.rows,
        columns: config.columns,
        warmups: config.warmups,
        repeats: config.repeats,
        retain: source.retain !== false,
        ownerDirectoryCache: metadataCachesFor(core.harness.space)?.directories !== undefined,
        storage: "Node MemoryBackend, native flushed immutable groups",
        boundary: "boundSource, fresh snapshot and all sizing reader/grant cleanup",
      },
      workloads: ["directory_size_retained", "directory_size_once"],
      boundaries: ["sizing"],
      async verify() {
        // Verify complete persisted rows, not a LIMIT, count or unconsumed stream.
        const result = await core.execute("SELECT * FROM directory_component ORDER BY c0");
        if (!("batches" in result) || !isDeepStrictEqual(oracleRows(result), input))
          throw new Error("Native directory component row mismatch");
        // Verify the once control's rows too, then remove the directory entries
        // seeded by that SELECT, outside the clock. Both arms start sizing cold.
        if (source.retain === false) metadataCachesFor(core.harness.space)?.directories?.clear();
        const bound = await once();
        if (expectedBound !== undefined && !isDeepStrictEqual(bound, expectedBound))
          throw new Error("Native directory component bound changed");
        if (
          !Number.isSafeInteger(bound.bytes) ||
          bound.bytes < 0 ||
          bound.bytes > core.harness.governor.capBytes
        )
          throw new Error("Native directory component exceeds its admitted byte cap");
        expectedBound = bound;
        if (!warmed) {
          for (let warm = 0; warm < config.warmups; warm++)
            if (!isDeepStrictEqual(await once(), bound)) throw new Error("Sizing warmup drift");
          warmed = true;
        }
        return {
          fingerprint: createHash("sha256")
            .update(fingerprint)
            // Resource credit now includes owned HLL/statistic copies. Record it
            // per sample; compare all other bound fields and complete rows across arms.
            .update(JSON.stringify({ ...bound, bytes: undefined }))
            .digest("hex"),
          rows: config.rows * config.repeats,
        };
      },
      async run() {
        if (!warmed) throw new Error("Unverified native directory component");
        const cache = metadataCachesFor(core.harness.space)?.directories,
          hits = cache?.hits ?? 0,
          misses = cache?.misses ?? 0;
        const result = await queryLifetimes(config.repeats, config.rows, async () => {
          await once();
          return { rows: config.rows };
        });
        return {
          ...result,
          detail: {
            ...(result.detail as Record<string, unknown>),
            bound: expectedBound,
            cache:
              cache === undefined
                ? null
                : {
                    hits: cache.hits - hits,
                    misses: cache.misses - misses,
                    entries: cache.entryCount,
                    residentBytes: cache.residentBytes,
                    evictableBytes: cache.evictableBytes,
                  },
          },
        };
      },
      async close() {
        await core.close();
        if (core.harness.governor.usedBytes !== 0 || core.spill.bytesInUse !== 0)
          throw new Error("Native directory component retained credit after close");
      },
    };
  } catch (error) {
    await core.close();
    throw error;
  }
}
