/** A dictionary-only certificate on the same pinned snapshot, before execution admission.
 * It retains no source values or grants while the complete execution minimum waits.
 */
import type { Snapshot } from "../../access/snapshot.js";
import type { MemoryGovernor } from "../../memory/governor.js";
import type { ReadBound } from "../../page/read-bounds.js";
import type { PhysicalType } from "../../types.js";
import { admitWorkingSet } from "../../memory/working-set.js";
import { readTable } from "../read-table.js";
import type { ChunkReads } from "../scan.js";
import { Slice, runSliced, type SlicedJob } from "../slices.js";
import { readValue } from "../../page/read-bounds.js";
import type { MaybePromise } from "../../access/key-index.js";
import { YIELD_INTERVAL_MS } from "../pipeline.js";
import { TableScan } from "../table-scan.js";
import { OutputArena } from "../kernels/vectors.js";
import { batchTargetFor } from "../../memory/governor.js";
import { SnapshotReleasedError } from "../../access/snapshot.js";
import { GROUP_PROBE_BYTES, GroupPartitionProbe, type PartitionBounds } from "./partition-probe.js";

export interface DictionaryProbeSource {
  readonly tableId: number;
  readonly keys: readonly number[];
  readonly types: readonly PhysicalType[];
  readonly payloads: readonly number[];
}
export function probeDictionaryGroups(
  view: Snapshot,
  source: DictionaryProbeSource,
  reads: ChunkReads,
  governor: MemoryGovernor,
  read: ReadBound,
  signal: AbortSignal,
  label = "aggregate-domain-planning",
  maxGroups = 1024,
): MaybePromise<PartitionBounds | null> {
  const slice = new Slice(YIELD_INTERVAL_MS);
  return runSliced(certify(), slice, () => undefined);
  function* certify(): SlicedJob<PartitionBounds | null> {
    signal.throwIfAborted();
    const readerBytes = read.bytes + read.io + read.progress;
    if (GROUP_PROBE_BYTES + readerBytes > governor.capBytes) return null;
    const parts = yield* readValue(
      admitWorkingSet(governor, {
        label,
        includesProgress: true,
        signal,
        grants: [
          { label: "domains", minimumBytes: GROUP_PROBE_BYTES, spillable: false },
          { label: "reader", minimumBytes: readerBytes, spillable: false },
        ],
      }),
    );
    const domains = parts[0],
      budget = parts[1];
    if (!domains || !budget) throw new Error("Missing dictionary planning minimum");
    let probe: GroupPartitionProbe | null = null;
    const check = () => {
      signal.throwIfAborted();
      if (view.released) throw new SnapshotReleasedError();
    };
    try {
      check();
      probe = new GroupPartitionProbe(source.types, source.payloads.length, domains, maxGroups, 1);
      const table = readTable(
        yield* readValue(view.table(source.tableId, { budget })),
        view.pages,
        budget,
        check,
      );
      probe.sourceRows(table.liveRowCount);
      if (
        table.segments.some((segment) => segment.kind === "memory" && segment.liveRowCount !== 0)
      ) {
        // The pinned rows can repoint at publication. Reuse the public bounded scan contract,
        // with only key/payload columns, instead of borrowing an unbounded legacy memory chunk.
        const columns = [...new Set([...source.keys, ...source.payloads])];
        const input = columns.map((id) => {
          const type = table.columns.find((column) => column.id === id)?.type;
          if (!type) throw new Error("Missing memory domain column");
          return type;
        });
        const arena = new OutputArena(budget, 2048);
        let scan: TableScan | null = null;
        try {
          scan = new TableScan({
            table,
            pages: view.pages,
            reads,
            budget,
            columns,
            filter: null,
            emitLocator: false,
            context: { arena, input, params: { slots: [] } },
            targetBytes: batchTargetFor(governor.capBytes, 4),
            boundedMemory: true,
            loans: {
              borrow: () =>
                Promise.reject(new Error("Domain probe exceeded its admitted read bound")),
            },
          });
          for (;;) {
            check();
            const item = yield* readValue(scan.next());
            if (item === null) break;
            try {
              const keys = source.keys.map((id) => {
                const vector = item.batch.columns[columns.indexOf(id)];
                if (!vector) throw new Error("Missing memory domain key");
                return vector;
              });
              const payloads = source.payloads.map((id) => {
                const vector = item.batch.columns[columns.indexOf(id)];
                if (!vector) throw new Error("Missing memory domain payload");
                return vector;
              });
              if (!probe.offer(keys, payloads, item.batch.length, item.batch.selection, true))
                return null;
            } finally {
              item.transfer?.release();
            }
            if (slice.due()) {
              yield;
              check();
            }
          }
          return probe.seal();
        } finally {
          scan?.abort();
          arena.close();
        }
      }
      for (const segment of table.segments) {
        check();
        if (segment.liveRowCount === 0) continue;
        // Memory rows were handled through the publication-stable bounded scan above.
        if (segment.kind !== "group") return null;
        try {
          yield* readValue(segment.root());
          for (let k = 0; k < source.keys.length + source.payloads.length; k++) {
            check();
            const key = k < source.keys.length,
              index = key ? k : k - source.keys.length;
            const column = key ? source.keys[index] : source.payloads[index];
            if (column === undefined) throw new Error("Missing dictionary planning column");
            const chunk = segment.chunk(column);
            if (chunk === null) {
              const missing = table.columns.find((c) => c.id === column)?.missing;
              if (missing == null) {
                if (key && !probe.offerDomain(index, null, true)) return null;
              } else if (
                key
                  ? !probe.offerDomain(index, missing, false)
                  : !probe.offerPayload(index, missing)
              )
                return null;
              continue;
            }
            const nullable = chunk.statistics.nullCount > 0;
            if (chunk.statistics.nullCount === segment.rowCount) {
              if (key && !probe.offerDomain(index, null, true)) return null;
              continue;
            }
            const pages = yield* readValue(
              reads.open(view.pages, chunk, {
                budget,
                signal,
                once: true,
                task: { clock: slice.clock, signal },
              }),
            );
            try {
              if (!pages.isDictionaryRange(0, pages.rowCount)) return null;
              const dictionary = yield* readValue(pages.loadDictionary());
              if (dictionary === null) return null;
              if (
                key
                  ? !probe.offerDomain(index, dictionary, nullable)
                  : !probe.offerPayload(index, dictionary)
              )
                return null;
            } finally {
              pages.close();
            }
            if (slice.due()) {
              yield;
              check();
            }
          }
        } finally {
          segment.releaseRoot?.();
        }
      }
      return probe.seal();
    } finally {
      probe?.close();
      for (const part of parts) part.close();
    }
  }
}
