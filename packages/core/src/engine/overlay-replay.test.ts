/**
 * The streamed replay finds the touched keys of a key-ordered scan block by binary search when
 * the block has far more rows than the deltas name keys. That must be exactly the row-by-row
 * walk: the same slots, the same dead rows, the same patches and older layers, and the same
 * refusal of a duplicate key. These replays run the two paths over the same blocks — a vector
 * that does not declare itself null-free takes the walk — and compare everything they produce.
 */
import { expect, it } from "vitest";
import type { SegmentRecord } from "../storage/types.js";
import { mulberry32 } from "../testing/seeds.js";
import { OverlayPatchCollector, OverlayPatchSources, OverlayReplay } from "./overlay-replay.js";
import type { ColumnVector } from "./vector.js";

function numberVector(values: readonly number[], declareNullFree: boolean): ColumnVector {
  return {
    kind: "number",
    length: values.length,
    values: Float64Array.from(values),
    validity: new Uint8Array(Math.ceil(values.length / 8)).fill(0xff),
    ...(declareNullFree ? { nullCount: 0 } : {}),
  };
}

function segment(id: string, kind: SegmentRecord["kind"], rowCount: number): SegmentRecord {
  return {
    id,
    tableId: "t",
    transactionId: "x",
    rowCount,
    rowIdStart: 0n,
    rowIdEndExclusive: 0n,
    columnBlockIds: { key: [`${id}/key`], value: [`${id}/value`] },
    kind,
    keyColumnId: "key",
    level: 0,
    logicalOrder: 0,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: "2026-01-01T00:00:00.000Z",
  };
}

interface Block {
  kind: SegmentRecord["kind"];
  keys: number[];
  /** Index in the patch sources, or -1. */
  source: number;
  locationStart: number;
}

async function replay(
  collected: readonly number[],
  blocks: readonly Block[],
  sources: OverlayPatchSources,
  baseRows: number,
  search: boolean,
) {
  const dead = new Uint8Array(Math.ceil(baseRows / 8));
  const state = new OverlayReplay({
    numeric: true,
    sources,
    dead,
    partition: 0,
    partitions: 1,
    zonePruned: false,
    tableName: "t",
    charge: () => undefined,
  });
  state.collect(numberVector(collected, true), collected.length);
  let baseSlot = 0;
  let outcome: string | undefined;
  try {
    for (const block of blocks) {
      state.replay(
        block.kind,
        numberVector(block.keys, search),
        block.keys.length,
        baseSlot,
        block.source,
        block.locationStart,
        "segment",
      );
      if (block.kind !== "update" && block.kind !== "delete") baseSlot += block.keys.length;
    }
  } catch (error) {
    outcome = error instanceof Error ? error.message : String(error);
  }
  const collector = new OverlayPatchCollector(Number.MAX_SAFE_INTEGER, () => () => undefined);
  state.emit(collector);
  const patches = await collector.finish(() => undefined);
  return {
    outcome,
    dead: [...dead],
    deadCount: state.deadCount,
    stats: state.patchStats(),
    slots: [...patches.slots],
    locations: [...patches.locations],
    older: [...(patches.older ?? new Map()).entries()].sort((left, right) => left[0] - right[0]),
  };
}

it("searches a key-ordered scan block exactly as it walks one", async () => {
  for (let seed = 1; seed <= 200; seed += 1) {
    const random = mulberry32(seed);
    const baseRows = 2_048 + Math.floor(random() * 4_096);
    // Ordered base keys with gaps, so some touched keys are absent from the block.
    const base: number[] = [];
    let next = Math.floor(random() * 10) - 5;
    for (let row = 0; row < baseRows; row += 1) {
      base.push(next);
      next += 1 + Math.floor(random() * 3);
    }
    const touched = new Set<number>();
    const wanted = 1 + Math.floor(random() * 40);
    while (touched.size < wanted) {
      touched.add(
        random() < 0.8
          ? (base[Math.floor(random() * base.length)] ?? 0)
          : Math.floor(random() * (next + 20)) - 10,
      );
    }
    const collected = [...touched];
    // An ordered upsert block may repeat a key in adjacent rows; a later one replaces it.
    const upsertKeys = [...collected]
      .filter(() => random() < 0.5)
      .flatMap((key) => (random() < 0.3 ? [key, key] : [key]))
      .sort((left, right) => left - right);
    const padding: number[] = [];
    for (let row = 0; row < upsertKeys.length * 40; row += 1) padding.push(next + 1_000 + row);
    const upsert = [...upsertKeys, ...padding].sort((left, right) => left - right);
    const updates = collected.filter(() => random() < 0.6);
    const segments = [
      segment("upsert", "upsert", upsert.length),
      segment("update", "update", updates.length),
    ];
    // Half the cases carry a duplicate in an ordered insert block, which the replay refuses.
    const duplicateInsert = random() < 0.1 && collected.length > 0;
    const insertKeys = duplicateInsert ? [collected[0] ?? 0] : [];
    const blocks: Block[] = [
      { kind: "base", keys: base, source: -1, locationStart: 0 },
      { kind: "upsert", keys: upsert, source: 0, locationStart: 0 },
      { kind: "update", keys: updates, source: 1, locationStart: upsert.length },
      ...(insertKeys.length > 0
        ? [{ kind: "insert" as const, keys: insertKeys, source: -1, locationStart: 0 }]
        : []),
    ];
    const baseRowsTotal = base.length + upsert.length + insertKeys.length;
    const walked = await replay(
      collected,
      blocks,
      new OverlayPatchSources(segments, "key"),
      baseRowsTotal,
      false,
    );
    const searched = await replay(
      collected,
      blocks,
      new OverlayPatchSources(segments, "key"),
      baseRowsTotal,
      true,
    );
    expect(searched, `seed ${String(seed)}`).toEqual(walked);
  }
});
