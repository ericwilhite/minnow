/**
 * The resident group directory (groups.ts; ruling on review B3-p2a-3 findings 1, 2, 6): every
 * field round-trips through the typed columns (any address, checksum, null or present deletion
 * bitmap), a generation shares every chunk it does not change, every byte is charged before it
 * is allocated and returned by the last release, and the lookups agree with a plain array.
 */
import { describe, expect, it } from "vitest";
import type { GroupRef } from "../format/manifest.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { need } from "./bytes.js";
import {
  CHUNK_GROUPS,
  chunkBufferBytes,
  chunkCharge,
  directoryObjectBytes,
  GroupDirectory,
} from "./groups.js";

class Meter {
  allocated = 0;
  constructor(readonly cap = Infinity) {}
  allocate(bytes: number): boolean {
    if (this.allocated + bytes > this.cap) return false;
    this.allocated += bytes;
    return true;
  }
  free(bytes: number): void {
    this.allocated -= bytes;
    if (this.allocated < 0) throw new Error("freed more than allocated");
  }
}

function group(id: number, random: () => number, huge = false): GroupRef {
  const base = huge ? 2 ** 40 : 0;
  const rows = 1 + Math.floor(random() * 65_536);
  return {
    groupId: id,
    firstRowId: id * 70_000,
    lastRowId: id * 70_000 + rows - 1,
    rowCount: rows,
    liveRowCount: Math.floor(random() * (rows + 1)),
    root: {
      extent: { offset: base + id * 8192, length: 4096 + Math.floor(random() * 1e6) },
      offset: Math.floor(random() * 4000),
      length: 1 + Math.floor(random() * 1e6),
      checksum: random() < 0.2 ? 0xffffffff : Math.floor(random() * 2 ** 32),
    },
    deletions:
      random() < 0.5
        ? null
        : {
            extent: { offset: base + id * 8192 + 4096, length: 4096 },
            offset: Math.floor(random() * 4000),
            length: 4 + Math.floor(random() * 8192),
            checksum: Math.floor(random() * 2 ** 32),
          },
  };
}

describe("group directory", () => {
  it("charges each chunk's buffer and object exactly, before allocating", () => {
    expect(chunkCharge(CHUNK_GROUPS)).toBe(chunkBufferBytes(CHUNK_GROUPS) + 640);
    // 97 bytes of columns per group (nine f64, six u32, one flag byte), however large the values.
    expect(chunkBufferBytes(CHUNK_GROUPS) / CHUNK_GROUPS).toBe(97);
    const random = mulberry32(1);
    const groups = Array.from({ length: 2500 }, (_, k) => group(k + 1, random, true));
    const meter = new Meter();
    const directory = GroupDirectory.build(groups, meter);
    expect(meter.allocated).toBe(GroupDirectory.buildCharge(2500));
    expect(meter.allocated).toBe(
      2 * chunkCharge(CHUNK_GROUPS) + chunkCharge(452) + directoryObjectBytes(3),
    );
    expect(directory.chargedBytes).toBe(meter.allocated);
    // A refusal leaves nothing charged, whichever chunk it hits.
    for (const cap of [10, chunkCharge(CHUNK_GROUPS) + 5, meter.allocated - 1]) {
      const tight = new Meter(cap);
      expect(() => GroupDirectory.build(groups, tight)).toThrow(ParseBudgetError);
      expect(tight.allocated).toBe(0);
    }
    directory.releaseCreation();
    expect(meter.allocated).toBe(0);
  });

  it("an append copies only the tail chunk; a change copies only its chunk", () => {
    const random = mulberry32(2);
    const groups = Array.from({ length: 3000 }, (_, k) => group(k + 1, random));
    const meter = new Meter();
    const first = GroupDirectory.build(groups, meter);
    first.retain(); // a generation holds it
    first.releaseCreation();
    const charged = meter.allocated;
    const appended = first.with(new Map(), [group(3001, random)], meter);
    expect(appended.chunks.slice(0, 2)).toEqual(first.chunks.slice(0, 2));
    expect(appended.chunks[0]).toBe(first.chunks[0]);
    expect(appended.chunks[1]).toBe(first.chunks[1]);
    expect(appended.chunks[2]).not.toBe(first.chunks[2]);
    expect(meter.allocated - charged).toBe(chunkCharge(953) + directoryObjectBytes(3));
    const changed = appended.with(
      new Map([[5, { ...groups[5], liveRowCount: 0 } as GroupRef]]),
      [],
      meter,
    );
    expect(changed.chunks[0]).not.toBe(appended.chunks[0]);
    expect(changed.chunks[1]).toBe(appended.chunks[1]);
    expect(changed.chunks[2]).toBe(appended.chunks[2]);
    expect(changed.liveRows(5)).toBe(0);
    expect(changed.liveRowCount).toBe(appended.liveRowCount - (groups[5]?.liveRowCount ?? 0));
    // Ownership: generations retain, makers drop creation references; the last release frees.
    appended.retain();
    appended.releaseCreation();
    changed.retain();
    changed.releaseCreation();
    first.release();
    appended.release();
    expect(meter.allocated).toBe(changed.chargedBytes);
    changed.release();
    expect(meter.allocated).toBe(0);
  });

  const SEEDS = seedsFor("next-write-group-directory", [11, 12, 13, 14]);
  for (const seed of SEEDS)
    it(`agrees with an array model over generated generations (seed ${String(seed)})`, () => {
      const random = mulberry32(seed);
      const meter = new Meter();
      let model: GroupRef[] = [];
      let directory = GroupDirectory.EMPTY;
      const live: GroupDirectory[] = [];
      let nextId = 1;
      for (let step = 0; step < 60; step++) {
        const changes = new Map<number, GroupRef>();
        if (model.length > 0)
          for (let k = Math.floor(random() * 4); k > 0; k--) {
            const at = Math.floor(random() * model.length);
            changes.set(at, {
              ...group(model[at]?.groupId ?? 0, random),
              groupId: model[at]?.groupId ?? 0,
            });
          }
        const count = random() < 0.1 ? Math.floor(random() * 2500) : Math.floor(random() * 4);
        const appended = Array.from({ length: count }, () =>
          group(nextId++, random, random() < 0.3),
        );
        const next = directory.with(changes, appended, meter);
        next.retain();
        next.releaseCreation();
        model = model.map((existing, at) => changes.get(at) ?? existing).concat(appended);
        live.push(next);
        // Older generations are dropped in random order (the newest is the current one).
        while (live.length > 3)
          live.splice(Math.floor(random() * (live.length - 1)), 1)[0]?.release();
        directory = next;
        expect(directory.count).toBe(model.length);
        expect(directory.liveRowCount).toBe(model.reduce((sum, g) => sum + g.liveRowCount, 0));
        for (let probe = 0; probe < 20 && model.length > 0; probe++) {
          const at = Math.floor(random() * model.length);
          const expected = need(model[at]);
          expect(directory.get(at)).toEqual(expected);
          expect(directory.indexOfGroupId(expected.groupId)).toBe(at);
          const row = expected.firstRowId + Math.floor(random() * expected.rowCount);
          expect(directory.indexOfRow(row)).toBe(at);
          expect(directory.indexOfRow(expected.lastRowId + 1)).toBe(-1);
        }
        expect(directory.slice(Math.max(0, model.length - 5))).toEqual(
          model.slice(Math.max(0, model.length - 5)),
        );
      }
      for (const generation of live) generation.release();
      expect(meter.allocated).toBe(0);
    });
});
