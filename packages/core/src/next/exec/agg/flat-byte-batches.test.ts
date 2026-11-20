/** SQL grouping is byte equality; a batch's exact source bounds fund its complete growth.
 * The flat route grows storage on each new key exactly as the per-row route does: the hybrid
 * operator keeps the ordinary growth policy because a different one changes later batch peaks
 * (`hybrid.ts`, `parent-peak.test.ts`). A twin table fed the same rows through the per-row
 * route must hold the same bytes and assign the same IDs after every batch. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, FLAT_BYTE_BATCH_REACH } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { Governor } from "../../memory/governor.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { FlatVector } from "../../vector/vector.js";
import { TEXT, dictionary, texts, valueAt } from "../kernels/test-support.js";
import { GroupTable } from "./groups.js";
import { HashWorkspace } from "./hash-workspace.js";

/** Constructor bytes are independent of allocate requests, including replacement overlap.
 * A free retires the corresponding logical backing; no task can observe the old view then. */
function buffers(workspace: HashWorkspace) {
  let actual = 0,
    funded = 0,
    active = false,
    constructions = 0;
  const restore: Array<() => void> = [];
  for (const name of [
    "Uint8Array",
    "Uint16Array",
    "Uint32Array",
    "Int32Array",
    "Float64Array",
  ] as const) {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, name);
    if (!descriptor || typeof descriptor.value !== "function") throw Error("Missing constructor");
    const original = descriptor.value as new (...args: unknown[]) => ArrayBufferView;
    const proxy = new Proxy(original, {
      construct(target, args, newTarget) {
        const result = Reflect.construct(target, args, newTarget) as ArrayBufferView;
        if (active && args[0] !== result.buffer) {
          const next = actual + result.buffer.byteLength;
          if (next > funded)
            throw Error(
              `Uncharged grouping backing: actual=${String(next)}, preceding=${String(funded)}`,
            );
          if (next > workspace.admitted) throw Error("Grouping backing exceeds complete admission");
          actual = next;
          constructions++;
        }
        return result;
      },
    });
    Object.defineProperty(globalThis, name, { ...descriptor, value: proxy });
    restore.push(() => Object.defineProperty(globalThis, name, descriptor));
  }
  const charge: ByteCharge = {
    allocate(bytes) {
      const ok = workspace.allocate(bytes);
      if (ok) funded += bytes;
      return ok;
    },
    free(bytes) {
      workspace.free(bytes);
      funded -= bytes;
      actual -= bytes;
      if (actual < 0) throw Error("Grouping freed unseen backing");
    },
  };
  return {
    charge,
    call<T>(body: () => T): T {
      active = true;
      try {
        return body();
      } finally {
        active = false;
      }
    },
    check() {
      expect(actual).toBe(funded);
      expect(constructions).toBeGreaterThan(5);
    },
    close() {
      for (const f of restore.reverse()) f();
      expect(actual).toBe(0);
      expect(funded).toBe(0);
    },
  };
}

for (const seed of seedsFor("flat-byte-batches", [0xb9fc1, 0xb9fc2]))
  it(`keeps exact IDs and independently funded buffers at batch boundaries seed${String(seed)}`, async () => {
    const reach = createReach("flat byte batches", FLAT_BYTE_BATCH_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          count: fc.integer({ min: 17000, max: 19000 }),
          width: fc.constantFrom(15, 16, 36, 64, 65, 80),
          selected: fc.boolean(),
          duplicate: fc.boolean(),
          offset: fc.integer({ min: 0, max: 15 }),
        }),
        async ({ count, width, selected, duplicate, offset }) => {
          reach.beginHistory();
          // The per-row growth policy peaks near 10.4 MB on 17,000 unique 65-byte keys
          // (4x arena steps from LARGE_ARRAY); the budget covers it with a pinned competitor.
          const governor = new Governor(16 << 20);
          const pinned = await governor.admit({
            label: "pinned grouping competitor",
            minimumBytes: 512 << 10,
            spillable: false,
          });
          expect(pinned.allocate(pinned.held)).toBe(true);
          const grant = await governor.admit({
            label: "byte groups",
            minimumBytes: 64 << 10,
            spillable: true,
          });
          const workspace = new HashWorkspace(grant, grant.held);
          const audit = buffers(workspace);
          const table = audit.call(() => new GroupTable([TEXT], audit.charge));
          const gids = new Int32Array(2048),
            model = new Map<string | null, number>(),
            first: Array<string | null> = [];
          // The per-row twin: an all-valid bitmap keeps the same bytes off the flat route.
          const twinGovernor = new Governor(64 << 20);
          const twinGrant = await twinGovernor.admit({
            label: "per-row twin",
            minimumBytes: 32 << 20,
            spillable: false,
          });
          const twinWorkspace = new HashWorkspace(twinGrant, twinGrant.held);
          const twin = new GroupTable([TEXT], twinWorkspace);
          const twinIds = new Int32Array(2048);
          const perRow = (vector: FlatVector): FlatVector => ({
            ...vector,
            validity: new Uint32Array((vector.length + 31) >>> 5).fill(0xffffffff),
          });
          const check = (keys: Array<string | null>, vector: FlatVector, flat: boolean) => {
            const selectedCount = keys.length > 2 ? Math.ceil(keys.length / 2) : keys.length;
            const rows = selected
              ? {
                  rows: Uint16Array.from(
                    { length: selectedCount },
                    (_, i) => keys.length - 1 - i * (keys.length > 2 ? 2 : 1),
                  ),
                  count: selectedCount,
                }
              : null;
            const result = workspace.prepare(
              workspace.used + table.growthBytes([vector], keys.length),
            );
            return Promise.resolve(result).then((ok) => {
              expect(ok).toBe(true);
              audit.call(() => table.assign([vector], keys.length, rows, gids));
              twin.assign([flat ? perRow(vector) : vector], keys.length, rows, twinIds);
              expect(workspace.used, "flat and per-row routes grow alike").toBe(twinWorkspace.used);
              for (let i = 0; i < (rows?.count ?? keys.length); i++) {
                const row = rows?.rows[i] ?? i,
                  key = keys[row] ?? null;
                let id = model.get(key);
                if (id === undefined) {
                  id = model.size;
                  model.set(key, id);
                  first.push(key);
                }
                expect(gids[row]).toBe(id);
                expect(twinIds[row]).toBe(id);
                if (i < 4 || i + 1 === (rows?.count ?? keys.length))
                  expect(
                    audit.call(() =>
                      table.lookup([vector], row, () => {
                        throw Error("Byte lookup must not encode");
                      }),
                    ),
                  ).toBe(id);
              }
              audit.check();
            });
          };
          try {
            for (let from = 0; from < count; from += 2048) {
              const keys = Array.from({ length: Math.min(2048, count - from) }, (_, i) =>
                String(duplicate ? (from + i) % 3000 : from + i).padStart(width, "x"),
              );
              const original = texts(keys);
              if (original.data.layout !== "varlen") throw Error("Expected bytes");
              const prefix = offset % 5;
              const bytes = new Uint8Array(original.data.bytes.length + offset + prefix + 3);
              bytes.set(original.data.bytes, offset + prefix);
              const vector: FlatVector = {
                ...original,
                data: {
                  ...original.data,
                  bytes: bytes.subarray(offset, offset + prefix + original.data.bytes.length),
                  offsets: original.data.offsets.map((value) => value + prefix),
                },
              };
              await check(keys, vector, true);
            }
            await check([], texts([]), true);
            if (selected) reach.reach("partial selection");
            // Re-enter the ordinary NULL path and a different dictionary, then flat again.
            await check([null, first[0] ?? ""], texts([null, first[0] ?? ""]), false);
            const entries = texts(["dictionary-only", first[1] ?? ""]);
            const encoded = dictionary(entries, [0, 1]);
            expect(await workspace.prepare(workspace.used + table.growthBytes([encoded], 2))).toBe(
              true,
            );
            audit.call(() => table.assign([encoded], 2, null, gids));
            twin.assign([encoded], 2, null, twinIds);
            expect(workspace.used).toBe(twinWorkspace.used);
            for (const [row, key] of ["dictionary-only", first[1] ?? ""].entries()) {
              let id = model.get(key);
              if (id === undefined) {
                id = model.size;
                model.set(key, id);
                first.push(key);
              }
              expect(gids[row]).toBe(id);
            }
            // Distinct 36-byte values with the same XXH32; equality still compares bytes.
            const collision = [
              "42a88c93815ba8d6d041ca3dc64f86781fc4",
              "d40f3fa74be0b8daadc9327126c8891c7efb",
            ];
            await check(collision, texts(collision), true);
            await check([...collision].reverse(), texts([...collision].reverse()), true);
            reach.reach("hash collision");
            await check(
              ["after-dictionary", first[0] ?? ""],
              texts(["after-dictionary", first[0] ?? ""]),
              true,
            );
            expect(table.groupCount).toBe(model.size);
            for (let from = 0; from < first.length; from += 2048) {
              const length = Math.min(2048, first.length - from),
                out = table.emitKey(0, from, length);
              for (let i = 0; i < length; i++) expect(valueAt(out, i)).toBe(first[from + i]);
            }
            expect(governor.peakBytes).toBeLessThanOrEqual(16 << 20);
            reach.reach("pinned competitor");
            reach.reach("second batch");
            reach.reach("NULL then flat");
            reach.reach("dictionary then flat");
            reach.reach("independent backing");
            if (selected) reach.reach("reversed selection");
            if (duplicate) reach.reach("duplicates");
            else reach.reach("unique growth");
            if (offset > 0) reach.reach("unaligned view");
            if (offset % 5 > 0) reach.reach("nonzero offset base");
            reach.reach("ordinary frozen lookup");
            if (width === 36) reach.reach("UUID36");
            if (width === 15 || width === 65) reach.reach("hash boundary");
          } finally {
            twin.close();
            twinWorkspace.close();
            twinGrant.close();
            audit.call(() => table.close());
            audit.close();
            workspace.close();
            grant.close();
            pinned.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero charges");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [{ count: 17000, width: 36, selected: false, duplicate: false, offset: 0 }],
          [{ count: 17001, width: 15, selected: true, duplicate: true, offset: 3 }],
          [{ count: 17002, width: 65, selected: false, duplicate: false, offset: 1 }],
        ],
      },
    );
    reach.assertReached();
  }, 90000);
