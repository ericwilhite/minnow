import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, OWNED_PARAMETER_COLUMN_BOUNDS_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { bindParameter } from "./parameters.js";
import { ParameterCheckpoint } from "./parameter-checkpoint.js";
import { TapeDirectory } from "./agg/hash-tape.js";
import { PageWriter, SpillFiles } from "./spill-pages.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
it.each(seedsFor("owned-parameter-checkpoint", [0xb4c5, 0xb4c6]))(
  "restores admitted copies after caller mutation and old grant release (seed %i)",
  async (seed) => {
    const reach = createReach("parameter handoff", [
      "empty",
      "large",
      "shared prefix",
      "caller mutation",
      "old grant released",
      ...OWNED_PARAMETER_COLUMN_BOUNDS_REACH,
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 65537, max: 1048576 }), async (length) => {
        reach.beginHistory();
        const governor = new Governor(32 << 20),
          old = await governor.admit({
            label: "owned inputs",
            minimumBytes: 8 << 20,
            spillable: false,
          }),
          iog = await governor.admit({ label: "I/O", minimumBytes: 2 << 20, spillable: false });
        const backend = new MemorySpillService(),
          files = new SpillFiles(backend, 1, {
            allocate: () => false,
            free: () => {
              throw new Error("unbuffered");
            },
          }),
          pool = new Pool(new Charge(iog), 2 << 20),
          pages = new PageWriter(pool),
          io = new SpillIo(pool, files, pages, { grant: iog, queryId: 1 }, () => undefined),
          directory = new TapeDirectory(io, pool),
          cp = new ParameterCheckpoint(directory);
        const backing = Uint8Array.from({ length: length + 1 }, (_, i) =>
          i < length ? i % 251 : 17,
        );
        const caller = [backing.subarray(0, 0), backing.subarray(0, length), backing];
        let owned = caller.map((value, i) => bindParameter({ kind: "bytes" }, value, i, old));
        const expected = caller.map((b) => b.slice());
        for (const b of caller) b.fill(9);
        reach.reach("caller mutation");
        let restored: Awaited<ReturnType<ParameterCheckpoint["restore"]>> | undefined;
        try {
          await cp.save(owned);
          // Slot identity survives pressure re-admission: the empty key must not
          // inherit either wide payload's bound, even when their inputs alias.
          const binding = cp.binding(),
            phase = cp.phaseBinding(new AbortController().signal);
          const sizes = expected.map((bytes) => 512 + bytes.length);
          expect(binding.slotBytes).toEqual(sizes);
          expect(phase.slotBytes).toEqual(sizes);
          expect(phase.logicalBytes).toBe(sizes.reduce((n, size) => n + size, 0));
          for (const state of OWNED_PARAMETER_COLUMN_BOUNDS_REACH) reach.reach(state);
          owned = [];
          old.free(old.allocated);
          old.close();
          expect(old.held).toBe(0);
          reach.reach("old grant released");
          const [grant] = await governor.admitGroup({
            label: "restore parameters",
            grants: [
              { label: "parameter copy", minimumBytes: cp.binding().bytes, spillable: false },
            ],
          });
          if (!grant) throw new Error("restore grant missing");
          try {
            restored = await cp.restore(grant);
            for (let i = 0; i < expected.length; i++) {
              const slot = restored.slots[i];
              if (slot?.data.layout !== "varlen") throw new Error("wrong restored bytes");
              const oracle = expected[i];
              if (!oracle) throw new Error("Missing owned byte oracle");
              expect(slot.data.bytes.length).toBe(oracle.length);
              expect(slot.data.bytes.every((byte, at) => byte === oracle[at])).toBe(true);
            }
            reach.reach("empty");
            reach.reach("large");
            reach.reach("shared prefix");
          } finally {
            restored?.close();
            expect(grant.allocated).toBe(0);
            grant.close();
          }
        } finally {
          await cp.close();
          await files.closeAll();
          directory.detach();
          pages.close();
          pool.close();
          old.close();
          iog.close();
        }
        expect(governor.usedBytes).toBe(0);
        expect(backend.bytesInUse).toBe(0);
        reach.reach("zero");
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  },
  30000,
);

it.each(seedsFor("owned-parameter-native-checkpoint", [0xb4c9, 0xb4ca]))(
  "binds native scalar copies within the saved admission and preserves float bits (seed %i)",
  async (seed) => {
    const reach = createReach("native parameter handoff", [
      "native scalar",
      "wide numeric",
      "negative zero",
      "exact admission",
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: -1000000, max: 1000000 }), async (value) => {
        reach.beginHistory();
        const governor = new Governor(32 << 20),
          old = await governor.admit({
            label: "owned native",
            minimumBytes: 2 << 20,
            spillable: false,
          }),
          iog = await governor.admit({
            label: "native I/O",
            minimumBytes: 2 << 20,
            spillable: false,
          });
        const backend = new MemorySpillService(),
          files = new SpillFiles(backend, 1, {
            allocate: () => false,
            free: () => {
              throw new Error("unbuffered");
            },
          }),
          pool = new Pool(new Charge(iog), 2 << 20),
          pages = new PageWriter(pool),
          io = new SpillIo(pool, files, pages, { grant: iog, queryId: 1 }, () => undefined),
          directory = new TapeDirectory(io, pool),
          cp = new ParameterCheckpoint(directory);
        const owned = [
          bindParameter({ kind: "bool" }, value % 2 === 0, 0, old),
          bindParameter({ kind: "int32", bits: 32 }, value, 1, old),
          bindParameter({ kind: "int64" }, (1n << 62n) + BigInt(value), 2, old),
          bindParameter({ kind: "float32" }, value / 128, 3, old),
          bindParameter({ kind: "float64" }, -0, 4, old),
          bindParameter({ kind: "float64" }, Number.NaN, 5, old),
          bindParameter(
            { kind: "decimal", precision: null, scale: null },
            (10n ** 90n + BigInt(value)).toString(),
            6,
            old,
          ),
          bindParameter({ kind: "int64" }, null, 7, old),
        ];
        const expected = owned.map((slot) => ({
          ...slot,
          data: structuredClone(slot.data),
          validity: slot.validity?.slice() ?? null,
        }));
        let restored: Awaited<ReturnType<ParameterCheckpoint["restore"]>> | undefined;
        let grant: import("../memory/governor.js").Grant | undefined;
        try {
          await cp.save(owned);
          const binding = cp.binding();
          owned.length = 0;
          old.close();
          [grant] = await governor.admitGroup({
            label: "native restore",
            grants: [{ label: "inputs", minimumBytes: binding.bytes, spillable: false }],
          });
          if (!grant) throw new Error("missing native restore");
          restored = await cp.restore(grant);
          expect(grant.allocated).toBe(binding.bytes);
          expect(restored.slots).toEqual(expected);
          const zero = restored.slots[4];
          if (zero?.data.layout !== "f64") throw new Error("missing float slot");
          expect(
            new BigUint64Array(zero.data.values.buffer, zero.data.values.byteOffset, 1)[0],
          ).toBe(1n << 63n);
          reach.reach("native scalar");
          reach.reach("wide numeric");
          reach.reach("negative zero");
          reach.reach("exact admission");
        } finally {
          restored?.close();
          expect(grant?.allocated ?? 0).toBe(0);
          grant?.close();
          await cp.close();
          await files.closeAll();
          directory.detach();
          pages.close();
          pool.close();
          old.close();
          iog.close();
        }
        expect(governor.usedBytes).toBe(0);
        expect(backend.bytesInUse).toBe(0);
        reach.reach("zero");
      }),
      { seed, numRuns: 6 },
    );
    reach.assertReached();
  },
  30000,
);
