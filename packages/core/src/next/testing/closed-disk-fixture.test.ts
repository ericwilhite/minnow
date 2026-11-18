/** The optimization must preserve SimDisk's immutable file images and seeded crash history. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  CLOSED_DISK_FIXTURE_REACH,
  CLOSED_DISK_FINGERPRINT_REACH,
} from "../../testing/reach.js";
import { SimDisk } from "../storage/sim-disk.js";
import { closedDiskFixture } from "./closed-disk-fixture.test-support.js";
function state(disk: SimDisk) {
  const file = disk.files.get("data");
  if (file === undefined) throw new Error("missing data");
  return file;
}
for (const seed of seedsFor("next-closed-disk-fixture", [0xb2fb, 0xb2fc]))
  it(`closed fixture forks preserve history and isolate views ${String(seed)}`, () => {
    const reach = createReach("closed disk fixture", CLOSED_DISK_FIXTURE_REACH);
    fc.assert(
      fc.property(
        fc.uint8Array({ minLength: 8, maxLength: 64 }),
        fc.integer({ min: 1, max: 255 }),
        fc.boolean(),
        fc.boolean(),
        fc.nat({ max: 65535 }),
        (initial, byte, lying, futureFlush, crashSeed) => {
          reach.beginHistory();
          const options = {
            seed: crashSeed,
            flush: lying ? ("lying" as const) : ("honest" as const),
          };
          const prepare = (s: number) => {
            const d = new SimDisk({ ...options, seed: s });
            d.epoch = 7;
            const f = d.open("data", false);
            f.write(initial, { at: 0 });
            f.flush();
            f.write(new Uint8Array(4).fill(byte), { at: 2 });
            f.write(new Uint8Array([byte, byte]), { at: f.getSize() });
            d.open("empty", false).close();
            return { d, f };
          };
          const { d: source, f } = prepare(crashSeed);
          expect(() => closedDiskFixture(source)).toThrow("live file handles");
          reach.reach("live handles refused");
          f.close();
          const before = state(source).current.slice(),
            durable = state(source).durable.slice(),
            fork = closedDiskFixture(source);
          expect(state(source).pending.length).toBeGreaterThan(0);
          reach.reach("pending writes");
          if (lying) {
            expect(state(source).queued.length).toBeGreaterThan(0);
            reach.reach("lying queued writes");
          } else {
            expect(durable).toEqual(initial);
            reach.reach("honest durable bytes");
          }
          const views: Array<{ bytes: Uint8Array; expected: Uint8Array }> = [];
          for (let branch = 0; branch < 2; branch++) {
            const d = fork({ ...options, seed: crashSeed + branch }),
              ref = prepare(crashSeed + branch);
            ref.f.close();
            expect(d.epoch).toBe(7);
            d.epoch++;
            expect(source.epoch).toBe(7);
            reach.reach("epoch carried");
            const change = (disk: SimDisk, observe: boolean) => {
              const h = disk.open("data", false),
                length = h.getSize();
              h.write(new Uint8Array([branch + 1, 99]), { at: length });
              if (observe) {
                for (const v of views) expect(v.bytes).toEqual(v.expected);
                const bytes = state(disk).current;
                views.push({ bytes, expected: bytes.slice() });
              }
              reach.reach("isolated append");
              h.write(new Uint8Array([42]), { at: 0 });
              reach.reach("isolated overwrite");
              h.truncate(length + 1);
              reach.reach("isolated truncate");
              if (futureFlush) h.flush();
              h.close();
            };
            change(d, true);
            change(ref.d, false);
            d.powerLoss();
            ref.d.powerLoss();
            expect(state(d).current).toEqual(state(ref.d).current);
            expect(state(d).durable).toEqual(state(ref.d).durable);
            expect([...d.files.keys()]).toEqual([...ref.d.files.keys()]);
            reach.reach("same seeded power loss");
            expect(() => closedDiskFixture(d)).toThrow("consumed crash random stream");
            reach.reach("consumed random stream refused");
            expect(state(source).current).toEqual(before);
            expect(state(source).durable).toEqual(durable);
            fork.verify();
          }
        },
      ),
      { seed, numRuns: 40, endOnFailure: true },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-closed-disk-fingerprint", [0xb2fd, 0xb2fe]))
  it(`closed fixture fingerprint detects shared mutations ${String(seed)}`, () => {
    const reach = createReach("closed disk fingerprint", CLOSED_DISK_FINGERPRINT_REACH);
    fc.assert(
      fc.property(fc.uint8Array({ minLength: 8, maxLength: 64 }), (initial) => {
        reach.beginHistory();
        for (const mutation of CLOSED_DISK_FINGERPRINT_REACH) {
          const options = { flush: "honest" as "honest" | "lying" },
            source = new SimDisk(options),
            handle = source.open("data", false);
          handle.write(initial, { at: 0 });
          handle.flush();
          options.flush = "lying";
          handle.write(new Uint8Array([17]), { at: 1 });
          handle.truncate(initial.length + 1);
          handle.flush();
          handle.write(new Uint8Array([23]), { at: 0 });
          handle.truncate(initial.length + 2);
          handle.close();
          const fork = closedDiskFixture(source),
            branch = fork(options),
            file = state(branch);
          // An older helper has no guard: the RED then fails the mutation assertion,
          // rather than failing while looking up a missing method.
          const verify = () => {
            const method: unknown = Reflect.get(fork, "verify");
            if (typeof method === "function") Reflect.apply(method, fork, []);
          };
          expect(() => verify()).not.toThrow();
          if (mutation === "current" || mutation === "durable") {
            file[mutation][0] = (file[mutation][0] ?? 0) ^ 1;
          } else {
            const operations = mutation.startsWith("pending") ? file.pending : file.queued,
              truncate = mutation.endsWith("truncate"),
              operation = operations.find((op) => op.kind === (truncate ? "truncate" : "write"));
            if (operation === undefined) throw new Error("Missing mutation operation");
            if (operation.kind === "truncate") Reflect.set(operation, "size", operation.size + 1);
            else if (mutation.endsWith("offset")) Reflect.set(operation, "at", operation.at + 1);
            else operation.bytes[0] = (operation.bytes[0] ?? 0) ^ 1;
          }
          expect(() => verify(), mutation).toThrow("Shared closed disk fixture changed");
          reach.reach(mutation);
        }
      }),
      { seed, numRuns: 40, endOnFailure: true },
    );
    reach.assertReached();
  });
