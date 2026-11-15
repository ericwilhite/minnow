/** R4/pg_dump: sequence state is nontransactional, even when the row pin is durable-only. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_SEQUENCE_DURABILITY_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { SnapshotKind, SnapshotStreamReader } from "../format/snapshot.js";
import { exportSnapshot } from "./snapshot-capture.js";
import { decodeSnapshotCatalog } from "./snapshot-catalog.js";
import { loadSnapshot } from "./snapshot-load.js";
import { snapshotBlob } from "./snapshot-source.js";
import { SnapshotAccount } from "./snapshot-stage.js";
import { snapshotTestTarget } from "./snapshot-test-support.js";
import { Harness, autocommit } from "./test-support.js";

async function horizon(parts: readonly Uint8Array[], grant: Grant): Promise<bigint> {
  const account = new SnapshotAccount(grant),
    reader = await SnapshotStreamReader.open(
      (async function* () {
        yield* parts;
      })(),
      grant,
    );
  try {
    const chunks: Uint8Array[] = [];
    let length = 0;
    for await (const frame of reader.frames())
      if (frame.kind === SnapshotKind.catalogPart) {
        account.take(frame.payload.length + 64);
        chunks.push(frame.payload.slice());
        length += frame.payload.length;
      }
    account.take(length);
    const bytes = new Uint8Array(length);
    let at = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, at);
      at += chunk.length;
    }
    const decoded = decodeSnapshotCatalog(bytes, grant);
    try {
      expect(decoded.value.horizons).toHaveLength(1);
      const value = decoded.value.horizons[0]?.reservedThrough;
      if (value === undefined) throw new Error("Missing sequence fixture horizon");
      return value;
    } finally {
      grant.free(decoded.charge);
    }
  } finally {
    reader.close();
    account.close();
  }
}

for (const seed of seedsFor("next-snapshot-sequence-durability", [0xb297, 0xb298]))
  it(`never reuses an emitted sequence value across durable row pins seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot sequence durability", SNAPSHOT_SEQUENCE_DURABILITY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 8 }),
        fc.integer({ min: 1, max: 20 }),
        fc.integer({ min: 1, max: 3 }),
        async (many, salt, step) => {
          reach.beginHistory();
          for (const direction of [1n, -1n])
            for (const mode of ["emitted ahead", "recycled", "restored"] as const)
              for (const count of [1, many]) {
                const source = new Harness("strict", "memory"),
                  initial = await source.open(),
                  increment = BigInt(step) * direction,
                  start = BigInt(salt) * direction,
                  parts: Uint8Array[] = [];
                let path = initial;
                await autocommit(path, (statement) =>
                  statement.changeCatalog({
                    kind: "createSequence",
                    sequence: {
                      name: "s",
                      start,
                      increment,
                      min: -100000n,
                      max: 100000n,
                      cycle: false,
                      ownedBy: null,
                    },
                  }),
                );
                // Admit BEFORE emission: an await between next() and export would erase the
                // strict durability gap this law must reach. Keep the one-value pinned repro.
                const grant = await source.governor.admit({
                  label: "sequence gap export",
                  minimumBytes: 8 << 20,
                  spillable: false,
                });
                try {
                  const emitted = { repr: "int64" as const, values: new BigInt64Array(count) };
                  const reservation = path.sequences.next(1, count, emitted, 0);
                  expect(typeof reservation).toBe("number");
                  const last = emitted.values[count - 1];
                  if (last === undefined) throw new Error("Missing emitted fixture value");
                  expect(last).toBe(start + BigInt(count - 1) * increment);
                  if (mode === "emitted ahead") {
                    expect(path.appliedLsn).toBeGreaterThan(path.durableLsn);
                    reach.reach("emitted ahead of durable");
                  } else {
                    await path.recycleLog();
                    expect(path.durableLsn).toBeGreaterThanOrEqual(Number(reservation));
                    reach.reach("durable ahead of emitted");
                    if (mode === "restored") {
                      await source.close();
                      path = await source.open();
                      reach.reach("restored horizon");
                    }
                  }
                  const applied = path.appliedLsn,
                    durable = path.durableLsn;
                  let pinned = -1;
                  const exporting = exportSnapshot(
                    {
                      sequences: path.sequences,
                      pin() {
                        const pin = path.pin();
                        pinned = pin.observedLsn;
                        return pin;
                      },
                    },
                    new Uint8Array(16),
                    {
                      write(bytes) {
                        parts.push(bytes.slice());
                      },
                    },
                    grant,
                  );
                  expect(pinned).toBe(durable);
                  if (mode === "emitted ahead") expect(pinned).toBeLessThan(applied);
                  reach.reach("pin before await");
                  await exporting;
                  expect(path.appliedLsn).toBe(applied);
                  const through = await horizon(parts, grant);
                  const next = { repr: "int64" as const, values: new BigInt64Array(1) };
                  await path.sequences.next(1, 1, next, 0);
                  expect(next.values[0]).toBe(mode === "restored" ? through : last + increment);
                  reach.reach("read only allocation");
                  expect(grant.allocated).toBe(0);

                  const governor = new Governor(64 << 20),
                    target = new Harness("strict", "memory");
                  Object.assign(target, { governor });
                  const startup = await governor.admit({
                    label: "sequence gap load owner",
                    minimumBytes: 8 << 20,
                    spillable: true,
                  });
                  try {
                    await loadSnapshot(
                      snapshotBlob(new Blob(parts.map((part) => part.slice().buffer))),
                      () => snapshotTestTarget(target, 0, true, undefined, startup),
                      governor,
                    );
                  } finally {
                    startup.close();
                  }
                  expect(governor.usedBytes).toBe(0);
                  try {
                    const loaded = await target.open();
                    await loaded.sequences.next(1, 1, next, 0);
                    expect(next.values[0]).toBe(through);
                    expect(direction * (next.values[0] ?? 0n)).toBeGreaterThan(direction * last);
                    reach.reach("ordinary load nextval");
                    const used = next.values[0] ?? 0n;
                    await target.close();
                    const reopened = await target.open();
                    await reopened.sequences.next(1, 1, next, 0);
                    expect(direction * (next.values[0] ?? 0n)).toBeGreaterThan(direction * used);
                    reach.reach("ordinary reopen nextval");
                  } finally {
                    await target.close();
                  }
                  expect(governor.usedBytes).toBe(0);
                  reach.reach(direction > 0n ? "ascending" : "descending");
                  reach.reach(count === 1 ? "single emitted value" : "multiple emitted values");
                } finally {
                  grant.close();
                  await source.close();
                }
                expect(source.governor.usedBytes).toBe(0);
                reach.reach("charges released");
              }
        },
      ),
      { seed, numRuns: 4, endOnFailure: true },
    );
    reach.assertReached();
  }, 60000);
