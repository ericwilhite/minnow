/** Complete import steps never wait on their own transport/source/control credit (governor rules2–3). */
import fc from "fast-check";
import { expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { SnapshotConnection, type SnapshotImportFunding } from "../owner/snapshot-connection.js";
import { spoolSnapshotInput } from "../write/snapshot-source.js";
import { loadSnapshot } from "../write/snapshot-load.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  SNAPSHOT_IMPORT_ADMISSION_REACH,
  SNAPSHOT_RETAINED_ADMISSION_REACH,
} from "../../testing/reach.js";

const transport = (1 << 20) + 1024;
const parts = [
  { label: "snapshot spool copy", minimumBytes: (1 << 20) + 65536 + 1024, spillable: false },
  { label: "snapshot spool source", minimumBytes: 1024, spillable: false },
];
const total = transport + parts.reduce((sum, part) => sum + part.minimumBytes, 0);
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
function region() {
  return {
    offset: 0,
    capacity: 65536,
    scratch: {
      write: () => Promise.resolve(),
      readInto: () => Promise.resolve(),
      truncate: () => Promise.resolve(),
      release: () => Promise.resolve(),
    },
  };
}
for (const seed of seedsFor("next-snapshot-import-admission", [0xb2f7, 0xb2f8]))
  it(`whole import admission and retained retry minima ${String(seed)}`, async () => {
    const reach = createReach("snapshot import admission", SNAPSHOT_IMPORT_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("oversize", "exact", "release", "cancel"),
        fc.integer({ min: 1, max: 1024 }),
        async (mode, extra) => {
          reach.beginHistory();
          const cap =
            mode === "oversize" ? 2120000 + extra - 1 : mode === "exact" ? total : 4 << 20;
          const governor = new Governor(cap),
            controller = new AbortController();
          let calls = 0,
            blocker: Grant | undefined;
          const connection = new SnapshotConnection({
            governor,
            exportMinimumBytes: 0,
            importGrants: parts,
            export: async () => {
              throw new Error("unused");
            },
            async import(source, signal, funding: SnapshotImportFunding | undefined) {
              calls++;
              const spool = await spoolSnapshotInput(source, region(), governor, {
                signal,
                maxChunkBytes: 1 << 20,
                ...(funding === undefined ? {} : { funding: funding.work }),
              });
              try {
                expect(funding, "spool must consume the atomic import group").toBeDefined();
              } finally {
                await spool.close();
              }
            },
          });
          let settled: { value?: unknown; error?: unknown } | undefined;
          try {
            const held = cap - total + extra;
            if (mode === "release" || mode === "cancel") {
              [blocker] = await governor.admitGroup({
                label: "other operation",
                includesProgress: true,
                grants: [{ label: "temporary", minimumBytes: held, spillable: false }],
              });
              expect(blocker?.allocate(held)).toBe(true);
            }
            const opening = connection.command("snapshotImportOpen", [], controller.signal);
            void opening.then(
              (value) => {
                settled = { value };
              },
              (error: unknown) => {
                settled = { error };
              },
            );
            await tick();
            if (mode === "oversize") {
              expect(
                settled?.error,
                "impossible combined minimum refuses instead of waiting",
              ).toMatchObject({
                code: "MINNOW_MEMORY_ADMISSION",
                bytes: total,
                capacity: cap,
              });
              expect(calls).toBe(0);
              reach.reach("combined oversize refuses");
            } else {
              if (mode !== "exact") {
                expect(settled).toBeUndefined();
                expect(calls).toBe(0);
                expect(governor.usedBytes, "waiting import retains no partial transport").toBe(
                  held,
                );
                reach.reach("wait holds nothing");
                if (mode === "cancel") {
                  const reason = new Error("cancel import admission");
                  controller.abort(reason);
                  await expect(opening).rejects.toBe(reason);
                  reach.reach("cancel wait");
                  return;
                }
                blocker?.close();
                reach.reach("release starts import");
              }
              const id = await opening;
              await connection.command(
                "snapshotImportWrite",
                [id as string, new Uint8Array([7])],
                controller.signal,
              );
              await connection.command("snapshotImportFinish", [id as string], controller.signal);
              expect(calls).toBe(1);
              reach.reach("whole cap admits");
            }
          } finally {
            controller.abort(new Error("test cleanup"));
            await connection.close();
            blocker?.close();
            expect(governor.usedBytes).toBe(0);
            reach.reach("all credit returned");
          }
        },
      ),
      {
        seed,
        numRuns: 9,
        examples: [
          ["oversize", 1],
          ["exact", 1],
          ["release", 1],
          ["cancel", 1],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-snapshot-retained-admission", [0xb2f9, 0xb2fa]))
  it(`retry includes immutable source and caller control ${String(seed)}`, async () => {
    const reach = createReach("retained snapshot admission", SNAPSHOT_RETAINED_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("source", "control", "spool"),
        fc.integer({ min: 1, max: 128 }),
        async (kind, extra) => {
          reach.beginHistory();
          const governor = new Governor(2 << 20),
            controller = new AbortController();
          const spool =
            kind === "spool"
              ? await spoolSnapshotInput(
                  (async function* () {
                    yield new Uint8Array(64);
                  })(),
                  region(),
                  governor,
                  { maxChunkBytes: 64 },
                )
              : undefined;
          const held = governor.capBytes - (spool === undefined ? 0 : 1024) - 1400 + extra;
          const [control] = await governor.admitGroup({
            label: "retained owner",
            includesProgress: true,
            grants: [{ label: "control", minimumBytes: held, spillable: false }],
          });
          if (control === undefined) throw new Error("missing control");
          expect(control.allocate(held)).toBe(true);
          const input = spool ?? {
            byteLength: 64,
            retainedBytes: (owner: unknown) =>
              owner === governor && kind === "source" ? control.held : 0,
            readInto: (_at: number, into: Uint8Array) => {
              into.fill(0);
              return Promise.resolve();
            },
          };
          let result: unknown;
          const loading = loadSnapshot(
            input,
            () => {
              throw new Error("must refuse before target");
            },
            governor,
            {
              signal: controller.signal,
              retainedBytes: () => (kind === "source" ? 0 : control.held),
            },
          );
          void loading.catch((error: unknown) => {
            result = error;
          });
          try {
            await tick();
            expect(
              result,
              "impossible step plus retained source/control must refuse",
            ).toMatchObject({ code: "MINNOW_MEMORY_ADMISSION" });
            expect((result as { bytes: number }).bytes).toBeGreaterThan(governor.capBytes);
            reach.reach(kind);
            reach.reach("typed combined refusal");
            expect(governor.usedBytes).toBe(held + (spool === undefined ? 0 : 1024));
          } finally {
            controller.abort(new Error("cleanup"));
            await loading.catch(() => undefined);
            await spool?.close();
            control.close();
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero final charges");
          }
        },
      ),
      {
        seed,
        numRuns: 9,
        examples: [
          ["source", 1],
          ["control", 1],
          ["spool", 1],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
