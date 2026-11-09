/** Publication proves exact A-B retirements and B's sealed ownership in one root merge.
 * Authority: PublishChange/ExtentSpace.publish (extents.ts), R8 and the S/W workspace contract.
 * The model partitions coordinates independently; a cursor-count guard rejects rescanning roots.
 */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, PUBLICATION_MERGE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { TaskClock } from "../task.js";
import type { ExtentRef } from "./extents.js";
import {
  MetadataJournal,
  MetadataWorkspace,
  METADATA_WORKSPACE_BYTES,
} from "./metadata-journal.js";
import { openPairs, spoolExtents } from "./metadata-extents.js";
import { validatePublication } from "./publication-validation.js";
import { ScriptedMetadataPages } from "./metadata-test-support.js";
import { countingBudget } from "./space-test-support.js";

const reach = createReach("next-publication-merge", PUBLICATION_MERGE_REACH);
const align = (n: number) => Math.ceil(n / 4096) * 4096;
const roles = ["kept", "retired", "new", "unused"] as const;
type Role = (typeof roles)[number];
interface Model {
  current: ExtentRef[];
  next: ExtentRef[];
  claims: ExtentRef[];
  owners: ExtentRef[];
  unused: ExtentRef[];
}
function model(entries: ReadonlyArray<readonly [Role, number]>): Model {
  const m: Model = { current: [], next: [], claims: [], owners: [], unused: [] };
  entries.forEach(([role, length], index) => {
    const extent = { offset: 2 ** 40 + index * 16384, length };
    if (role === "kept" || role === "retired") m.current.push(extent);
    if (role === "kept" || role === "new") m.next.push(extent);
    if (role === "retired") m.claims.push({ ...extent, length: align(length) });
    if (role === "new" || role === "unused") m.owners.push(extent);
    if (role === "unused") m.unused.push(extent);
  });
  return m;
}
const ordered = (xs: ExtentRef[]) => xs.sort((a, b) => a.offset - b.offset);

for (const seed of seedsFor("next-publication-merge", [28101, 28102, 28103, 28104]))
  it(`merges ownership once and refuses false proofs, seed ${String(seed)}`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.tuple(
          fc.tuple(...roles.map(() => fc.integer({ min: 1, max: 8192 }))),
          fc.array(fc.tuple(fc.constantFrom(...roles), fc.integer({ min: 1, max: 8192 })), {
            maxLength: 128,
          }),
        ),
        async ([lengths, tail]) => {
          reach.beginHistory();
          const entries = [...roles.map((role, i) => [role, lengths[i] ?? 1] as const), ...tail];
          const m = model(entries);
          const budget = countingBudget(METADATA_WORKSPACE_BYTES + (1 << 20));
          const workspace = MetadataWorkspace.create(budget);
          expect(budget.allocate(1 << 20)).toBe(true);
          const io = { slab: new Uint8Array(1 << 20) };
          const journal = new MetadataJournal(new ScriptedMetadataPages(), workspace);
          const handles: number[] = [];
          const clock = new TaskClock(() => 0);
          const work = { clock };
          const save = async (xs: ExtentRef[]) => {
            const h = await workspace.run("spool", () => spoolExtents(journal, xs, 1, work));
            handles.push(h);
            return h;
          };
          const read = (h: number) =>
            workspace.run("spool", async () => {
              const cursor = await openPairs(journal, h, 1, true);
              const result: ExtentRef[] = [];
              try {
                while (await cursor.next(work))
                  result.push({ offset: cursor.offset, length: cursor.length });
                return result;
              } finally {
                await cursor.close();
              }
            });
          const run = async (input: Model, failure?: RegExp) => {
            const a = await save(input.current),
              b = await save(input.next),
              c = await save(input.claims),
              d = await save(input.owners);
            const cursors = vi.spyOn(journal, "cursor");
            try {
              const checking = validatePublication(journal, a, b, c, d, io, work);
              if (failure !== undefined) await expect(checking).rejects.toThrow(failure);
              else {
                const checked = await checking;
                handles.push(checked.unused);
                expect(checked.retiredBytes).toBe(
                  input.claims.reduce((sum, e) => sum + align(e.length), 0),
                );
                for (const h of [a, b])
                  expect(
                    cursors.mock.calls.filter(([handle]) => handle === h).length,
                    "each immutable root must be scanned once for both proofs",
                  ).toBe(1);
                cursors.mockRestore();
                expect(await read(checked.unused)).toEqual(input.unused);
              }
            } finally {
              cursors.mockRestore();
            }
          };
          try {
            await run(m);
            reach.reach("mixed ownership");
            reach.reach("one root scan");
            reach.reach("exact rounded retirements");
            reach.reach("unused sealed output");
            for (const [name, role] of [
              ["empty current", "new"],
              ["empty next", "retired"],
              ["all shared", "kept"],
            ] as const) {
              await run(model(entries.map(([, length]) => [role, length])));
              reach.reach(name);
            }
            const retired = m.claims[0],
              shared = m.current[0],
              incoming = m.owners[0];
            if (retired === undefined || shared === undefined || incoming === undefined)
              throw new Error("Missing guaranteed generator roles");
            for (const [name, input, error] of [
              ["missing claim", { ...m, claims: m.claims.slice(1) }, /retirement/],
              [
                "extra claim",
                { ...m, claims: ordered([...m.claims, { offset: 2 ** 42, length: 4096 }]) },
                /retirement/,
              ],
              [
                "wrong claim length",
                {
                  ...m,
                  claims: [{ ...retired, length: retired.length + 4096 }, ...m.claims.slice(1)],
                },
                /retirement/,
              ],
              ["duplicate claim", { ...m, claims: ordered([...m.claims, retired]) }, /repeated/],
              [
                "changed shared length",
                {
                  ...m,
                  next: m.next.map((e) => (e === shared ? { ...e, length: e.length + 1 } : e)),
                },
                /disagree on extent length/,
              ],
              ["missing sealed owner", { ...m, owners: m.owners.slice(1) }, /neither current-root/],
              [
                "wrong sealed length",
                {
                  ...m,
                  owners: [{ ...incoming, length: incoming.length + 1 }, ...m.owners.slice(1)],
                },
                /neither current-root/,
              ],
              [
                "writer overlaps shared",
                { ...m, owners: ordered([...m.owners, shared]) },
                /writer overlaps/,
              ],
              [
                "incoming overlaps left",
                {
                  ...m,
                  current: [{ ...shared, length: 8192 }, ...m.current.slice(1)],
                  next: ordered([
                    ...m.next.filter((e) => e !== shared),
                    { offset: shared.offset + 4096, length: 1 },
                  ]),
                  claims: ordered([...m.claims, { ...shared, length: 8192 }]),
                  owners: ordered([...m.owners, { offset: shared.offset + 4096, length: 1 }]),
                },
                /incoming extent overlaps/,
              ],
              [
                "incoming overlaps right",
                {
                  ...m,
                  next: [{ offset: shared.offset - 4096, length: 8192 }, ...m.next],
                  owners: [{ offset: shared.offset - 4096, length: 8192 }, ...m.owners],
                },
                /incoming extent overlaps/,
              ],
            ] satisfies Array<[(typeof PUBLICATION_MERGE_REACH)[number], Model, RegExp]>) {
              await run(input, error);
              reach.reach(name);
            }
          } finally {
            for (const h of handles) await workspace.run("spool", () => journal.release(h));
            workspace.close();
            budget.free(1 << 20);
          }
          expect(budget.used).toBe(0);
          reach.reach("exact cleanup");
        },
      ),
      { seed, numRuns: 8 },
    );
  });
afterAll(() => reach.assertReached());
