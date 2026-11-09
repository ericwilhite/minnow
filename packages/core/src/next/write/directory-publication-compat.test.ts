/** Copy-on-write packing transitions are atomic across all physical publication edges. Both
 * supported builds must refuse damaged pages and serialize competing ordinary openers. */
import { deepStrictEqual } from "node:assert/strict";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach, DIRECTORY_TRANSITION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { SimLocks, SimulatedCrash } from "../storage/sim-disk.js";
import { openDirectory, type DirectoryDb } from "./directory-compat-api.js";
import { DirectoryFiles, simulatedImage, type DirectoryImage } from "./directory-fixture-files.js";
import { frozenDirectory, readDirectoryFixture } from "./directory-frozen.js";
import { decodeGroupPage, decodeGroupIndex } from "../format/manifest-codec.js";
import { partition } from "./directory-tails-test-support.js";
import { yieldTask } from "./merge.js";
const reach = createReach("next-directory-transition", DIRECTORY_TRANSITION_REACH);
const transitions = ["packed normalization", "full carry", "partial edit", "shared carry"] as const;
const edges = [
  "page written",
  "index written",
  "data flushed",
  "root written",
  "root truncated",
  "root flushed",
] as const;
type Transition = (typeof transitions)[number];
const range = (n: number) => Array.from({ length: n }, (_, k) => k);
let old: Awaited<ReturnType<typeof frozenDirectory>>;
let images: Record<Transition, DirectoryImage>;
beforeAll(async () => {
  old = await frozenDirectory();
  const empty = await readDirectoryFixture("packed-0");
  const files = DirectoryFiles.load(empty),
    locks = new SimLocks();
  const db = await openDirectory(files, locks);
  await db.insert(range(7));
  await db.settle();
  await db.close();
  const shared = files.image();
  const edited = await openDirectory(files, locks);
  await edited.delete(6);
  await edited.settle();
  await edited.close();
  images = {
    "packed normalization": await readDirectoryFixture("packed-4099"),
    "full carry": await readDirectoryFixture("packed-4095"),
    "partial edit": shared,
    "shared carry": files.image(),
  };
}, 60_000);
afterAll(async () => {
  await old.dispose();
});
const values = (transition: Transition) =>
  transition === "packed normalization"
    ? range(4100)
    : transition === "full carry"
      ? range(4096)
      : transition === "partial edit"
        ? range(6)
        : [...range(6), 7];
async function change(db: DirectoryDb, transition: Transition) {
  if (transition === "partial edit") await db.delete(6);
  else
    await db.insert([
      transition === "packed normalization" ? 4099 : transition === "full carry" ? 4095 : 7,
    ]);
}
function crashCause(error: unknown): boolean {
  for (let e: unknown = error; e instanceof Error; e = e.cause)
    if (
      ["SimulatedCrash", "InvalidStateError", "OwnershipLostError", "StoreClosedError"].includes(
        e.name,
      ) ||
      e.message === "Simulated crash"
    )
      return true;
  return false;
}
for (const seed of seedsFor("next-directory-publication-compat", [28011, 28012, 28013, 28014]))
  for (const build of ["current", "baseline"] as const)
    it(`publication interruption and ordinary retry ${build}, seed ${String(seed)}`, async () => {
      for (const transition of transitions)
        for (const edge of edges) {
          reach.beginHistory();
          const plan = { seed, crashAtStep: Infinity };
          const disk = simulatedImage(images[transition], plan),
            locks = new SimLocks();
          const db = await (build === "current" ? openDirectory : old.open)(disk, locks);
          let armed = false;
          const before = await db.pages(),
            rewritten: number[] = [];
          const suffix =
            transition === "packed normalization"
              ? [4]
              : transition === "full carry"
                ? [4096]
                : transition === "partial edit"
                  ? build === "current"
                    ? [1]
                    : [7]
                  : [8];
          const nextCounts =
            transition === "packed normalization"
              ? [4096, 4]
              : transition === "full carry"
                ? [4096]
                : transition === "partial edit"
                  ? build === "current"
                    ? [4, 2, 1]
                    : [7]
                  : [8];
          const kept =
            transition === "packed normalization"
              ? 1
              : transition === "partial edit" && build === "current"
                ? 2
                : 0;
          let indexSeen = false;
          if (kept > 0) expect(before[0]?.ref.extent.offset).toBe(before[1]?.ref.extent.offset);
          const begin = db.space.beginWrite.bind(db.space);
          db.space.beginWrite = (...args) => {
            const writer = begin(...args),
              write = writer.write.bind(writer);
            writer.write = async (parts) => {
              const isPage = parts.some(
                (p) => p[0] === 77 && p[1] === 71 && p[2] === 80 && p[3] === 49,
              );
              const isIndex = parts.some(
                (p) => p[0] === 77 && p[1] === 71 && p[2] === 73 && p[3] === 49,
              );
              if (armed && isPage)
                for (const p of parts)
                  if (p[0] === 77 && p[1] === 71 && p[2] === 80 && p[3] === 49)
                    rewritten.push(
                      decodeGroupPage(p, { allocate: () => true, free: () => undefined }).value
                        .length,
                    );
              if (armed && isIndex) {
                const bytes = parts.find(
                  (p) => p[0] === 77 && p[1] === 71 && p[2] === 73 && p[3] === 49,
                );
                if (bytes === undefined) throw new Error("missing index");
                const entries = decodeGroupIndex(bytes, {
                  allocate: () => true,
                  free: () => undefined,
                }).value;
                deepStrictEqual(
                  entries.map((p) => p.groupCount),
                  nextCounts,
                );
                deepStrictEqual(entries.slice(0, kept), before.slice(0, kept));
                indexSeen = true;
              }
              const result = await write(parts);
              if (
                armed &&
                ((edge === "page written" && isPage) || (edge === "index written" && isIndex))
              ) {
                deepStrictEqual(rewritten, suffix);
                armed = false;
                disk.powerLoss();
                throw new SimulatedCrash();
              }
              return result;
            };
            return writer;
          };
          const publish = db.session.publish.bind(db.session);
          db.session.publish = async (root) => {
            const at = edges.indexOf(edge) - 2;
            if (armed && at >= 0) {
              deepStrictEqual(rewritten, suffix);
              expect(indexSeen).toBe(true);
              armed = false;
              plan.crashAtStep = disk.steps + at;
            }
            return publish(root);
          };
          try {
            await change(db, transition); // Strict durable acknowledgement precedes the armed maintenance.
            armed = true;
            // Another in-flight waiter may first report the dead handle/fence after power loss.
            // Only crash/closed-ownership errors are allowed; recovered values remain exact.
            await expect(db.settle(), `${transition}/${edge}`).rejects.toSatisfy(crashCause);
            expect(disk.crashes, `${transition}/${edge} reached its physical crash`).toBe(1);
          } finally {
            locks.reset();
            try {
              await db.close();
            } catch (error) {
              expect(disk.crashes, `cleanup after ${transition}/${edge}: ${String(error)}`).toBe(1);
            }
          }
          const retry = await openDirectory(disk, locks);
          try {
            deepStrictEqual(await retry.rows(), values(transition));
            await retry.settle();
            await retry.space.audit();
            const recovered = await retry.pages();
            const counts = recovered.map((p) => p.groupCount);
            // An old publication may survive as packed [7], or replay in the current build
            // may perform the edit as [4,2,1]. Both are supported before another rewrite.
            if (build === "baseline" && transition === "partial edit")
              expect(["7", "4,2,1"]).toContain(counts.join(","));
            else deepStrictEqual(counts, nextCounts);
            deepStrictEqual(recovered.slice(0, kept), before.slice(0, kept));
            await retry.insert([10000]);
            await retry.settle();
            const count =
              before.reduce((n, p) => n + p.groupCount, 0) +
              (transition === "partial edit" ? 0 : 1) +
              1;
            deepStrictEqual(
              (await retry.pages()).map((p) => p.groupCount),
              partition(count),
            );
          } finally {
            await retry.close();
          }
          expect(retry.governor.usedBytes).toBe(0);
          const baseline = await old.open(disk, locks);
          try {
            deepStrictEqual(await baseline.rows(), [...values(transition), 10000]);
            await baseline.insert([10001]);
            await baseline.settle();
            await baseline.space.audit();
          } finally {
            await baseline.close();
          }
          const reopened = await openDirectory(disk, locks);
          try {
            deepStrictEqual(await reopened.rows(), [...values(transition), 10000, 10001]);
            await reopened.space.audit();
          } finally {
            await reopened.close();
          }
          reach.reach(transition);
          reach.reach(edge);
          reach.reach("acknowledged values recovered");
          reach.reach("retry both writers");
          reach.reach(`interrupted ${build} writer`);
        }
    }, 300_000);
for (const transition of transitions)
  for (const first of ["current", "baseline"] as const)
    it(`two competing opens: ${first} owns ${transition}`, async () => {
      reach.beginHistory();
      const files = DirectoryFiles.load(images[transition]),
        locks = new SimLocks();
      const owner = await (first === "current" ? openDirectory : old.open)(files, locks);
      let settled = false;
      const waiting = (first === "current" ? old.open : openDirectory)(files, locks).then((db) => {
        settled = true;
        return db;
      });
      try {
        await yieldTask();
        await yieldTask();
        expect(settled, "the second opener waits on the shared fence").toBe(false);
        await change(owner, transition);
        await owner.settle();
        await owner.space.audit();
      } finally {
        await owner.close();
      }
      const next = await waiting;
      try {
        deepStrictEqual(await next.rows(), values(transition));
        await next.insert([10000]);
        await next.settle();
        await next.space.audit();
      } finally {
        await next.close();
      }
      const reopened = await openDirectory(files, locks);
      try {
        deepStrictEqual(await reopened.rows(), [...values(transition), 10000]);
      } finally {
        await reopened.close();
      }
      reach.reach(`concurrent ${first} owner`);
      reach.reach(transition);
    }, 60_000);
for (const transition of transitions)
  for (const build of ["current", "baseline"] as const)
    it(`${build} refuses damaged page/index after ${transition}`, async () => {
      reach.beginHistory();
      const files = DirectoryFiles.load(images[transition]),
        locks = new SimLocks();
      const db = await openDirectory(files, locks);
      await change(db, transition);
      await db.settle();
      const pages = await db.pages();
      const index = db.path.state().root.tables.find((t) => t.tableId === 1)?.groups;
      expect(index).toBeDefined();
      const target = pages.at(-1)?.ref;
      expect(target).toBeDefined();
      await db.close();
      const image = files.image();
      for (const [kind, ref] of [
        ["page", target],
        ["index", index],
      ] as const) {
        if (ref === undefined) throw new Error("missing damage target");
        const broken = DirectoryFiles.load(image),
          data = broken.files.get("data");
        if (data === undefined) throw new Error("missing data");
        const at = ref.extent.offset + ref.offset + ref.length - 1;
        data.bytes[at] = (data.bytes[at] ?? 0) ^ 1;
        await expect(
          (build === "current" ? openDirectory : old.open)(broken, new SimLocks()),
        ).rejects.toThrow(/checksum|corrupt/i);
        reach.reach(`damaged ${kind} refused`);
      }
      reach.reach(`damage refused by ${build}`);
      reach.reach(transition);
    }, 60_000);
it("reached packing transition failure and concurrency states", () => reach.assertReached());
