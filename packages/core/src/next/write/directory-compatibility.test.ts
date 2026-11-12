import { readdir, readFile } from "node:fs/promises";
import { deepStrictEqual } from "node:assert/strict";
import { expect, it, beforeAll, afterAll } from "vitest";
import { createReach } from "../../testing/reach.js";
import { SimLocks } from "../storage/sim-disk.js";
import { openDirectory } from "./directory-compat-api.js";
import { DirectoryFiles } from "./directory-fixture-files.js";
import { directoryPeer, currentDirectoryFixture } from "./directory-current.test-support.js";
import { partition, expected } from "./directory-tails-test-support.js";
const reach = createReach("next-directory-compat-fixtures", [
  "reused packed tail",
  "partial extent retained after edit",
  "shared extent carry retired",
  "full-page carry",
  "full-page plus tail",
]);
let legacy: Awaited<ReturnType<typeof directoryPeer>>;
beforeAll(async () => {
  legacy = await directoryPeer();
});
afterAll(async () => {
  await legacy.dispose();
});
for (const n of [4095, 4099])
  it(`reused packed-${String(n)} opens, normalizes, writes and reopens`, async () => {
    reach.beginHistory();
    const native = DirectoryFiles.load(
      await currentDirectoryFixture(n === 4095 ? "packed-4095" : "packed-4099"),
    );
    const gate = new SimLocks();
    const old = await legacy.open(native, gate);
    try {
      deepStrictEqual(await old.rows(), expected(n));
      deepStrictEqual(
        (await old.pages()).map((p) => p.groupCount),
        n === 4095 ? [4095] : [4096, 3],
      );
    } finally {
      await old.close();
    }
    const db = await openDirectory(native, gate);
    try {
      deepStrictEqual(await db.rows(), expected(n));
      await db.insert([n]);
      await db.settle();
      deepStrictEqual(
        (await db.pages()).map((p) => p.groupCount),
        partition(n + 1),
      );
      await db.space.audit();
    } finally {
      await db.close();
    }
    const back = await legacy.open(native, gate);
    try {
      deepStrictEqual(await back.rows(), expected(n + 1));
      await back.insert([n + 1]);
      await back.settle();
    } finally {
      await back.close();
    }
    const again = await openDirectory(native, gate);
    try {
      deepStrictEqual(await again.rows(), expected(n + 2));
      await again.space.audit();
    } finally {
      await again.close();
    }
    reach.reach("reused packed tail");
  }, 120_000);
it("edits only the last page of a shared partial extent, then carries it", async () => {
  reach.beginHistory();
  const native = DirectoryFiles.load(await currentDirectoryFixture("packed-0"));
  const gate = new SimLocks();
  const db = await openDirectory(native, gate);
  try {
    await db.insert(expected(7));
    await db.settle();
    const before = await db.pages();
    deepStrictEqual(
      before.map((p) => p.groupCount),
      [4, 2, 1],
    );
    expect(new Set(before.map((p) => p.ref.extent.offset)).size).toBe(1);
    await db.delete(6);
    await db.settle();
    const after = await db.pages();
    deepStrictEqual(after.slice(0, 2), before.slice(0, 2));
    expect(after[2]?.ref.extent.offset).not.toBe(before[2]?.ref.extent.offset);
    await db.space.audit();
    reach.reach("partial extent retained after edit");
  } finally {
    await db.close();
  }
  const old = await legacy.open(native, gate);
  try {
    deepStrictEqual(await old.rows(), expected(6));
    await old.stamp(9);
    await old.space.audit();
  } finally {
    await old.close();
  }
  const again = await openDirectory(native, gate);
  try {
    deepStrictEqual(await again.rows(), expected(6));
    await again.insert([7]);
    await again.settle();
    deepStrictEqual(
      (await again.pages()).map((p) => p.groupCount),
      [8],
    );
    await again.space.audit();
  } finally {
    await again.close();
  }
  reach.reach("shared extent carry retired");
}, 120_000);

it("canonical all-bits tail carries into a full page across both writers", async () => {
  reach.beginHistory();
  const files = DirectoryFiles.load(await currentDirectoryFixture("packed-0")),
    locks = new SimLocks();
  const initial = await openDirectory(files, locks);
  try {
    await initial.insert(expected(4095));
    await initial.settle();
    deepStrictEqual(
      (await initial.pages()).map((p) => p.groupCount),
      partition(4095),
    );
  } finally {
    await initial.close();
  }
  const peer = await legacy.open(files, locks);
  try {
    deepStrictEqual(await peer.rows(), expected(4095));
    await peer.stamp(4095);
  } finally {
    await peer.close();
  }
  const current = await openDirectory(files, locks);
  try {
    deepStrictEqual(await current.rows(), expected(4095));
    expect(await current.takeStamp()).toBe(4096);
    await current.insert([4095]);
    await current.settle();
    deepStrictEqual(
      (await current.pages()).map((p) => p.groupCount),
      [4096],
    );
    await current.space.audit();
  } finally {
    await current.close();
  }
  const old = await legacy.open(files, locks);
  try {
    deepStrictEqual(await old.rows(), expected(4096));
    await old.insert([4096]);
    await old.settle();
  } finally {
    await old.close();
  }
  const next = await openDirectory(files, locks);
  try {
    deepStrictEqual(await next.rows(), expected(4097));
    await next.insert([4097]);
    await next.settle();
    await next.space.audit();
  } finally {
    await next.close();
  }
  const reopened = await legacy.open(files, locks);
  try {
    deepStrictEqual(await reopened.rows(), expected(4098));
  } finally {
    await reopened.close();
  }
  reach.reach("full-page carry");
  reach.reach("full-page plus tail");
}, 120_000);
it("reached current-fixture, shared-edit and boundary states", () => reach.assertReached());

it("the committed shard population covers every tail exactly once", async () => {
  const folder = new URL("./directory-compat/", import.meta.url);
  const names = (await readdir(folder)).filter((n) => n.endsWith(".test.ts")).sort();
  const firsts = Array.from({ length: 32 }, (_, n) => n * 128);
  deepStrictEqual(
    names,
    firsts.map((n) => `tails-${String(n).padStart(4, "0")}.test.ts`),
  );
  for (let n = 0; n < names.length; n++) {
    const source = await readFile(new URL(names[n] ?? "", folder), "utf8");
    expect(Number(/registerDirectoryTails\(\s*(\d+)/.exec(source)?.[1])).toBe(firsts[n]);
  }
});
