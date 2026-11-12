/** Exhaustive ordinary-open tails split into independent native stores. Prefix bootstrap is
 * one ordinary current-build transaction; no tail state or independent-current-writer continuation is skipped. */
import { deepStrictEqual } from "node:assert/strict";
import { afterAll, beforeAll, expect, it } from "vitest";
import type { Reach } from "../../testing/reach.js";
import { SimLocks } from "../storage/sim-disk.js";
import { openDirectory } from "./directory-compat-api.js";
import { DirectoryFiles } from "./directory-fixture-files.js";
import { directoryPeer, currentDirectoryFixture } from "./directory-current.test-support.js";
export const expected = (n: number) => Array.from({ length: n }, (_, k) => k);
export function partition(n: number): number[] {
  const counts: number[] = [];
  while (n >= 4096) {
    counts.push(4096);
    n -= 4096;
  }
  for (let bit = 2048; bit > 0; bit >>= 1) if ((n & bit) !== 0) counts.push(bit);
  return counts;
}
export function registerDirectoryTails(first: number, reach: Reach<string>) {
  let legacy: Awaited<ReturnType<typeof directoryPeer>> | undefined;
  let disk: DirectoryFiles;
  const locks = new SimLocks();
  const getBaseline = () => {
    if (legacy === undefined) throw new Error("Baseline not loaded");
    return legacy;
  };
  beforeAll(async () => {
    legacy = await directoryPeer();
    disk = DirectoryFiles.load(await currentDirectoryFixture("packed-0"));
    if (first > 0) {
      const db = await openDirectory(disk, locks);
      try {
        deepStrictEqual(await db.rows(), []);
        await db.insert(expected(first));
        await db.settle();
      } finally {
        await db.close();
      }
    }
  }, 60_000);
  afterAll(async () => {
    if (legacy !== undefined) await legacy.dispose();
  });
  for (let start = first; start < first + 128; start += 32)
    it(`ordinary independent-current-writer tails ${String(start)}..${String(start + 31)}`, async () => {
      for (let n = start; n < start + 32; n++) {
        reach.beginHistory();
        const current = await openDirectory(disk, locks);
        try {
          deepStrictEqual(await current.rows(), expected(n));
          await current.stamp(n);
          await current.settle();
          deepStrictEqual(
            (await current.pages()).map((p) => p.groupCount),
            partition(n),
          );
        } finally {
          await current.close();
        }
        expect(current.governor.usedBytes).toBe(0);
        const peer = await getBaseline().open(disk, locks);
        try {
          deepStrictEqual(await peer.rows(), expected(n));
          expect(await peer.takeStamp()).toBe(n + 1);
          await peer.insert([n]);
        } finally {
          await peer.close();
        }
        expect(peer.governor.usedBytes).toBe(0);
        reach.reach(`tail ${String(n)}`);
        reach.reach("current to peer continuation");
        reach.reach("peer to current continuation");
        reach.reach("every value preserved");
        reach.reach("exact cleanup");
      }
    }, 300_000);
  it("reopens the range's final peer write and audits its boundary carry", async () => {
    reach.beginHistory();
    const count = first + 128;
    const current = await openDirectory(disk, locks);
    try {
      deepStrictEqual(await current.rows(), expected(count));
      await current.settle();
      deepStrictEqual(
        (await current.pages()).map((p) => p.groupCount),
        partition(count),
      );
      await current.space.audit();
    } finally {
      await current.close();
    }
    expect(current.governor.usedBytes).toBe(0);
    const peer = await getBaseline().open(disk, locks);
    try {
      deepStrictEqual(await peer.rows(), expected(count));
    } finally {
      await peer.close();
    }
    expect(peer.governor.usedBytes).toBe(0);
    reach.reach("range end reopened");
  }, 300_000);
  it("reached every ordinary-open tail in this range", () => reach.assertReached());
  return { peer: getBaseline, files: () => disk, locks };
}
