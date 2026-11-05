/**
 * An expired spill owner swept by the next owner's creation takes its pages with it.
 *
 * Core 0.19.x swept the expired owner's record in `createTempOwner` and kept its pages in the
 * leader's temp-page ledger and on disk. The next checkpoint — a graceful close writes one —
 * captured pages with no owner, and every later open refused the database: "opfs storage
 * corruption at recovery: Temp page has no owner". It was measured in Chromium after a session
 * of killed and failed spilling queries. A create now removes the swept owners' pages in the same
 * frame, and an open reclaims the orphan pages a 0.19.x checkpoint already holds: they are spill
 * scratch no query can renew, so reclaiming them loses nothing.
 *
 * `format-fixtures/opfs-orphan-temp-pages.json` is such a database, frozen from core 0.19.1 by
 * `scripts/write-opfs-orphan-temp-fixture.mts`. The generated lifecycle campaign is
 * `../temp-owner-lifecycle.test.ts`.
 */
/* eslint-disable no-restricted-imports -- Node-only test reads a frozen fixture; not shipped. */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { decodeSyncCheckpoint, encodeSyncCheckpoint } from "../toolkit/wire.js";
import { StorageCorruptionError, type TempOwnerRecord } from "../types.js";
import { OpfsBlockStore } from "./index.js";

const iso = (ms: number): string => new Date(ms).toISOString();
const t0 = Date.parse("2026-11-01T00:00:00.000Z");
const page = (ownerId: string, runId: string, pageIndex: number, fill: number) => ({
  ownerId,
  runId,
  pageIndex,
  bytes: new Uint8Array(16).fill(fill),
});
const owner = (ownerId: string, createdAtMs: number, ttlMs = 60_000): TempOwnerRecord => ({
  ownerId,
  createdAt: iso(createdAtMs),
  expiresAt: iso(createdAtMs + ttlMs),
  revision: 0,
});

interface CheckpointShape {
  generation: number;
  tempPages: Array<{ ownerId: string; runId: string; pageIndex: number; length: number }>;
  core: { tempOwners: Array<{ ownerId: string }> };
}

function checkpoints(shim: MemoryOpfs, name: string): CheckpointShape[] {
  return ["checkpoint-a", "checkpoint-b"].flatMap((slot) => {
    const bytes = shim.readFileBytes(`minnowdb/${name}/${slot}`);
    return bytes === undefined || bytes.byteLength === 0
      ? []
      : [decodeSyncCheckpoint(bytes) as CheckpointShape];
  });
}

function orphanPages(checkpoint: CheckpointShape): string[] {
  const owners = new Set(checkpoint.core.tempOwners.map(({ ownerId }) => ownerId));
  return checkpoint.tempPages
    .filter(({ ownerId }) => !owners.has(ownerId))
    .map(({ ownerId, runId, pageIndex }) => `${ownerId}/${runId}/${String(pageIndex)}`);
}

describe("a create that sweeps an expired spill owner", () => {
  for (const ending of ["close", "crash", "checkpoint then crash"] as const) {
    it(`takes the owner's pages, and the database reopens after a ${ending}`, async () => {
      const shim = new MemoryOpfs();
      const name = crypto.randomUUID();
      const open = () =>
        OpfsBlockStore.open({
          name,
          root: shim.root,
          checkpointEntries: ending === "checkpoint then crash" ? 1 : 1_000_000,
        });
      const store = await open();
      await store.createTempOwner(owner("query-abandoned", t0));
      await store.putTempRunPages([
        page("query-abandoned", "run-0", 0, 1),
        page("query-abandoned", "run-0", 1, 2),
      ]);
      // After the abandoned owner expired: this create sweeps it.
      await store.createTempOwner(owner("query-next", t0 + 120_000));
      await store.putTempRunPage(page("query-next", "run-0", 0, 3));

      expect(await store.getTempOwner("query-abandoned")).toBeUndefined();
      expect(await store.getTempRunPage("query-abandoned", "run-0", 0)).toBeUndefined();
      expect(await store.getTempRunPage("query-abandoned", "run-0", 1)).toBeUndefined();
      expect(shim.readFileBytes(`minnowdb/${name}/temp/query-abandoned/run-0/0`)).toBeUndefined();
      expect((await store.listTempOwnerIdsPage(null, 16)).records).toEqual(["query-next"]);
      for (const checkpoint of checkpoints(shim, name)) expect(orphanPages(checkpoint)).toEqual([]);

      if (ending === "close") store.close();
      else store._crashForTests();
      const reopened = await open();
      expect(await reopened.getTempOwner("query-next")).toEqual(owner("query-next", t0 + 120_000));
      expect(await reopened.getTempRunPage("query-next", "run-0", 0)).toEqual(
        page("query-next", "run-0", 0, 3).bytes,
      );
      expect(await reopened.getTempRunPage("query-abandoned", "run-0", 0)).toBeUndefined();
      expect(await reopened.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
      reopened.close();
    });
  }
});

interface Fixture {
  layoutFormatVersion: number;
  writerPackageVersion: string;
  files: Record<string, string>;
}

const FIXTURE = JSON.parse(
  readFileSync(
    new URL("../../../format-fixtures/opfs-orphan-temp-pages.json", import.meta.url),
    "utf8",
  ),
) as Fixture;
const NAME = "orphan-fixture";
const PREFIX = `minnowdb/${NAME}`;
const ORPHAN_FILES = [
  `${PREFIX}/temp/query-abandoned/run-0/0`,
  `${PREFIX}/temp/query-abandoned/run-0/1`,
];
const LIVE_OWNER = owner("query-live", t0 + 120_000);

function hydrate(): MemoryOpfs {
  const shim = new MemoryOpfs();
  for (const [path, base64] of Object.entries(FIXTURE.files)) {
    shim.writeFileBytes(
      path,
      Uint8Array.from(atob(base64), (character) => character.charCodeAt(0)),
    );
  }
  return shim;
}

async function rows(store: OpfsBlockStore): Promise<unknown[]> {
  const database = new MinnowDatabase(store, { now: () => new Date(t0 + 200_000) });
  return (await database.query("SELECT id, note FROM kept ORDER BY id")).rows;
}

/**
 * Everything the fixture holds that is not scratch, plus the orphan pages being gone. An open
 * whose directory delete failed may leave the orphan owner's empty directory; spill cleanup lists
 * it as an ownerless owner and reclaims it, as it does any physical orphan.
 */
async function expectRecovered(
  store: OpfsBlockStore,
  shim: MemoryOpfs,
  { leftoverDirectory = false } = {},
): Promise<void> {
  expect(await rows(store)).toEqual([
    { id: 1, note: "first" },
    { id: 2, note: "second" },
    { id: 3, note: "third" },
  ]);
  expect(await store.getTempOwner("query-live")).toEqual(LIVE_OWNER);
  expect(await store.getTempRunPage("query-live", "run-0", 0)).toEqual(
    page("query-live", "run-0", 0, 3).bytes,
  );
  expect(await store.getTempOwner("query-abandoned")).toBeUndefined();
  expect(await store.getTempRunPage("query-abandoned", "run-0", 0)).toBeUndefined();
  expect(await store.getTempRunPage("query-abandoned", "run-0", 1)).toBeUndefined();
  if (leftoverDirectory) {
    const listed = (await store.listTempOwnerIdsPage(null, 16)).records;
    if (listed.includes("query-abandoned")) {
      expect(await store.removeTempOwnerIfExpired("query-abandoned", iso(t0))).toBe(true);
    }
  }
  expect((await store.listTempOwnerIdsPage(null, 16)).records).toEqual(["query-live"]);
  for (const path of ORPHAN_FILES) expect(shim.readFileBytes(path), path).toBeUndefined();
  expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true, issueCount: 0 });
}

describe(`a database frozen from @minnowdb/core@${FIXTURE.writerPackageVersion} with orphan temp pages`, () => {
  it("holds the state 0.19.x refused: a checkpoint with pages of an owner it does not record", () => {
    const shim = hydrate();
    expect(checkpoints(shim, NAME).flatMap(orphanPages)).toEqual(
      expect.arrayContaining(["query-abandoned/run-0/0", "query-abandoned/run-0/1"]),
    );
    for (const path of ORPHAN_FILES) expect(shim.readFileBytes(path), path).toBeDefined();
  });

  it("opens, keeps every row and the live owner's page, and reclaims the orphans", async () => {
    const shim = hydrate();
    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    await expectRecovered(store, shim);
    // The reclaimed ledger is published: both slots hold it, so no later open meets the orphans.
    const published = checkpoints(shim, NAME);
    expect(published).toHaveLength(2);
    for (const checkpoint of published) expect(orphanPages(checkpoint)).toEqual([]);
    store.close();
  });

  it("continues writing, then reopens with the current build after a crash", async () => {
    const shim = hydrate();
    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    const database = new MinnowDatabase(store, { now: () => new Date(t0 + 300_000) });
    await database.insertBatch("kept", { columns: { id: [4], note: ["fourth"] } });
    // The live owner expires; the next spill's create sweeps it, pages and all.
    await store.createTempOwner(owner("query-later", t0 + 300_000));
    await store.putTempRunPage(page("query-later", "run-0", 0, 4));
    expect(await store.getTempRunPage("query-live", "run-0", 0)).toBeUndefined();
    store._crashForTests();

    for (let open = 0; open < 2; open += 1) {
      const reopened = await OpfsBlockStore.open({ name: NAME, root: shim.root });
      expect((await rows(reopened)).map((row) => (row as { id: number }).id)).toEqual([1, 2, 3, 4]);
      expect((await reopened.listTempOwnerIdsPage(null, 16)).records).toEqual(["query-later"]);
      expect(await reopened.getTempRunPage("query-later", "run-0", 0)).toEqual(
        page("query-later", "run-0", 0, 4).bytes,
      );
      expect(await reopened.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
      reopened.close();
    }
  });

  it("recovers when the open that reclaims the orphans is interrupted at any write or delete, then retried", async () => {
    // Count the file writes, flushes and deletes a clean open of the fixture performs.
    const steps: string[] = [];
    {
      const shim = hydrate();
      shim.setWriteFault((path, phase) => {
        if (phase !== "create") steps.push(`${phase} ${path}`);
      });
      shim.setDeleteFault((path) => {
        steps.push(`delete ${path}`);
      });
      const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
      store.close();
    }
    expect(steps.some((step) => step.startsWith("write") && step.includes("checkpoint-"))).toBe(
      true,
    );
    expect(
      steps.some((step) => step.startsWith("delete") && step.includes("query-abandoned")),
    ).toBe(true);
    for (let failAt = 0; failAt < steps.length; failAt += 1) {
      const shim = hydrate();
      let seen = 0;
      const fail = (): void => {
        if (seen++ === failAt) throw new Error(`power lost at ${steps[failAt] ?? ""}`);
      };
      shim.setWriteFault((_path, phase) => {
        if (phase !== "create") fail();
      });
      shim.setDeleteFault(fail);
      try {
        const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
        store._crashForTests();
      } catch {
        // The interrupted open may refuse; the retry below must still succeed.
      }
      shim.setWriteFault(null);
      shim.setDeleteFault(null);
      const retried = await OpfsBlockStore.open({ name: NAME, root: shim.root });
      await expectRecovered(retried, shim, { leftoverDirectory: true });
      retried.close();
    }
  });

  it("serves concurrent openers the same recovered state", async () => {
    const shim = hydrate();
    const [first, second] = await Promise.all([
      OpfsBlockStore.open({ name: NAME, root: shim.root }),
      OpfsBlockStore.open({ name: NAME, root: shim.root }),
    ]);
    await expectRecovered(first, shim);
    await expectRecovered(second, shim);
    second.close();
    first.close();
  });

  it("still refuses a malformed orphan page record and an unreadable checkpoint", async () => {
    // Reclaiming orphans drops well-formed scratch records only; a damaged ledger is corruption.
    const malformed = hydrate();
    for (const slot of ["checkpoint-a", "checkpoint-b"]) {
      const path = `${PREFIX}/${slot}`;
      const bytes = malformed.readFileBytes(path);
      if (bytes === undefined || bytes.byteLength === 0) continue;
      const state = decodeSyncCheckpoint(bytes) as CheckpointShape;
      for (const record of state.tempPages) {
        if (record.ownerId === "query-abandoned") record.length = -1;
      }
      malformed.writeFileBytes(path, encodeSyncCheckpoint(state));
    }
    await expect(OpfsBlockStore.open({ name: NAME, root: malformed.root })).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );

    const unreadable = hydrate();
    for (const slot of ["checkpoint-a", "checkpoint-b"]) {
      const path = `${PREFIX}/${slot}`;
      if ((unreadable.readFileBytes(path)?.byteLength ?? 0) > 16)
        unreadable.corruptFileByte(path, 16);
    }
    await expect(OpfsBlockStore.open({ name: NAME, root: unreadable.root })).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
  });
});
