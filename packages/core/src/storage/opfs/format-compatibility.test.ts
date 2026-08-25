/* eslint-disable no-restricted-imports -- Node-only test discovers every frozen fixture; this file is not shipped. */
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { decodeBlock, encodeBlock } from "../../block-format/index.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { decodeSyncCheckpoint, LOG_FORMAT_VERSION } from "../toolkit/wire.js";
import type { TableRecord } from "../types.js";
import { OpfsBlockStore } from "./index.js";

interface NativeFixture {
  layoutFormatVersion: number;
  files: Record<string, string>;
  expectations: {
    tables: string[];
    blockId: string;
    blockValues: Array<string | null>;
  };
}

interface Fixture {
  stem: string;
  fixture: NativeFixture;
}

const FIXTURE_DIRECTORY = new URL("../../../format-fixtures/", import.meta.url);

function loadFixtures(): Fixture[] {
  const directory = fileURLToPath(FIXTURE_DIRECTORY);
  return readdirSync(directory)
    .filter((name) => /^opfs-layout[0-9]+\.json$/.test(name))
    .sort()
    .map((name) => ({
      stem: name.slice(0, -".json".length),
      fixture: JSON.parse(readFileSync(new URL(name, FIXTURE_DIRECTORY), "utf8")) as NativeFixture,
    }));
}

const fixtures = loadFixtures();
const FIRST_STABLE_OPFS_LAYOUT_VERSION = 5;

function decodeBase64(base64: string): Uint8Array {
  return Uint8Array.from(atob(base64), (character) => character.charCodeAt(0));
}

function hydrate(fixture: NativeFixture): MemoryOpfs {
  const shim = new MemoryOpfs();
  for (const [path, base64] of Object.entries(fixture.files)) {
    shim.writeFileBytes(path, decodeBase64(base64));
  }
  return shim;
}

function table(name: string): TableRecord {
  return {
    id: `continued-${name}`,
    name,
    managed: false,
    columns: [{ id: "id", name: "id", type: "number", nullable: false }],
    revision: 0,
    createdAt: "2026-08-24T13:00:00.000Z",
  };
}

describe("frozen native OPFS layout", () => {
  it("retains exactly one fixture for every locked layout", () => {
    const expectedVersions = Array.from(
      { length: LOG_FORMAT_VERSION - FIRST_STABLE_OPFS_LAYOUT_VERSION + 1 },
      (_, index) => FIRST_STABLE_OPFS_LAYOUT_VERSION + index,
    );
    expect(
      fixtures
        .map(({ fixture }) => fixture.layoutFormatVersion)
        .sort((left, right) => left - right),
    ).toEqual(expectedVersions);
  });

  it("has a fixture for the layout this build writes", () => {
    expect(fixtures.length).toBeGreaterThan(0);
    const current = fixtures.find(
      ({ fixture }) => fixture.layoutFormatVersion === LOG_FORMAT_VERSION,
    );
    expect(
      current,
      `No native OPFS fixture covers layout ${String(LOG_FORMAT_VERSION)}. Freeze the current ` +
        `writer before changing it, and retain every prior locked fixture.`,
    ).toBeDefined();
    const fixture = current?.fixture;
    if (fixture === undefined) throw new Error("Current native OPFS fixture is missing");
    expect(Object.keys(fixture.files)).toEqual(
      expect.arrayContaining([
        "minnowdb/native-fixture/format.json",
        "minnowdb/native-fixture/wal",
        "minnowdb/native-fixture/checkpoint-b",
        "minnowdb/native-fixture/extents/000000",
      ]),
    );
    const checkpointBytes = fixture.files["minnowdb/native-fixture/checkpoint-b"];
    expect(checkpointBytes).toBeDefined();
    const checkpoint = decodeSyncCheckpoint(decodeBase64(checkpointBytes ?? ""));
    expect(checkpoint).toMatchObject({ core: { catalogEpoch: 3, schemaEpoch: 2 } });
  });

  for (const { stem, fixture } of fixtures) {
    it(`reopens ${stem}'s checkpoint, WAL tail, and extent bytes`, async () => {
      const store = await OpfsBlockStore.open({
        name: "native-fixture",
        root: hydrate(fixture).root,
      });
      expect((await store.listTables()).map(({ name }) => name)).toEqual(
        fixture.expectations.tables,
      );
      const block = await store.getBlock(fixture.expectations.blockId);
      expect(block).toBeDefined();
      expect((await decodeBlock(block ?? new Uint8Array())).column.values).toEqual(
        fixture.expectations.blockValues,
      );
      store.close();
    });

    it(`continues writing and recovering from ${stem}`, async () => {
      const shim = hydrate(fixture);
      const store = await OpfsBlockStore.open({ name: "native-fixture", root: shim.root });
      await store.addTable(table("after-fixture"));
      const block = await encodeBlock({ type: "number", values: [10, 20] }, "raw");
      const begun = await store.beginTransaction({
        record: {
          id: "continued-transaction",
          ownerId: "continued-owner",
          expiresAt: "2026-08-24T14:00:00.000Z",
          pendingBlockIds: [],
          pendingSegmentIds: [],
          status: "active",
          revision: 0,
          startedAt: "2026-08-24T13:00:00.000Z",
          updatedAt: "2026-08-24T13:00:00.000Z",
          committedVersion: null,
        },
      });
      await store.stageTransactionArtifacts({
        transactionId: begun.record.id,
        expectedRevision: begun.record.revision,
        blocks: [{ id: "continued-block", bytes: block }],
        segments: [],
        updatedAt: "2026-08-24T13:00:01.000Z",
      });
      store._crashForTests();

      const reopened = await OpfsBlockStore.open({ name: "native-fixture", root: shim.root });
      expect((await reopened.listTables()).map(({ name }) => name)).toEqual(
        [...fixture.expectations.tables, "after-fixture"].sort(),
      );
      expect(await reopened.getBlock("continued-block")).toEqual(block);
      reopened.close();
    });
  }
});
