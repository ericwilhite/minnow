/**
 * Every database an earlier build wrote must still read.
 *
 * This is the one guarantee a browser database cannot recover from breaking. The data lives in
 * the user's own IndexedDB, so it survives every deploy — there is no migration window, no
 * maintenance mode, and no way to reach back and rewrite it. If a format change makes yesterday's
 * blocks unreadable, the first anyone hears is a user whose application will not open.
 *
 * `packages/core/format-fixtures/` holds one snapshot per released format version, each frozen by
 * the build that produced it, together with the answers that build gave to a fixed set of
 * queries. This runs all of them against the current engine. Two things can fail here, and they
 * mean different things:
 *
 *   - a fixture no longer opens, or answers differently → the change breaks existing databases
 *   - no fixture covers the current version pair → the change is unprotected, freeze it first
 *
 * The second is the one that matters most, because it fires *before* the damage. When it does,
 * check out the previous release, run `npm run fixture:format`, commit the fixture, and only then
 * make the format change.
 */
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { BLOCK_FORMAT_VERSION } from "../block-format/index.js";
import { MinnowDatabase } from "../engine/database.js";
import { MemoryBlockStore } from "./memory.js";
import { decodeSnapshot, SNAPSHOT_FORMAT_VERSION } from "./snapshot.js";
import { buildFixtureDatabase, FIXTURE_QUERIES } from "./fixture-shape.js";

const FIXTURE_DIRECTORY = new URL("../../format-fixtures/", import.meta.url);

interface FixtureManifest {
  blockFormatVersion: number;
  snapshotFormatVersion: number;
  expectations: Array<{ sql: string; rows: unknown }>;
}

interface Fixture {
  stem: string;
  bytes: Uint8Array;
  manifest: FixtureManifest;
}

function loadFixtures(): Fixture[] {
  const directory = fileURLToPath(FIXTURE_DIRECTORY);
  return readdirSync(directory)
    .filter((name) => name.endsWith(".minnow"))
    .sort()
    .map((name) => {
      const stem = name.slice(0, -".minnow".length);
      return {
        stem,
        bytes: new Uint8Array(readFileSync(new URL(name, FIXTURE_DIRECTORY))),
        manifest: JSON.parse(
          readFileSync(new URL(`${stem}.json`, FIXTURE_DIRECTORY), "utf8"),
        ) as FixtureManifest,
      };
    });
}

/**
 * Dates decode as `Date`, and the recorded answers are JSON, where a Date is an ISO string.
 * Normalizing both sides the same way compares what the values *are* rather than how they
 * happened to serialize.
 */
function normalize(rows: unknown): unknown {
  return JSON.parse(JSON.stringify(rows));
}

const fixtures = loadFixtures();

describe("format compatibility with databases earlier builds wrote", () => {
  it("has a fixture for the format versions this build writes", () => {
    // The ratchet. A format change with no fixture behind it means the previous format is no
    // longer represented anywhere, and nothing will notice when it stops being readable.
    const covered = fixtures.some(
      ({ manifest }) =>
        manifest.blockFormatVersion === BLOCK_FORMAT_VERSION &&
        manifest.snapshotFormatVersion === SNAPSHOT_FORMAT_VERSION,
    );
    expect(
      covered,
      `No fixture covers block format ${String(BLOCK_FORMAT_VERSION)} / snapshot format ` +
        `${String(SNAPSHOT_FORMAT_VERSION)}. Run "npm run fixture:format" and commit the result. ` +
        `If you are about to change a format version, freeze the current one first: the fixture ` +
        `can only be produced by the build that writes it.`,
    ).toBe(true);
  });

  it("keeps at least one fixture on file", () => {
    // Guards the guard: an empty directory would make every case below vacuous while still
    // reporting green.
    expect(fixtures.length).toBeGreaterThan(0);
    for (const fixture of fixtures) {
      expect(fixture.manifest.expectations.length, `${fixture.stem} has no expectations`).toBe(
        FIXTURE_QUERIES.length,
      );
    }
  });

  for (const fixture of fixtures) {
    it(`reads ${fixture.stem} and answers exactly as that build did`, async () => {
      const snapshot = await decodeSnapshot(fixture.bytes);
      const database = new MinnowDatabase(MemoryBlockStore.fromSnapshot(snapshot));

      const failures: string[] = [];
      for (const expectation of fixture.manifest.expectations) {
        let actual: unknown;
        try {
          actual = (await database.query(expectation.sql, { memoize: false })).rows;
        } catch (error) {
          failures.push(`${expectation.sql}\n  threw: ${String(error)}`);
          continue;
        }
        const got = JSON.stringify(normalize(actual));
        const want = JSON.stringify(normalize(expectation.rows));
        if (got !== want) failures.push(`${expectation.sql}\n  now:  ${got}\n  then: ${want}`);
      }

      if (failures.length > 0) {
        expect.fail(
          `${String(failures.length)} of ${String(fixture.manifest.expectations.length)} ` +
            `answers changed since ${fixture.stem} was written:\n\n${failures.join("\n\n")}`,
        );
      }
    });

    it(`accepts writes into ${fixture.stem} after loading it`, async () => {
      // Reading an old database is half the guarantee. An application that opens one goes on to
      // write to it, so the restored catalog -- row-id counters, unique-key membership, segment
      // ordering -- has to be sound rather than merely readable.
      const snapshot = await decodeSnapshot(fixture.bytes);
      const database = new MinnowDatabase(MemoryBlockStore.fromSnapshot(snapshot));

      const before = (await database.query("SELECT COUNT(*) AS n FROM records", { memoize: false }))
        .rows[0] as { n: number };
      await database.insertBatch("records", [
        {
          id: 100_001,
          region: "west",
          note: "added after loading",
          noise: "zzzz",
          amount: 1,
          active: true,
          seen_at: new Date("2026-08-18T00:00:00.000Z"),
          maybe_text: null,
          maybe_number: null,
          maybe_flag: null,
          maybe_time: null,
        },
      ]);
      const after = (await database.query("SELECT COUNT(*) AS n FROM records", { memoize: false }))
        .rows[0] as { n: number };
      expect(after.n).toBe(before.n + 1);

      // The restored unique-key membership must still reject a duplicate of a row that came out
      // of the fixture rather than out of this test.
      await expect(
        database.insertBatch("records", [
          {
            id: 1,
            region: "west",
            note: "duplicate",
            noise: "zzzz",
            amount: 1,
            active: true,
            seen_at: new Date("2026-08-18T00:00:00.000Z"),
            maybe_text: null,
            maybe_number: null,
            maybe_flag: null,
            maybe_time: null,
          },
        ]),
      ).rejects.toThrow();

      // And an update through the restored delta history must land.
      await database.execute("UPDATE records SET amount = ? WHERE id = ?", [77, 2]);
      expect(
        (await database.query("SELECT amount FROM records WHERE id = 2", { memoize: false })).rows,
      ).toEqual([{ amount: 77 }]);
    });
  }

  it("still produces the fixture shape it records, so the questions stay answerable", async () => {
    // If the engine stops being able to *build* the fixture database -- a column type removed, a
    // statement no longer accepted -- then no future fixture can be generated, and the corpus
    // silently stops growing. Better to hear it here than at the next format change.
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      rowsPerBlock: 64,
      autoCompact: false,
    });
    await buildFixtureDatabase(database);
    for (const sql of FIXTURE_QUERIES) {
      await expect(
        database.query(sql, { memoize: false }),
        `the fixture shape can no longer answer: ${sql}`,
      ).resolves.toBeDefined();
    }
  });

  it("rejects a block whose version it does not know", async () => {
    // The other half of the contract: an unknown *future* version must be refused clearly rather
    // than misread. A user who downgrades the application should see a plain error.
    const snapshot = await decodeSnapshot(fixtures[0]?.bytes ?? new Uint8Array());
    expect(snapshot.blocks.length).toBeGreaterThan(0);
    // Every block, not just the first: which block a given query happens to open depends on the
    // table and the pruning, and a tampered block the read never reaches proves nothing.
    const store = MemoryBlockStore.fromSnapshot({
      ...snapshot,
      blocks: snapshot.blocks.map(({ id, bytes }) => {
        const tampered = bytes.slice();
        // The version sits at byte 8, little-endian uint16 -- see the block header layout. It is
        // checked before the envelope checksum, so this reads back as a version error rather
        // than as the corruption the edit also causes.
        new DataView(tampered.buffer).setUint16(8, BLOCK_FORMAT_VERSION + 1, true);
        return { id, bytes: tampered };
      }),
    });
    const database = new MinnowDatabase(store);
    // A query that has to decode column payloads. `COUNT(*)` would not: it answers from segment
    // metadata without opening a single block, so it reads a tampered database quite happily.
    await expect(
      database.query("SELECT note, noise, amount FROM records ORDER BY id", { memoize: false }),
    ).rejects.toThrow(/Unsupported block version/);
  });
});
