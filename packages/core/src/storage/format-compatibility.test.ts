/**
 * Golden compatibility tests for the v1 storage contract. There was no pre-v1 persisted-data
 * contract, so the corpus intentionally starts with block format 2 and framed snapshot format 1.
 * From this lock onward, every shipped format stays readable and keeps answering the same fixed
 * queries. A new writer format must add a fixture before changing the Minnow-owned framing or
 * logical payload.
 */
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  BLOCK_FORMAT_VERSION,
  crc32,
  decodePhysicalBlock,
  inspectBlock,
} from "../block-format/index.js";
import { MinnowDatabase } from "../engine/database.js";
import { MemoryBlockStore } from "./memory.js";
import {
  decodeSnapshotFrameStream,
  encodeSnapshotFrameStreamFooter,
  encodeSnapshotFrameStreamHeader,
  extendSnapshotFrameStreamChecksum,
  readSnapshotSummary,
  SNAPSHOT_FORMAT_VERSION,
  snapshotFrameEnvelopeParts,
} from "./snapshot.js";
import {
  SNAPSHOT_FRAME_KINDS,
  type SnapshotFrame,
  type SnapshotFrameStreamHeader,
} from "./types.js";
import {
  buildFixtureDatabase,
  createFormatFixtureArtifact,
  FIXTURE_QUERIES,
} from "./fixture-shape.js";

const FIXTURE_DIRECTORY = new URL("../../format-fixtures/", import.meta.url);

interface FixtureManifest {
  blockFormatVersion: number;
  snapshotFormatVersion: number;
  writerPackageVersion: string;
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

function packageVersionTuple(version: string): [number, number, number] {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:-|$)/u.exec(version);
  if (match === null) throw new TypeError(`Invalid fixture package version: ${version}`);
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

const currentPackageVersion = (
  JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8")) as {
    version: string;
  }
).version;

function comparePackageVersions(left: string, right: string): number {
  const leftParts = packageVersionTuple(left);
  const rightParts = packageVersionTuple(right);
  return (
    leftParts[0] - rightParts[0] || leftParts[1] - rightParts[1] || leftParts[2] - rightParts[2]
  );
}

async function restoreFixture(bytes: Uint8Array): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore());
  await database.importSnapshot(bytes);
  return database;
}

async function fixtureBlockPayloads(bytes: Uint8Array): Promise<Uint8Array[]> {
  const payloads: Uint8Array[] = [];
  for await (const entry of decodeSnapshotFrameStream(
    (async function* (): AsyncGenerator<Uint8Array> {
      yield bytes;
    })(),
  )) {
    if (entry.type === "frame" && entry.frame.kind === "block") {
      payloads.push(entry.frame.payload);
    }
  }
  return payloads;
}

/**
 * Native CompressionStream implementations are free to emit different valid deflate streams.
 * Strip only those native bytes and their derived sizes/checksums; every Minnow-owned envelope
 * field, frame, ordering decision, and uncompressed physical payload remains byte-comparable.
 */
async function canonicalBlockPayload(bytes: Uint8Array): Promise<Uint8Array> {
  const physical = await decodePhysicalBlock(bytes);
  const description = physical.description;
  if (description.compression === "raw") return bytes;

  // Preserve the complete block envelope and metadata. Replace only the native gzip stream with
  // the verified logical payload, then canonicalize the stored length/checksum fields derived
  // from that stream. The retained codec ID means changing gzip/raw policy still changes this
  // artifact even though the comparison payload is deliberately not itself a decodable block.
  const payloadOffset = description.headerLength + description.metadataLength;
  const canonical = new Uint8Array(payloadOffset + physical.column.bytes.byteLength);
  canonical.set(bytes.subarray(0, payloadOffset));
  canonical.set(physical.column.bytes, payloadOffset);
  const view = new DataView(canonical.buffer);
  view.setUint32(32, physical.column.bytes.byteLength, true);
  view.setUint32(40, description.checksum, true);
  view.setUint32(4, crc32(canonical.subarray(8, payloadOffset)), true);
  return canonical;
}

function concatenate(chunks: readonly Uint8Array[]): Uint8Array {
  const byteLength = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  const bytes = new Uint8Array(byteLength);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

async function canonicalWriterArtifact(bytes: Uint8Array): Promise<Uint8Array> {
  let header: SnapshotFrameStreamHeader | undefined;
  const frames: SnapshotFrame[] = [];
  for await (const entry of decodeSnapshotFrameStream(
    (async function* (): AsyncGenerator<Uint8Array> {
      yield bytes;
    })(),
  )) {
    if (entry.type === "header") {
      header = entry.header;
      continue;
    }
    if (entry.type === "footer") continue;
    const { frame } = entry;
    const payload =
      frame.kind === "block" ? await canonicalBlockPayload(frame.payload) : frame.payload;
    frames.push({
      ...frame,
      payload,
      checksum: crc32(payload),
    });
  }
  if (header === undefined) throw new Error("Snapshot header is missing");

  const kinds = Object.fromEntries(
    SNAPSHOT_FRAME_KINDS.map((kind) => [
      kind,
      {
        frameCount: header.kinds[kind].frameCount,
        itemCount: header.kinds[kind].itemCount,
        storedBytes: frames
          .filter((frame) => frame.kind === kind)
          .reduce((total, frame) => total + frame.payload.byteLength, 0),
      },
    ]),
  ) as SnapshotFrameStreamHeader["kinds"];
  const canonicalHeader: SnapshotFrameStreamHeader = { ...header, kinds };
  const chunks: Uint8Array[] = [encodeSnapshotFrameStreamHeader(canonicalHeader)];
  let checksum = 0;
  let itemCount = 0;
  let storedBytes = 0;
  for (const frame of frames) {
    const parts = snapshotFrameEnvelopeParts(frame);
    chunks.push(...parts);
    checksum = extendSnapshotFrameStreamChecksum(checksum, parts);
    itemCount += frame.itemCount;
    storedBytes += frame.payload.byteLength;
  }
  chunks.push(
    encodeSnapshotFrameStreamFooter({
      frameCount: frames.length,
      itemCount,
      storedBytes,
      checksum,
    }),
  );
  return concatenate(chunks);
}

describe("v1 format compatibility", () => {
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
      expect(
        comparePackageVersions(fixture.manifest.writerPackageVersion, currentPackageVersion),
        `${fixture.stem} claims a writer newer than this package`,
      ).toBeLessThanOrEqual(0);
      expect(fixture.manifest.expectations.length, `${fixture.stem} has no expectations`).toBe(
        FIXTURE_QUERIES.length,
      );
    }
  });

  it("keeps the current v1 writer canonical across native gzip encoders", async () => {
    const frozen = fixtures.find(
      ({ manifest }) =>
        manifest.blockFormatVersion === BLOCK_FORMAT_VERSION &&
        manifest.snapshotFormatVersion === SNAPSHOT_FORMAT_VERSION,
    );
    if (frozen === undefined) throw new Error("Current format fixture is missing");
    const generated = await createFormatFixtureArtifact();
    expect(await canonicalWriterArtifact(generated.bytes)).toEqual(
      await canonicalWriterArtifact(frozen.bytes),
    );
    expect(normalize(generated.expectations)).toEqual(normalize(frozen.manifest.expectations));
  });

  for (const fixture of fixtures) {
    it(`keeps ${fixture.stem}'s canonical framed header`, async () => {
      const summary = await readSnapshotSummary(fixture.bytes);
      expect(summary.formatVersion).toBe(fixture.manifest.snapshotFormatVersion);
      expect(summary.blockCount).toBeGreaterThan(0);
      expect(summary.byteLength).toBe(fixture.bytes.byteLength);
    });

    it(`reads ${fixture.stem} from @minnowdb/core@${fixture.manifest.writerPackageVersion}`, async () => {
      const database = await restoreFixture(fixture.bytes);

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

    it(`accepts current writes after loading ${fixture.stem} from @minnowdb/core@${fixture.manifest.writerPackageVersion}`, async () => {
      // Reading an old database is half the guarantee. An application that opens one goes on to
      // write to it, so the restored catalog -- row-id counters, unique-key membership, segment
      // ordering -- has to be sound rather than merely readable.
      const database = await restoreFixture(fixture.bytes);

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
    const blocks = await fixtureBlockPayloads(fixtures[0]?.bytes ?? new Uint8Array());
    expect(blocks.length).toBeGreaterThan(0);
    // Every block, not just the first: which block a given query happens to open depends on the
    // table and the pruning, and a tampered block the read never reaches proves nothing.
    for (const bytes of blocks) {
      const tampered = bytes.slice();
      // The version sits at byte 8, little-endian uint16. It is checked before the envelope
      // checksum, so a downgrade gets a version error rather than a misleading corruption error.
      new DataView(tampered.buffer, tampered.byteOffset, tampered.byteLength).setUint16(
        8,
        BLOCK_FORMAT_VERSION + 1,
        true,
      );
      expect(() => inspectBlock(tampered)).toThrow(/Unsupported block version/);
    }
  });
});
