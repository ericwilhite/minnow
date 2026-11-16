/** Native fencing identities are monotonic and name their exact authority, never only an epoch. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, OWNER_IDENTITY_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { DURABLE_READER_VERSION, DURABLE_WRITER_VERSION } from "../durable-format.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { OwnershipLostError, StoreCorruptionError } from "./contract.js";
import {
  decodeOwnerIdentity,
  encodeOwnerIdentity,
  identityOwnerName,
  identityFromNames,
  identityGateName,
  nextIdentity,
  requireIdentity,
  storedIdentity,
} from "./owner-identity.js";

const reach = createReach("durable owner identity", OWNER_IDENTITY_REACH);
afterAll(() => reach.assertReached());

for (const seed of seedsFor("native-owner-identity", [0xb2cf, 0xb2d0]))
  it(`round trips exact counters and rejects a different expected authority seed ${String(seed)}`, () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: Number.MAX_SAFE_INTEGER - 1024 }),
        fc.integer({ min: 0, max: 1024 }),
        fc.string(),
        (epoch, extra, name) => {
          reach.beginHistory();
          const identity = {
            epoch,
            generation: epoch + extra,
            reader: DURABLE_READER_VERSION,
            writer: DURABLE_WRITER_VERSION,
          };
          const bytes = encodeOwnerIdentity(identity),
            decoded = decodeOwnerIdentity(bytes),
            view = new DataView(bytes.buffer);
          expect(bytes.length).toBe(40);
          expect(view.getUint32(12, true)).toBe(xxHash32(bytes, 0, 0, 12));
          expect(view.getUint32(36, true)).toBe(xxHash32(bytes, 0, 0, 36));
          expect(view.getUint32(32, true)).toBe(0);
          expect(decoded).toEqual(identity);
          reach.reach(view.getUint32(8, true) === 0 ? "single-word epoch" : "multi-word epoch");
          reach.reach("both checksums and zero reserved");
          if (decoded === null) throw new Error("missing identity");
          expect(encodeOwnerIdentity(decoded)).toEqual(bytes);
          requireIdentity(decoded, identity);
          for (const field of ["epoch", "generation", "reader", "writer"] as const) {
            const wrong = { ...identity, [field]: identity[field] + 1 };
            expect(() => requireIdentity(wrong, identity)).toThrow(OwnershipLostError);
            expect(identityOwnerName(name, wrong)).not.toBe(identityOwnerName(name, identity));
            reach.reach("all identity fields affect authority");
          }
          const provisional = { ...identity, generation: epoch + 1 },
            winner = { ...identity, epoch: epoch + 1, generation: epoch + 1 };
          const provisionalKey = identityOwnerName(name, provisional),
            winnerKey = identityOwnerName(name, winner);
          for (const order of [
            [provisionalKey, winnerKey],
            [winnerKey, provisionalKey],
          ])
            expect(identityFromNames(name, order)).toEqual(winner);
          reach.reach("hint ties use higher epoch");
          expect(identityGateName(name, identity)).not.toBe(identityOwnerName(name, identity));
          expect(identityOwnerName(`${name}:1`, identity)).not.toBe(
            identityOwnerName(name, identity),
          );
          if (identity.generation < Number.MAX_SAFE_INTEGER) {
            expect(nextIdentity(identity, false)).toEqual({
              ...identity,
              generation: identity.generation + 1,
            });
            reach.reach("same epoch recovery");
            reach.reach("takeover advances epoch");
            expect(nextIdentity(identity, true)).toEqual({
              ...identity,
              epoch: epoch + 1,
              generation: identity.generation + 1,
            });
          }
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [1, 0, ":"],
          [2 ** 32 + 17, 65, "minnow-owner:9:"],
        ],
      },
    );
  });

it("refuses exhausted generations rather than wrapping or reusing an owner name", () => {
  for (const epoch of [1, Number.MAX_SAFE_INTEGER]) {
    const identity = {
      epoch,
      generation: Number.MAX_SAFE_INTEGER,
      reader: DURABLE_READER_VERSION,
      writer: DURABLE_WRITER_VERSION,
    };
    for (const takeover of [false, true])
      expect(() => nextIdentity(identity, takeover)).toThrow(StoreCorruptionError);
  }
});

it("refuses either future capability with the unsupported-format type", () => {
  for (const offset of [24, 28]) {
    const bytes = encodeOwnerIdentity({
      epoch: 1,
      generation: 1,
      reader: DURABLE_READER_VERSION,
      writer: DURABLE_WRITER_VERSION,
    });
    const view = new DataView(bytes.buffer);
    view.setUint32(offset, view.getUint32(offset, true) + 1, true);
    view.setUint32(36, xxHash32(bytes, 0, 0, 36), true);
    expect(() => decodeOwnerIdentity(bytes)).toThrow(
      expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }),
    );
    for (const extra of [0, 1, 8]) {
      const extended = new Uint8Array(bytes.length + extra);
      extended.set(bytes);
      expect(() => storedIdentity(extended.buffer)).toThrow(
        expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }),
      );
    }
  }
});
