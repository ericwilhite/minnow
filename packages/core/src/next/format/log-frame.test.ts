import { describe, expect, it } from "vitest";
import {
  LogCorruptionError,
  decodeLogHeader,
  encodeFrame,
  encodeLogHeader,
  frameLength,
  newestLogHeader,
  scanLog,
  type LogHeader,
} from "./log-frame.js";

const SALT = 0xabcd_1234;
const INCARNATION = { salt: SALT, firstLsn: 1, rootSequence: 5 };

function body(seed: number, length: number): Uint8Array {
  return Uint8Array.from({ length }, (_, index) => (seed * 31 + index) & 0xff);
}

function concat(parts: readonly Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

/** Frames with chained checksums; `parts` lists each transaction's part count. */
function transactions(parts: readonly number[], salt = SALT, firstLsn = 1, bodyLength = 24) {
  let chain = salt;
  let lsn = firstLsn;
  const frames: Array<{ bytes: Uint8Array; chain: number; lsn: number }> = [];
  for (const count of parts)
    for (let partIndex = 0; partIndex < count; partIndex++) {
      const encoded = encodeFrame(
        { salt, lsn, partIndex, partCount: count },
        body(lsn, bodyLength),
        chain,
      );
      chain = encoded.chain;
      frames.push({ ...encoded, lsn });
      lsn += 1;
    }
  return frames;
}

const header = (fields: Partial<LogHeader>) =>
  encodeLogHeader({
    salt: SALT,
    generation: 1,
    ackEnd: 0,
    ackChain: SALT,
    ackLsn: 0,
    rootSequence: 5,
    ...fields,
  });

const ends = (frames: ReadonlyArray<{ bytes: Uint8Array }>) =>
  frames.reduce<number[]>((acc, frame) => [...acc, (acc.at(-1) ?? 0) + frame.bytes.length], []);

describe("log frame envelope", () => {
  it("pads frames to 8 bytes and scans complete transactions back in order", () => {
    expect([0, 1, 4, 5].map(frameLength)).toEqual([32, 40, 40, 40]);
    const frames = transactions([1, 3, 1]);
    const scan = scanLog(concat(frames.map((f) => f.bytes)), [null, null], INCARNATION);
    expect(scan.frames.map((frame) => [frame.lsn, frame.transaction, frame.partIndex])).toEqual([
      [1, 1, 0],
      [2, 2, 0],
      [3, 2, 1],
      [4, 2, 2],
      [5, 5, 0],
    ]);
    expect(scan.lastLsn).toBe(5);
    expect(scan.chain).toBe(frames[4]?.chain);
    expect(scan.validEnd).toBe(ends(frames).at(-1));
  });

  it("stops at the first damaged frame of an unacknowledged tail", () => {
    const frames = transactions([1, 1, 1], SALT, 1, 40);
    const bytes = concat(frames.map((f) => f.bytes));
    const [first = 0, second = 0] = ends(frames);
    for (let byte = first; byte < second; byte++) {
      const damaged = bytes.slice();
      damaged[byte] = (damaged[byte] ?? 0) ^ 1;
      const scan = scanLog(damaged, [null, null], INCARNATION);
      expect(scan.frames.length, `byte ${String(byte)}`).toBe(1);
      expect(scan.validEnd).toBe(first);
    }
  });

  it("does not revive a stale frame behind a rewritten one (chained checksums)", () => {
    const [a, b] = transactions([1, 1]);
    const c = encodeFrame({ salt: SALT, lsn: 1, partIndex: 0, partCount: 1 }, body(99, 24), SALT);
    expect(a?.bytes.length).toBe(c.bytes.length);
    const scan = scanLog(
      concat([c.bytes, b?.bytes ?? new Uint8Array()]),
      [null, null],
      INCARNATION,
    );
    expect(scan.frames.length).toBe(1);
  });

  it("ignores frames of another incarnation and requires contiguous LSNs from the root", () => {
    const foreign = transactions([1], 77);
    expect(
      scanLog(foreign[0]?.bytes ?? new Uint8Array(), [null, null], INCARNATION).frames,
    ).toEqual([]);
    const fromFive = transactions([1], SALT, 5);
    const bytes = fromFive[0]?.bytes ?? new Uint8Array();
    expect(scanLog(bytes, [null, null], INCARNATION).frames).toEqual([]);
    expect(scanLog(bytes, [null, null], { ...INCARNATION, firstLsn: 5 }).frames.length).toBe(1);
  });
});

describe("multipart transactions", () => {
  it("drops an incomplete transaction and exposes the last complete boundary (review probe)", () => {
    const frames = transactions([1, 3]);
    const [firstEnd] = ends(frames);
    const scan = scanLog(concat(frames.slice(0, 3).map((f) => f.bytes)), [null, null], INCARNATION);
    expect(scan.frames.length).toBe(1);
    expect(scan.validEnd).toBe(firstEnd);
    expect(scan.lastLsn).toBe(1);
    expect(scan.abandonedParts).toBe(2);
    const lone = transactions([2]);
    expect(scanLog(lone[0]?.bytes ?? new Uint8Array(), [null, null], INCARNATION).validEnd).toBe(0);
  });

  it("an unrelated transaction cannot complete an abandoned prefix", () => {
    const [part0] = transactions([2]);
    const independent = encodeFrame(
      { salt: SALT, lsn: 2, partIndex: 0, partCount: 1 },
      body(7, 24),
      part0?.chain ?? 0,
    );
    const wrongCount = encodeFrame(
      { salt: SALT, lsn: 2, partIndex: 1, partCount: 3 },
      body(8, 24),
      part0?.chain ?? 0,
    );
    for (const next of [independent, wrongCount]) {
      const bytes = concat([part0?.bytes ?? new Uint8Array(), next.bytes]);
      const scan = scanLog(bytes, [null, null], INCARNATION);
      expect(scan.frames).toEqual([]);
      expect(scan.validEnd).toBe(0);
    }
  });
});

describe("log header slots and the acknowledged boundary", () => {
  it("round-trips a header and refuses a torn or unsafe one", () => {
    const fields = {
      salt: SALT,
      generation: 2 ** 40,
      ackEnd: 2 ** 33,
      ackChain: 5,
      ackLsn: 9,
      rootSequence: 2 ** 52,
    };
    const bytes = encodeLogHeader(fields);
    expect(decodeLogHeader(bytes)).toEqual(fields);
    for (let cut = 0; cut < bytes.length; cut++)
      expect(decodeLogHeader(bytes.slice(0, cut))).toBeNull();
    const flipped = bytes.slice();
    flipped[21] = (flipped[21] ?? 0) ^ 4;
    expect(decodeLogHeader(flipped)).toBeNull();
    expect(() => encodeLogHeader({ ...fields, ackEnd: 2 ** 53 })).toThrow(RangeError);
  });

  it("takes the newest valid slot of this salt", () => {
    const older = header({ generation: 1 });
    const newer = header({ generation: 2 });
    const foreign = encodeLogHeader({
      salt: 1,
      generation: 9,
      ackEnd: 0,
      ackChain: 1,
      ackLsn: 0,
      rootSequence: 1,
    });
    expect(newestLogHeader([older, newer], SALT)).toMatchObject({
      header: { generation: 2 },
      nextSlot: 0,
    });
    expect(newestLogHeader([newer, older], SALT)).toMatchObject({
      header: { generation: 2 },
      nextSlot: 1,
    });
    expect(newestLogHeader([foreign, older], SALT).header?.generation).toBe(1);
    expect(newestLogHeader([newer.slice(0, 20), older], SALT).header?.generation).toBe(1);
  });

  it("refuses to lose an acknowledged frame, and accepts losing the unacknowledged tail", () => {
    const frames = transactions([1, 1, 1], SALT, 1, 30);
    const bytes = concat(frames.map((f) => f.bytes));
    const [firstEnd = 0, secondEnd = 0] = ends(frames);
    const ack = header({ ackEnd: secondEnd, ackChain: frames[1]?.chain ?? 0, ackLsn: 2 });
    expect(scanLog(bytes.slice(0, secondEnd + 8), [ack, null], INCARNATION).frames.length).toBe(2);
    expect(() => scanLog(bytes.slice(0, firstEnd + 8), [ack, null], INCARNATION)).toThrow(
      LogCorruptionError,
    );
  });

  it("validates the whole boundary tuple (review probes: interior ack, wrong ack LSN, empty)", () => {
    const frames = transactions([1, 3]);
    const bytes = concat(frames.map((f) => f.bytes));
    const [firstEnd = 0, secondEnd = 0] = ends(frames);
    const chain0 = frames[0]?.chain ?? 0;
    const cases = [
      ["wrong chain", header({ ackEnd: firstEnd, ackChain: 1, ackLsn: 1 })],
      ["wrong LSN", header({ ackEnd: firstEnd, ackChain: chain0, ackLsn: 999 })],
      ["interior of a frame", header({ ackEnd: 8, ackChain: chain0, ackLsn: 1 })],
      [
        "inside a transaction",
        header({ ackEnd: secondEnd, ackChain: frames[1]?.chain ?? 0, ackLsn: 2 }),
      ],
      ["empty with the wrong chain", header({ ackEnd: 0, ackChain: 3, ackLsn: 0 })],
      ["empty with the wrong LSN", header({ ackEnd: 0, ackChain: SALT, ackLsn: 4 })],
    ] as const;
    for (const [label, slot] of cases)
      expect(() => scanLog(bytes, [slot, null], INCARNATION), label).toThrow(LogCorruptionError);
    const good = header({ ackEnd: firstEnd, ackChain: chain0, ackLsn: 1 });
    expect(scanLog(bytes, [good, null], INCARNATION).frames.length).toBe(4);
  });

  it("detects a root that was rolled back behind its log (lying flush)", () => {
    const incarnation = (rootSequence: number) =>
      encodeLogHeader({
        salt: 0x5555,
        generation: 1,
        ackEnd: 0,
        ackChain: 0x5555,
        ackLsn: 0,
        rootSequence,
      });
    expect(() => scanLog(new Uint8Array(0), [incarnation(6), null], INCARNATION)).toThrow(
      LogCorruptionError,
    );
    expect(scanLog(new Uint8Array(0), [incarnation(4), null], INCARNATION).frames).toEqual([]);
  });
});
