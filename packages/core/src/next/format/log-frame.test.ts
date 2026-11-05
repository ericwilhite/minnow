import { describe, expect, it } from "vitest";
import {
  FrameKind,
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

/** Frames 1…n with chained checksums; returns their bytes and running chain values. */
function frames(bodies: readonly Uint8Array[], salt = SALT, firstLsn = 1) {
  let chain = salt;
  return bodies.map((frameBody, index) => {
    const encoded = encodeFrame(salt, firstLsn + index, FrameKind.commit, frameBody, chain);
    chain = encoded.chain;
    return encoded;
  });
}

function header(fields: Partial<LogHeader>): Uint8Array {
  return encodeLogHeader({
    salt: SALT,
    generation: 1,
    ackEnd: 0,
    ackChain: SALT,
    ackLsn: 0,
    ...fields,
  });
}

describe("log frame envelope", () => {
  it("pads frames to 8 bytes and scans them back in order", () => {
    expect([0, 1, 4, 5].map(frameLength)).toEqual([32, 32, 32, 40]);
    const bodies = [body(1, 0), body(2, 5), body(3, 700)];
    const encoded = frames(bodies);
    const scan = scanLog(concat(encoded.map((f) => f.bytes)), [null, null], SALT, 1);
    expect(scan.frames.map((frame) => [...frame.body])).toEqual(bodies.map((b) => [...b]));
    expect(scan.frames.map((frame) => frame.lsn)).toEqual([1, 2, 3]);
    expect(scan.lastLsn).toBe(3);
    expect(scan.chain).toBe(encoded[2]?.chain);
    expect(scan.validEnd).toBe(encoded.reduce((sum, f) => sum + f.bytes.length, 0));
  });

  it("stops at the first damaged frame of an unacknowledged tail", () => {
    const encoded = frames([body(1, 40), body(2, 40), body(3, 40)]);
    const bytes = concat(encoded.map((f) => f.bytes));
    const second = encoded[0]?.bytes.length ?? 0;
    for (let byte = second; byte < second + (encoded[1]?.bytes.length ?? 0); byte++) {
      const damaged = bytes.slice();
      damaged[byte] = (damaged[byte] ?? 0) ^ 1;
      const scan = scanLog(damaged, [null, null], SALT, 1);
      expect(scan.frames.length, `byte ${String(byte)}`).toBe(1);
      expect(scan.validEnd).toBe(second);
    }
  });

  it("does not revive a stale frame behind a rewritten one (chained checksums)", () => {
    // A, B written; recovery cut the log at A (say A was torn); C rewritten at A's offset with
    // the same length and LSN. B is still physically behind C, intact, with the right salt.
    const [a, b] = frames([body(1, 16), body(2, 16)]);
    const [c] = frames([body(9, 16)]);
    expect(a?.bytes.length).toBe(c?.bytes.length);
    const log = concat([c?.bytes ?? new Uint8Array(), b?.bytes ?? new Uint8Array()]);
    const scan = scanLog(log, [null, null], SALT, 1);
    expect(scan.frames.length).toBe(1);
    expect([...(scan.frames[0]?.body ?? [])]).toEqual([...body(9, 16)]);
  });

  it("ignores frames of another incarnation and requires contiguous LSNs from the root", () => {
    const encoded = frames([body(1, 8)], 77);
    expect(scanLog(encoded[0]?.bytes ?? new Uint8Array(), [null, null], SALT, 1).frames).toEqual(
      [],
    );
    const fromFive = frames([body(1, 8)], SALT, 5);
    expect(scanLog(fromFive[0]?.bytes ?? new Uint8Array(), [null, null], SALT, 1).frames).toEqual(
      [],
    );
    expect(
      scanLog(fromFive[0]?.bytes ?? new Uint8Array(), [null, null], SALT, 5).frames.length,
    ).toBe(1);
  });
});

describe("log header slots and the acknowledged boundary", () => {
  it("round-trips a header and refuses a torn one", () => {
    const fields = { salt: SALT, generation: 2 ** 40, ackEnd: 2 ** 33, ackChain: 5, ackLsn: 9 };
    const bytes = encodeLogHeader(fields);
    expect(decodeLogHeader(bytes)).toEqual(fields);
    for (let cut = 0; cut < bytes.length; cut++)
      expect(decodeLogHeader(bytes.slice(0, cut))).toBeNull();
    const flipped = bytes.slice();
    flipped[21] = (flipped[21] ?? 0) ^ 4;
    expect(decodeLogHeader(flipped)).toBeNull();
  });

  it("takes the newest valid slot of this salt", () => {
    const older = header({ generation: 1 });
    const newer = header({ generation: 2 });
    const foreign = encodeLogHeader({ salt: 1, generation: 9, ackEnd: 0, ackChain: 1, ackLsn: 0 });
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
    const encoded = frames([body(1, 30), body(2, 30), body(3, 30)]);
    const bytes = concat(encoded.map((f) => f.bytes));
    const firstEnd = encoded[0]?.bytes.length ?? 0;
    const secondEnd = firstEnd + (encoded[1]?.bytes.length ?? 0);
    const ack = header({ ackEnd: secondEnd, ackChain: encoded[1]?.chain ?? 0, ackLsn: 2 });
    // A cleanly truncated log (lying flush) inside the newest group: shorter, not an error.
    expect(scanLog(bytes.slice(0, secondEnd + 8), [ack, null], SALT, 1).frames.length).toBe(2);
    // Truncated inside an acknowledged group: corruption, never silently shorter.
    expect(() => scanLog(bytes.slice(0, firstEnd + 8), [ack, null], SALT, 1)).toThrow(
      LogCorruptionError,
    );
    // The boundary must land on the acknowledged frame's chain value.
    const wrongChain = header({ ackEnd: secondEnd, ackChain: 1, ackLsn: 2 });
    expect(() => scanLog(bytes, [wrongChain, null], SALT, 1)).toThrow(LogCorruptionError);
  });
});
