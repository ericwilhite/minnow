/**
 * Crash sweeps: the log protocol (format/log-frame.ts) and root publication, run against every
 * crash point of a workload and several post-crash outcomes per point. They prove the contract
 * is sufficient for the engine's recovery promises, and that the fault store exercises them.
 */
import { describe, expect, it } from "vitest";
import { xxHash32 } from "../hash/xxhash32.js";
import { FrameKind, encodeFrame, encodeLogHeader, newestLogHeader, scanLog } from "./log-frame.js";
import type { SealedPack, StoreSession } from "../storage/contract.js";
import { FaultInjectingStore, SimulatedCrash } from "../storage/fault-store.js";

const SALT = 0x5eed_0001;
const SEEDS = 12;

function frameBody(group: number, frame: number): Uint8Array {
  // Sizes cross the 512-byte sector so tears land inside frames.
  const length = 20 + ((group * 7 + frame * 13) % 5) * 300;
  return Uint8Array.from({ length }, (_, index) => (group * 17 + frame * 5 + index) & 0xff);
}

/** The engine side of group commit, minimal: chain frames, ack the previous group, flush once. */
class GroupCommitter {
  chain = SALT;
  lsn = 0;
  generation = 0;
  ack = { end: 0, chain: SALT, lsn: 0 };
  readonly acknowledged: Uint8Array[] = [];
  readonly appended: Uint8Array[] = [];

  constructor(readonly session: StoreSession) {}

  async commit(bodies: readonly Uint8Array[]): Promise<void> {
    for (const body of bodies) {
      const frame = encodeFrame(SALT, ++this.lsn, FrameKind.commit, body, this.chain);
      this.chain = frame.chain;
      this.appended.push(body);
      this.session.appendLog(frame.bytes);
    }
    const { nextSlot } = newestLogHeader(await this.session.readLogHeaders(), SALT);
    this.session.writeLogHeader(
      nextSlot,
      encodeLogHeader({
        salt: SALT,
        generation: ++this.generation,
        ackEnd: this.ack.end,
        ackChain: this.ack.chain,
        ackLsn: this.ack.lsn,
      }),
    );
    await this.session.flushLog();
    this.acknowledged.push(...bodies);
    this.ack = { end: this.session.logLength(), chain: this.chain, lsn: this.lsn };
  }
}

async function recoverLog(session: StoreSession) {
  const body = await session.readLog(0, session.logLength());
  return scanLog(body, await session.readLogHeaders(), SALT, 1);
}

async function logWorkload(committer: GroupCommitter): Promise<void> {
  for (let group = 0; group < 6; group++)
    await committer.commit(
      Array.from({ length: 1 + (group % 3) }, (_, frame) => frameBody(group, frame)),
    );
}

describe("log recovery under every crash point", () => {
  it("keeps every acknowledged commit, invents nothing, and resumes cleanly", async () => {
    const clean = new FaultInjectingStore({ seed: 0 });
    await logWorkload(new GroupCommitter(await clean.open()));
    const totalSteps = clean.steps;
    expect(totalSteps).toBeGreaterThan(20);

    let crashes = 0;
    for (let step = 0; step < totalSteps; step++)
      for (let seed = 1; seed <= SEEDS; seed++) {
        const store = new FaultInjectingStore({ crashAtStep: step, seed: seed * 1000 + step });
        const committer = new GroupCommitter(await store.open());
        await expect(logWorkload(committer)).rejects.toThrow(SimulatedCrash);
        crashes += 1;
        store.recover();

        const session = await store.open();
        const scan = await recoverLog(session);
        const recovered = scan.frames.map((frame) => frame.body.slice());
        const label = `step ${String(step)} seed ${String(seed)}`;
        // Every acknowledged commit survives, in order.
        expect(recovered.length, label).toBeGreaterThanOrEqual(committer.acknowledged.length);
        // Nothing is invented: the recovered frames are a prefix of what was appended.
        recovered.forEach((recoveredBody, index) =>
          expect(recoveredBody, label).toEqual(committer.appended[index]),
        );

        // Resume: cut the invalid tail, continue the chain, then reopen and rescan cleanly.
        await session.truncateLog(scan.validEnd);
        const resumed = new GroupCommitter(session);
        resumed.chain = scan.chain;
        resumed.lsn = scan.lastLsn;
        resumed.generation = scan.header?.generation ?? 0;
        resumed.ack = { end: scan.validEnd, chain: scan.chain, lsn: scan.lastLsn };
        await resumed.commit([frameBody(90, 0)]);
        await resumed.commit([frameBody(91, 0), frameBody(91, 1)]);
        session.close();
        const final = await recoverLog(await store.open());
        expect(
          final.frames.map((frame) => frame.body.slice()),
          label,
        ).toEqual([...recovered, ...resumed.acknowledged]);
      }
    expect(crashes).toBe(totalSteps * SEEDS);
  });
});

describe("root publication under every crash point", () => {
  const packBytes = (id: number) =>
    Uint8Array.from({ length: 600 + id * 50 }, (_, i) => (id * 13 + i) & 0xff);
  const encodeRoot = (packs: readonly number[]) => new TextEncoder().encode(JSON.stringify(packs));

  /** Publishes roots that reference the two newest packs and reclaims the pack that left. */
  async function publishWorkload(
    session: StoreSession,
    published: Array<readonly number[]>,
  ): Promise<void> {
    for (let id = 1; id <= 5; id++) {
      const writer = session.createPack(id);
      const bytes = packBytes(id);
      await writer.append(bytes.subarray(0, 300));
      await writer.append(bytes.subarray(300));
      const sealed: SealedPack = await writer.seal();
      const referenced = id === 1 ? [1] : [id - 1, id];
      published.push(referenced);
      await session.publish(encodeRoot(referenced), [sealed]);
      if (id > 2) await session.reclaim([id - 2]);
    }
  }

  it("always opens to a published root whose packs are whole", async () => {
    const clean = new FaultInjectingStore({ seed: 0 });
    await publishWorkload(await clean.open(), []);
    const totalSteps = clean.steps;

    for (let step = 0; step < totalSteps; step++)
      for (let seed = 1; seed <= SEEDS; seed++) {
        const store = new FaultInjectingStore({ crashAtStep: step, seed: seed * 7919 + step });
        const attempted: Array<readonly number[]> = [];
        await expect(publishWorkload(await store.open(), attempted)).rejects.toThrow(
          SimulatedCrash,
        );
        store.recover();
        const session = await store.open();
        const root = await session.readRoot();
        const label = `step ${String(step)} seed ${String(seed)}`;
        if (root === null) {
          expect(attempted.length, label).toBeLessThanOrEqual(1);
          continue;
        }
        const referenced = JSON.parse(new TextDecoder().decode(root)) as number[];
        expect(
          attempted.map((packs) => packs.join()),
          label,
        ).toContain(referenced.join());
        for (const id of referenced) {
          const expected = packBytes(id);
          const [read] = await session.read([{ pack: id, offset: 0, length: expected.length }]);
          expect(xxHash32(read ?? new Uint8Array()), `${label} pack ${String(id)}`).toBe(
            xxHash32(expected),
          );
        }
        // The open-time sweep: everything the root does not reach is an orphan.
        const orphans = (await session.listPacks()).filter((id) => !referenced.includes(id));
        await session.reclaim(orphans);
        expect(await session.listPacks(), label).toEqual([...referenced].sort((a, b) => a - b));
      }
  });

  it("can crash at an arbitrary moment and refuses calls until recovered", async () => {
    const store = new FaultInjectingStore({ seed: 3 });
    const session = await store.open();
    session.appendLog(new Uint8Array([1, 2, 3]));
    store.crash();
    expect(() => session.appendLog(new Uint8Array([4]))).toThrow(SimulatedCrash);
    await expect(store.open()).rejects.toThrow(TypeError);
    store.recover();
    const reopened = await store.open();
    expect(reopened.logLength()).toBeLessThanOrEqual(3);
  });
});
