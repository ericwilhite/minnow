/**
 * The storage kit: what a backend must pass, as one call. Conformance alone is not durability
 * evidence (review A1-2: a store that never flushed its data or its log passed every conformance
 * case), so the kit runs the conformance cases **and** a physical durability sweep, and
 * `kit.test.ts` proves the sweep rejects both wrong stores.
 *
 * The sweep needs a crash model, so a backend enters the kit built over the simulated platform
 * (`createOverDisk(disk)`): the file store over `SimDisk` handles (what OPFS runs, with real
 * handles in browsers); an IndexedDB backend over a simulated IndexedDB with the same physical
 * step and power-loss hooks (storage track). Each crash point is followed by a fresh backend over
 * the same disk, as a new process would be.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import { storeConformanceCases } from "./conformance.js";
import { DATA_ALIGN, type StorageBackend, type StoreSession } from "./contract.js";
import { SimDisk, SimulatedCrash } from "./sim-disk.js";

export interface KitOptions {
  /** Post-crash outcomes per crash point. */
  readonly seeds?: number;
}

function fail(message: string): never {
  throw new Error(`Storage kit: ${message}`);
}

interface Acknowledged {
  /** Roots whose publish resolved, in order; the last one in flight. */
  readonly published: string[];
  inFlight: string | null;
  /** Log bytes whose flush resolved. */
  logBytes: Uint8Array;
}

const extentBytes = (round: number) =>
  Uint8Array.from({ length: 900 + round * 70 }, (_, i) => (round * 31 + i) & 0xff);
const logBytes = (round: number) =>
  Uint8Array.from({ length: 600 + round * 50 }, (_, i) => (round * 17 + i) & 0xff);

/** Rounds of: write an extent, publish a root naming it, append log bytes, flush the log. */
async function workload(session: StoreSession, acknowledged: Acknowledged): Promise<void> {
  let log = new Uint8Array(0);
  for (let round = 1; round <= 4; round++) {
    const bytes = extentBytes(round);
    const offset = (round % 3) * 2 * DATA_ALIGN;
    await session.writeData(offset, bytes);
    const root = JSON.stringify({ offset, length: bytes.length, checksum: xxHash32(bytes) });
    acknowledged.inFlight = root;
    await session.publish(new TextEncoder().encode(root));
    acknowledged.published.push(root);
    acknowledged.inFlight = null;
    const appended = logBytes(round);
    await session.appendLog(appended);
    const next = new Uint8Array(log.length + appended.length);
    next.set(log);
    next.set(appended, log.length);
    log = next;
    await session.flushLog();
    acknowledged.logBytes = log;
  }
}

async function open(backend: StorageBackend): Promise<StoreSession> {
  return backend.open(await backend.acquire({ timeoutMs: 2_000 }));
}

/** Runs conformance and the durability sweep; throws on the first failure. */
export async function runStorageKit(
  createOverDisk: (disk: SimDisk) => StorageBackend,
  options: KitOptions = {},
): Promise<void> {
  for (const testCase of storeConformanceCases)
    await testCase.run(async () => createOverDisk(new SimDisk()));

  const clean = new SimDisk();
  await workload(await open(createOverDisk(clean)), {
    published: [],
    inFlight: null,
    logBytes: new Uint8Array(0),
  });
  const steps = clean.steps;
  const seeds = options.seeds ?? 8;
  for (let step = 0; step <= steps; step++)
    for (let seed = 1; seed <= seeds; seed++) {
      const disk = new SimDisk({ crashAtStep: step, seed: seed * 7907 + step });
      const acknowledged: Acknowledged = {
        published: [],
        inFlight: null,
        logBytes: new Uint8Array(0),
      };
      try {
        await workload(await open(createOverDisk(disk)), acknowledged);
        disk.powerLoss();
      } catch (error) {
        if (!(error instanceof SimulatedCrash)) throw error;
      }
      const label = `crash at step ${String(step)}, seed ${String(seed)}`;
      const session = await open(createOverDisk(disk));
      // Guarantee 1: the last published root or the one in flight, and its extent intact.
      const rootBytes = await session.readRoot();
      const root = rootBytes === null ? null : new TextDecoder().decode(rootBytes);
      const allowed = [acknowledged.published.at(-1) ?? null, acknowledged.inFlight];
      if (!allowed.includes(root))
        fail(`${label}: root ${String(root)} is not the last published or in-flight one`);
      if (root !== null) {
        const ref = JSON.parse(root) as { offset: number; length: number; checksum: number };
        if (ref.offset + ref.length > (await session.dataLength()))
          fail(`${label}: a published extent is missing`);
        const [bytes] = await session.readData([ref]);
        if (bytes === undefined || xxHash32(bytes) !== ref.checksum)
          fail(`${label}: a published extent does not match its checksum`);
      }
      // Guarantee 3: every log byte acknowledged by a flush is there.
      const flushed = acknowledged.logBytes;
      if (session.logLength() < flushed.length)
        fail(`${label}: acknowledged log bytes are missing`);
      const read = await session.readLog(0, flushed.length);
      if (read.some((byte, index) => byte !== flushed[index]))
        fail(`${label}: acknowledged log bytes changed`);
    }
}
