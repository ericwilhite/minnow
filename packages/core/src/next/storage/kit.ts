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

/** Checks that `root` (a workload root) names an intact extent. */
async function checkExtent(session: StoreSession, root: string, what: string): Promise<void> {
  const ref = JSON.parse(root) as { offset: number; length: number; checksum: number };
  if (ref.offset + ref.length > (await session.dataLength())) fail(`${what}'s extent is missing`);
  const [bytes] = await session.readData([ref]);
  if (bytes === undefined || xxHash32(bytes) !== ref.checksum)
    fail(`${what}'s extent does not match its checksum`);
}

const decode = (bytes: Uint8Array | null) =>
  bytes === null ? null : new TextDecoder().decode(bytes);

/**
 * Guarantee 1 plus the previous root (two-generation reclaim walks both): the current root is the
 * last published or the in-flight one; the previous root is exactly the one published before the
 * current, or absent only when the current is the first, or when an interrupted publish may have
 * overwritten its slot; and the extents both roots name are intact.
 */
async function checkRoots(session: StoreSession, acknowledged: Acknowledged, label: string) {
  const root = decode(await session.readRoot());
  const previous = decode(await session.readPreviousRoot());
  const published = acknowledged.published;
  const last = published.at(-1) ?? null;
  if (root !== last && root !== acknowledged.inFlight)
    fail(`${label}: root ${String(root)} is not the last published or in-flight one`);
  let expected: string | null;
  let mayBeLost: boolean;
  if (root === null) [expected, mayBeLost] = [null, false];
  else if (root === acknowledged.inFlight && root !== last) [expected, mayBeLost] = [last, false];
  else [expected, mayBeLost] = [published.at(-2) ?? null, acknowledged.inFlight !== null];
  if (previous !== expected && !(mayBeLost && previous === null))
    fail(
      `${label}: previous root ${String(previous)} is not the one before the current (${String(expected)})`,
    );
  if (root !== null) await checkExtent(session, root, `${label}: the published root`);
  if (previous !== null) await checkExtent(session, previous, `${label}: the previous root`);
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
  const cleanRun: Acknowledged = { published: [], inFlight: null, logBytes: new Uint8Array(0) };
  const firstBackend = createOverDisk(clean);
  const firstLease = await firstBackend.acquire({ timeoutMs: 2_000 });
  const first = await firstBackend.open(firstLease);
  await workload(first, cleanRun);
  const steps = clean.steps;
  await checkRoots(first, cleanRun, "clean run");
  await first.close();
  await firstLease.release();
  // Reopened as a new process would: both roots and their extents survive.
  await checkRoots(await open(createOverDisk(clean)), cleanRun, "clean reopen");
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
      await checkRoots(session, acknowledged, label);
      // Guarantee 3: every log byte acknowledged by a flush is there.
      const flushed = acknowledged.logBytes;
      if (session.logLength() < flushed.length)
        fail(`${label}: acknowledged log bytes are missing`);
      const read = await session.readLog(0, flushed.length);
      if (read.some((byte, index) => byte !== flushed[index]))
        fail(`${label}: acknowledged log bytes changed`);
    }
}
