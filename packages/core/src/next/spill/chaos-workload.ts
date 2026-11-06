/**
 * The storage kill campaign's worker side (Gate A, "store opens after a killed query"): a seeded
 * random workload on one store — builders, publish, retire and reclaim, log append, flush,
 * truncate and reset, spill — run by next-storage-worker.ts until the page kills it, and the
 * recovery a fresh worker runs first: reopen, check the root and every acknowledged log byte,
 * audit exactly, then carry on.
 *
 * ## What the page knows (next-storage-run.ts)
 * Before each step the worker announces its intent and waits for the page's go; after it, it
 * reports its acknowledgement (a published root, a flushed log). Messages on one port arrive in
 * order, so when the page kills the worker it knows every acknowledged state exactly, plus at most
 * one step in flight. The next worker is handed that knowledge (`ChaosExpect`) and checks:
 * - **Root.** The recovered root is the last acknowledged one, or the one an in-flight publish was
 *   writing; its round and the identity of every blob (coordinates and checksum) match. Every blob
 *   it names reads back and verifies, and the free map is exact (`audit`).
 * - **Log.** The log header names the generation (written by each reset); every byte the last
 *   acknowledged flush covered reads back as written (cut to an in-flight truncate's length; none
 *   when a reset was in flight, which may have run). Recovery then truncates the log to that
 *   prefix, as the write path does with a torn tail.
 * - **Spill.** The startup sweep empties it and a round trip works.
 * Any other outcome is a corruption, reported with the seed, the iteration and the kill.
 */
import type { StorageBackend, StoreSession } from "../storage/contract.js";
import type { BlobRef, ExtentRef, ExtentSpace } from "../storage/extents.js";
import { openExtentSpace } from "../storage/space.js";
import {
  bytesOf,
  countingBudget,
  countingMemory,
  firstDifference,
  jsonReachable,
} from "../storage/space-test-support.js";
import { ScratchSpillService } from "./scratch-spill.js";

/** The kinds of step; each is announced (`ChaosIntent`) before it starts. */
export const CHAOS_STEPS = [
  "write",
  "publish",
  "reclaim",
  "log-append",
  "log-flush",
  "truncate",
  "reset",
  "spill",
  "read",
] as const;
export type ChaosStep = (typeof CHAOS_STEPS)[number];

/** A step about to run (the page answers with a go). */
export interface ChaosIntent {
  readonly step: ChaosStep;
  readonly sequence: number;
  /** For publish and reclaim: the root it writes. */
  readonly root?: { readonly round: number; readonly identity: string };
  /** For truncate: the new length. */
  readonly truncateTo?: number;
}

/** The store as the page last knew it acknowledged. */
export interface ChaosState {
  readonly round: number;
  readonly identity: string;
  /** The log generation and the bytes the last acknowledged flush made durable. */
  readonly generation: number;
  readonly flushed: number;
}

/**
 * What a recovering worker is told: the acknowledged state (every strict acknowledgement the page
 * received: a published root, a flushed, truncated or reset log; relaxed ones, appends and data
 * writes, promise nothing after a crash) and the steps in flight.
 */
export interface ChaosExpect {
  readonly state: ChaosState;
  readonly inFlight: readonly ChaosIntent[];
}

/** The serialized workload (a step at a time, each with the page's go) or the concurrent one. */
export type ChaosMode = "serial" | "concurrent";

/** Test-only faults (the campaign's own regression tests). */
export type ChaosInjection = "spill-mismatch" | "stall-publish-after-go";

export interface ChaosRecovery {
  /** Where the root and log were found, and the state the workload continues from. */
  readonly found: {
    readonly round: number;
    readonly generation: number;
    readonly logLength: number;
  };
  readonly state: ChaosState;
  readonly blobs: number;
  readonly verifiedLogBytes: number;
  readonly openMs: number;
  /** The previous worker's lock was still held after its kill and had to be stolen. */
  readonly stolenLock: boolean;
}

/**
 * Messages to the page: an intent, the step's storage work beginning (`began`, posted just before
 * its first storage call) and ending (`settled`, just after its last), an acknowledgement, the
 * end of a step, recovery.
 */
export type ChaosMessage =
  | { readonly chaos: "intent"; readonly intent: ChaosIntent }
  | { readonly chaos: "began"; readonly sequence: number }
  | { readonly chaos: "failure"; readonly message: string }
  | { readonly chaos: "settled"; readonly sequence: number }
  | { readonly chaos: "ack"; readonly state: ChaosState }
  | { readonly chaos: "done"; readonly sequence: number }
  | { readonly chaos: "recovered"; readonly recovery: ChaosRecovery };

interface TestRoot {
  readonly round: number;
  readonly blobs: readonly BlobRef[];
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const encodeRoot = (root: TestRoot) => encoder.encode(JSON.stringify(root));

/** The identity of a root's blobs: their count and a hash of every coordinate and checksum. */
export function identity(blobs: readonly BlobRef[]): string {
  let hash = 0x811c9dc5;
  for (const blob of blobs)
    for (const part of [
      blob.extent.offset,
      blob.extent.length,
      blob.offset,
      blob.length,
      blob.checksum,
    ]) {
      const text = String(part);
      for (let index = 0; index < text.length; index++)
        hash = Math.imul(hash ^ text.charCodeAt(index), 0x01000193) >>> 0;
      hash = Math.imul(hash ^ 0x2c, 0x01000193) >>> 0;
    }
  return `${String(blobs.length)}:${hash.toString(16)}`;
}

/** The log byte at `offset` in `generation`: a pure function, so truncation keeps it valid. */
const logByte = (generation: number, offset: number) =>
  (generation * 97 + offset * 13 + (offset >>> 9)) & 0xff;

function logBytes(generation: number, offset: number, length: number): Uint8Array {
  return Uint8Array.from({ length }, (_, index) => logByte(generation, offset + index));
}

const headerOf = (generation: number) => encoder.encode(JSON.stringify({ generation }));

function generationOf(header: Uint8Array | null): number | null {
  if (header === null) return null;
  const end = header.indexOf(0);
  try {
    const parsed = JSON.parse(decoder.decode(end < 0 ? header : header.subarray(0, end))) as {
      generation?: unknown;
    };
    return typeof parsed.generation === "number" ? parsed.generation : null;
  } catch {
    return null;
  }
}

function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** A corruption: what was found against what the acknowledgements require. */
export class ChaosCorruption extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ChaosCorruption";
  }
}

async function readAllBlobs(space: ExtentSpace, blobs: readonly BlobRef[]): Promise<void> {
  const budget = countingBudget(Infinity);
  let batch: BlobRef[] = [];
  let bytes = 0;
  const flush = async () => {
    if (batch.length === 0) return;
    const lease = await space.read(batch, { budget });
    lease.release();
    batch = [];
    bytes = 0;
  };
  for (const blob of blobs) {
    if (bytes + blob.length > 2 << 20) await flush();
    batch.push(blob);
    bytes += blob.length;
  }
  await flush();
}

export interface ChaosHooks {
  readonly backendFor: (name: string) => Promise<StorageBackend>;
  readonly post: (message: ChaosMessage) => void;
  /** Resolves when the page answers `sequence` with a go. */
  readonly go: (sequence: number) => Promise<void>;
  /**
   * Receives the graceful step-down once recovery is done: close the session (its queue commits),
   * then release the lease (the IndexedDB backend drains and closes its connection first).
   */
  readonly registerRelease?: (
    release: () => Promise<{ readonly sessionMs: number; readonly releaseMs: number }>,
  ) => void;
}

/**
 * Recovers the store `dir` against `expect` (null for a new store), then runs the seeded workload
 * until this worker is killed. Throws `ChaosCorruption` when recovery finds what no
 * acknowledgement allows.
 */
export async function runChaos(
  dir: string,
  seed: number,
  expect: ChaosExpect | null,
  hooks: ChaosHooks,
  options: {
    readonly inject?: ChaosInjection;
    readonly mode?: ChaosMode;
    /** Concurrent mode: the lanes to run (all by default; diagnosis narrows them). */
    readonly lanes?: readonly string[];
  } = {},
): Promise<never> {
  const inject = options.inject ?? null;
  const started = performance.now();
  const backend = await hooks.backendFor(dir);
  // The killed worker's lock should be gone; if the platform keeps it (WebKit, after a worker
  // killed with IndexedDB transactions in flight), the new owner steals it, as the engine's
  // ownership does with an unresponsive owner (the epoch fence refuses the old one's writes).
  let stolen = false;
  let lease;
  try {
    lease = await backend.acquire({ timeoutMs: 20_000 });
  } catch (error) {
    if (!(error instanceof Error) || error.name !== "OwnerUnresponsiveError") throw error;
    lease = await backend.acquire({ steal: true, timeoutMs: 20_000 });
    stolen = true;
  }
  const session = await backend.open(lease);
  const space = await openExtentSpace(session, {
    reachable: jsonReachable,
    budget: countingBudget(Infinity),
    registerCache: () => countingMemory(Infinity),
  });
  const openMs = performance.now() - started;
  const fail = (message: string): never => {
    throw new ChaosCorruption(message);
  };

  // ── The root ───────────────────────────────────────────────────────────────────────────────
  const root = space.root === null ? null : (JSON.parse(decoder.decode(space.root)) as TestRoot);
  const known = expect?.state ?? { round: 0, identity: identity([]), generation: 0, flushed: 0 };
  const inFlight = expect?.inFlight ?? [];
  const pending = inFlight.flatMap((intent) => (intent.root === undefined ? [] : [intent.root]));
  const round = root?.round ?? 0;
  const found = root === null ? identity([]) : identity(root.blobs);
  const matchesAcked = round === known.round && found === known.identity;
  const matchesPending = pending.some(
    (candidate) => round === candidate.round && found === candidate.identity,
  );
  if (!matchesAcked && !matchesPending)
    fail(
      `root round ${String(round)} (${found}) is neither the acknowledged round ${String(known.round)} (${known.identity})` +
        (pending.length === 0
          ? ""
          : ` nor an in-flight one (${pending.map((candidate) => `${String(candidate.round)} ${candidate.identity}`).join(", ")})`),
    );
  let blobs: BlobRef[] = [...(root?.blobs ?? [])];
  await readAllBlobs(space, blobs);
  await space.audit();

  // ── The log ────────────────────────────────────────────────────────────────────────────────
  const [header] = await session.readLogHeaders();
  const headerGeneration = generationOf(header);
  const resetting = inFlight.some((intent) => intent.step === "reset");
  const truncateTo = Math.min(
    Number.POSITIVE_INFINITY,
    ...inFlight.map((intent) => intent.truncateTo ?? Number.POSITIVE_INFINITY),
  );
  let generation: number;
  let verified = 0;
  if (headerGeneration === known.generation) {
    generation = known.generation;
    // An in-flight truncate may have cut the acknowledged bytes; an in-flight reset may have run
    // and left this generation's header (it writes the header after the reset).
    verified = resetting ? 0 : Math.min(known.flushed, truncateTo);
  } else if (
    resetting &&
    (headerGeneration === null || headerGeneration === known.generation + 1)
  ) {
    generation = known.generation + 1;
    verified = 0;
  } else if (known.generation === 0 && known.flushed === 0 && headerGeneration === null) {
    generation = 0; // a new store, or one whose first header was never acknowledged
    verified = 0;
  } else {
    generation = fail(
      `log header names generation ${String(headerGeneration)}, acknowledged ${String(known.generation)}${resetting ? " (a reset in flight)" : ""}`,
    );
  }
  const logLength = session.logLength();
  if (logLength < verified)
    fail(`log is ${String(logLength)} bytes; ${String(verified)} acknowledged bytes are missing`);
  if (verified > 0) {
    const bytes = await session.readLog(0, verified);
    const at = firstDifference(bytes, logBytes(generation, 0, verified));
    if (at !== -1)
      fail(
        `acknowledged log byte ${String(at)} reads ${String(bytes[at])}, not ${String(logByte(generation, at))}`,
      );
  }
  // Recovery: keep exactly the verified prefix; a new store or an ambiguous reset starts a fresh,
  // acknowledged generation.
  if (verified === 0) {
    generation += 1;
    await session.resetLog();
    await session.writeLogHeader(0, headerOf(generation));
    await session.flushLog();
  } else if (logLength > verified) await session.truncateLog(verified);

  // ── Spill ──────────────────────────────────────────────────────────────────────────────────
  const scratch = backend.scratch(lease);
  const spill = scratch === null ? null : new ScratchSpillService(scratch);
  await spill?.sweep();
  if (spill !== null && spill.bytesInUse !== 0) fail("the spill sweep left space in use");

  let state: ChaosState = { round, identity: found, generation, flushed: verified };
  hooks.registerRelease?.(async () => {
    const started = performance.now();
    await session.close();
    const closed = performance.now();
    await lease.release();
    return { sessionMs: closed - started, releaseMs: performance.now() - closed };
  });
  hooks.post({
    chaos: "recovered",
    recovery: {
      found: { round, generation: headerGeneration ?? -1, logLength },
      state,
      blobs: blobs.length,
      verifiedLogBytes: verified,
      openMs,
      stolenLock: stolen,
    },
  });

  // ── The workload ───────────────────────────────────────────────────────────────────────────
  if (options.mode === "concurrent")
    return concurrentWorkload(
      seed,
      hooks,
      { space, session, spill, blobs, round, state, fail },
      options.lanes,
    );
  const random = mulberry32(seed);
  let sequence = 0;
  const announce = async (intent: Omit<ChaosIntent, "sequence">) => {
    sequence += 1;
    const full = { ...intent, sequence };
    hooks.post({ chaos: "intent", intent: full });
    await hooks.go(sequence);
    // The campaign's regression: a worker that has its go but never reaches storage.
    if (inject === "stall-publish-after-go" && intent.step === "publish")
      await new Promise<never>(() => undefined);
    hooks.post({ chaos: "began", sequence });
    return full;
  };
  /** The step's last storage call returned (before its acknowledgement and its end). */
  const settledStep = () => {
    hooks.post({ chaos: "settled", sequence });
  };
  let injected = false;
  const done = () => {
    hooks.post({ chaos: "done", sequence });
  };
  const ack = (next: ChaosState) => {
    state = next;
    hooks.post({ chaos: "ack", state });
  };
  /** Extents retired by the last publish: the next publish frees them (two generations). */
  let retiredLast = false;
  let currentRound = round;
  let query = 1;
  const extentsOf = (list: readonly BlobRef[]) => {
    const seen = new Map<number, ExtentRef>();
    for (const blob of list) seen.set(blob.extent.offset, blob.extent);
    return [...seen.values()];
  };

  for (;;) {
    const roll = random();
    if (roll < 0.36) {
      // Builders (some with their own buffers), then a publish that may retire whole extents.
      await announce({ step: "write" });
      const writer = space.beginWrite(
        "chaos",
        random() < 0.5 ? { budget: countingBudget(Infinity) } : {},
      );
      const written: BlobRef[] = [];
      const builders = Array.from({ length: 1 + Math.floor(random() * 3) }, () =>
        writer.open(4096 + Math.floor(random() * 65_536)),
      );
      const pages = 1 + Math.floor(random() * 12);
      for (let page = 0; page < pages; page++)
        for (const builder of builders) {
          const size = 1 + Math.floor(random() * (random() < 0.2 ? 60_000 : 6_000));
          if (builder.length + size > builder.capacity) builder.extend(size + 4096);
          await builder.append([bytesOf(size, Math.floor(random() * 1e6))]);
        }
      for (const builder of builders) {
        const sealed = await builder.seal();
        if (sealed !== null) written.push(...sealed.blobs);
      }
      settledStep();
      done();
      let retired: ExtentRef[] = [];
      let kept = blobs;
      if (blobs.length > 0 && (random() < 0.35 || blobs.length > 150)) {
        const extents = extentsOf(blobs);
        const drop = new Set(extents.filter(() => random() < 0.4).map((extent) => extent.offset));
        retired = extents.filter((extent) => drop.has(extent.offset));
        kept = blobs.filter((blob) => !drop.has(blob.extent.offset));
      }
      const next = [...kept, ...written];
      const target = { round: currentRound + 1, identity: identity(next) };
      const reclaiming = retiredLast;
      await announce({ step: reclaiming ? "reclaim" : "publish", root: target });
      await space.publish(encodeRoot({ round: target.round, blobs: next }), {
        writers: [writer],
        retired,
      });
      currentRound = target.round;
      blobs = next;
      ack({ ...state, round: target.round, identity: target.identity });
      if (reclaiming) await (space as unknown as { settled(): Promise<void> }).settled();
      settledStep();
      retiredLast = retired.length > 0;
      done();
    } else if (roll < 0.46 && retiredLast) {
      // A publish that only frees what the last one retired (and settles the release).
      const target = { round: currentRound + 1, identity: identity(blobs) };
      await announce({ step: "reclaim", root: target });
      await space.publish(encodeRoot({ round: target.round, blobs }), {
        writers: [],
        retired: [],
      });
      currentRound = target.round;
      ack({ ...state, round: target.round, identity: target.identity });
      await (space as unknown as { settled(): Promise<void> }).settled();
      settledStep();
      retiredLast = false;
      done();
    } else if (roll < 0.62) {
      await announce({ step: "log-append" });
      for (let count = 1 + Math.floor(random() * 4); count > 0; count--) {
        const offset = session.logLength();
        const length = 1 + Math.floor(random() * (random() < 0.1 ? 90_000 : 3_000));
        await session.appendLog(logBytes(state.generation, offset, length));
      }
      settledStep();
      done();
    } else if (roll < 0.76) {
      await announce({ step: "log-flush" });
      const length = session.logLength();
      await session.flushLog();
      settledStep();
      ack({ ...state, flushed: length });
      done();
    } else if (roll < 0.84) {
      const to = Math.floor(random() * (session.logLength() + 1));
      await announce({ step: "truncate", truncateTo: to });
      await session.truncateLog(to);
      settledStep();
      ack({ ...state, flushed: Math.min(state.flushed, to) });
      done();
    } else if (roll < 0.87) {
      await announce({ step: "reset" });
      const generation = state.generation + 1;
      await session.resetLog();
      await session.writeLogHeader(0, headerOf(generation));
      await session.flushLog();
      settledStep();
      ack({ ...state, generation, flushed: 0 });
      done();
    } else if (spill !== null) {
      await announce({ step: "spill" });
      const file = await spill.create(
        query++,
        random() < 0.5 ? countingBudget(1 << 20) : countingBudget(0),
      );
      const refs = [];
      for (let page = 0; page < 2 + Math.floor(random() * 10); page++) {
        const bytes = bytesOf(1 + Math.floor(random() * 50_000), page);
        refs.push([await file.append(bytes), bytes] as const);
      }
      for (const [ref, bytes] of refs)
        if (firstDifference(await file.read(ref), bytes) !== -1)
          fail("a spill page read back wrong");
      // The campaign's regression: a transient mismatch that the next sweep cannot see.
      if (inject === "spill-mismatch" && !injected) {
        injected = true;
        fail("a spill page read back wrong (injected)");
      }
      await file.close();
      settledStep();
      done();
    }
  }
}

interface Recovered {
  readonly space: ExtentSpace;
  readonly session: StoreSession;
  readonly spill: ScratchSpillService | null;
  readonly blobs: BlobRef[];
  readonly round: number;
  readonly state: ChaosState;
  readonly fail: (message: string) => never;
}

/** Runs `work` alone (the next caller waits for it). */
function mutex(): <T>(work: () => Promise<T>) => Promise<T> {
  let tail: Promise<unknown> = Promise.resolve();
  return <T>(work: () => Promise<T>) => {
    const run = tail.then(work);
    tail = run.catch(() => undefined);
    return run;
  };
}

/**
 * The concurrent workload: lanes that run at once and never wait for the page.
 * - Two writer lanes, each building one or two builders' worth of pages, then publishing (under a
 *   lock that keeps rounds and the live set consistent; the space serializes publishes anyway),
 *   sometimes retiring whole extents; the publish after a retiring one frees them, and its
 *   release is settled outside the lock, concurrently with the other lanes.
 * - A reader lane: pins the current root (so a concurrent reclaim cannot free what it reads) and
 *   reads a sample of its blobs, each verified.
 * - A log lane: appends (relaxed), flushes (strict: acknowledged), truncates, rarely resets.
 * - A spill lane: files written, read back and closed.
 * Every operation posts an intent and its end; every strict acknowledgement posts the state.
 */
async function concurrentWorkload(
  seed: number,
  hooks: ChaosHooks,
  recovered: Recovered,
  only?: readonly string[],
): Promise<never> {
  const { space, session, spill, fail } = recovered;
  const random = mulberry32(seed);
  let blobs = recovered.blobs;
  let round = recovered.round;
  let state = recovered.state;
  let retiredLast = false;
  let sequence = 0;
  // Each operation announces itself and waits for the page's go, which the page gives at once
  // (lanes stay concurrent) until it decides to kill: then nothing new starts, so whatever the
  // worker does before the kill takes effect (terminate() is asynchronous, and messages posted
  // meanwhile may be dropped) was announced. Then `began`, the storage work, `settled`, `done`.
  const begin = async (intent: Omit<ChaosIntent, "sequence">): Promise<number> => {
    sequence += 1;
    const at = sequence;
    hooks.post({ chaos: "intent", intent: { ...intent, sequence: at } });
    await hooks.go(at);
    hooks.post({ chaos: "began", sequence: at });
    return at;
  };
  const end = (at: number) => {
    hooks.post({ chaos: "settled", sequence: at });
    hooks.post({ chaos: "done", sequence: at });
  };
  const ack = (next: ChaosState) => {
    state = next;
    hooks.post({ chaos: "ack", state });
  };
  const publishing = mutex();
  const settle = () => (space as unknown as { settled(): Promise<void> }).settled();
  const extentsOf = (list: readonly BlobRef[]) => {
    const seen = new Map<number, ExtentRef>();
    for (const blob of list) seen.set(blob.extent.offset, blob.extent);
    return [...seen.values()];
  };
  const pause = () =>
    new Promise<void>((resolve) => {
      setTimeout(resolve, Math.floor(random() * 3));
    });

  const writer = async (): Promise<never> => {
    for (;;) {
      const at = await begin({ step: "write" });
      const writer = space.beginWrite(
        "chaos",
        random() < 0.5 ? { budget: countingBudget(Infinity) } : {},
      );
      const builders = Array.from({ length: 1 + Math.floor(random() * 2) }, () =>
        writer.open(4096 + Math.floor(random() * 65_536)),
      );
      const written: BlobRef[] = [];
      for (let page = 1 + Math.floor(random() * 8); page > 0; page--)
        for (const builder of builders) {
          const size = 1 + Math.floor(random() * (random() < 0.2 ? 60_000 : 6_000));
          if (builder.length + size > builder.capacity) builder.extend(size + 4096);
          await builder.append([bytesOf(size, Math.floor(random() * 1e6))]);
        }
      for (const builder of builders) {
        const sealed = await builder.seal();
        if (sealed !== null) written.push(...sealed.blobs);
      }
      end(at);
      const reclaiming = await publishing(async () => {
        let retired: ExtentRef[] = [];
        let kept = blobs;
        if (blobs.length > 0 && (random() < 0.35 || blobs.length > 150)) {
          const extents = extentsOf(blobs);
          const drop = new Set(extents.filter(() => random() < 0.4).map((extent) => extent.offset));
          retired = extents.filter((extent) => drop.has(extent.offset));
          kept = blobs.filter((blob) => !drop.has(blob.extent.offset));
        }
        const next = [...kept, ...written];
        const target = { round: round + 1, identity: identity(next) };
        const frees = retiredLast;
        const step = await begin({ step: frees ? "reclaim" : "publish", root: target });
        await space.publish(encodeRoot({ round: target.round, blobs: next }), {
          writers: [writer],
          retired,
        });
        round = target.round;
        blobs = next;
        ack({ ...state, round: target.round, identity: target.identity });
        retiredLast = retired.length > 0;
        end(step);
        return frees;
      });
      if (reclaiming) {
        const step = await begin({ step: "reclaim" });
        await settle();
        end(step);
      }
      await pause();
    }
  };

  const reader = async (): Promise<never> => {
    for (;;) {
      const at = await begin({ step: "read" });
      const pin = space.pin("chaos reader");
      const sample = blobs.filter(() => random() < 0.2).slice(0, 24);
      try {
        if (sample.length > 0) {
          const lease = await space.read(sample, { budget: countingBudget(Infinity) });
          lease.release();
        }
      } finally {
        pin.release();
      }
      end(at);
      await pause();
    }
  };

  const log = async (): Promise<never> => {
    for (;;) {
      const roll = random();
      if (roll < 0.5) {
        const at = await begin({ step: "log-append" });
        for (let count = 1 + Math.floor(random() * 4); count > 0; count--) {
          const offset = session.logLength();
          const length = 1 + Math.floor(random() * (random() < 0.1 ? 90_000 : 3_000));
          await session.appendLog(logBytes(state.generation, offset, length));
        }
        end(at);
      } else if (roll < 0.85) {
        const at = await begin({ step: "log-flush" });
        const length = session.logLength();
        await session.flushLog();
        ack({ ...state, flushed: length });
        end(at);
      } else if (roll < 0.97) {
        const to = Math.floor(random() * (session.logLength() + 1));
        const at = await begin({ step: "truncate", truncateTo: to });
        await session.truncateLog(to);
        ack({ ...state, flushed: Math.min(state.flushed, to) });
        end(at);
      } else {
        const at = await begin({ step: "reset" });
        const generation = state.generation + 1;
        await session.resetLog();
        await session.writeLogHeader(0, headerOf(generation));
        await session.flushLog();
        ack({ ...state, generation, flushed: 0 });
        end(at);
      }
      await pause();
    }
  };

  const spiller = async (): Promise<never> => {
    let query = 1;
    for (;;) {
      if (spill === null) return new Promise<never>(() => undefined);
      const at = await begin({ step: "spill" });
      const file = await spill.create(
        query++,
        random() < 0.5 ? countingBudget(1 << 20) : countingBudget(0),
      );
      const refs = [];
      for (let page = 0; page < 2 + Math.floor(random() * 10); page++) {
        const bytes = bytesOf(1 + Math.floor(random() * 50_000), page);
        refs.push([await file.append(bytes), bytes] as const);
      }
      for (const [ref, bytes] of refs)
        if (firstDifference(await file.read(ref), bytes) !== -1)
          fail("a spill page read back wrong");
      await file.close();
      end(at);
      await pause();
    }
  };

  const all: Record<string, () => Promise<never>> = {
    writer,
    writer2: writer,
    reader,
    log,
    spiller,
  };
  const lanes = Object.entries(all)
    .filter(([name]) => only === undefined || only.includes(name))
    .map(([, lane]) => lane());
  // The first failure ends the workload; the other lanes' later failures (after a crash, every
  // lane fails) are expected and not reported again.
  for (const lane of lanes) lane.catch(() => undefined);
  return Promise.race(lanes);
}
