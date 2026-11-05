/**
 * Crash sweeps for the log, checkpoint and replay protocol, on the real store logic over the
 * simulated disk, crashing at every physical step. A minimal reference engine ("toy") drives:
 * table creation, single- and multi-table commits, multipart transactions, sequence-only
 * commits, a schema change (flush first), a drop, per-table flushes, global checkpoints and a log
 * recycle. After each crash the recovered state must equal the state after some prefix of commits
 * that includes every acknowledged one (honest flush), or after some prefix or a detected
 * corruption (lying flush). Recovery then resumes and a further commit must survive.
 */
import { describe, expect, it } from "vitest";
import { SimBackend, SimDisk, SimulatedCrash } from "../storage/sim-disk.js";
import type { StoreSession } from "../storage/contract.js";
import {
  LogCorruptionError,
  encodeFrame,
  encodeLogHeader,
  newestLogHeader,
  scanLog,
  type LogIncarnation,
} from "./log-frame.js";
import { Replayer, type TransactionEffects } from "./replay.js";

const SEEDS = 12;
const FIRST_SALT = 0x1111;

type Catalog =
  | { kind: "createTable"; tableId: number }
  | { kind: "alterTable"; tableId: number; schemaVersion: number }
  | { kind: "dropTable"; tableId: number }
  | null;

interface Body {
  readonly catalog: Catalog;
  readonly sequences: Array<[number, number]>;
  readonly tables: Array<{ tableId: number; schemaVersion: number; rows: string[] }>;
}

interface RootDoc {
  readonly sequence: number;
  readonly log: LogIncarnation;
  readonly globalLsn: number;
  readonly nextTableId: number;
  readonly tables: Array<{ id: number; version: number; flushed: number; rows: string[] }>;
  readonly sequences: Array<[number, number]>;
}

const EMPTY_ROOT: RootDoc = {
  sequence: 0,
  log: { salt: FIRST_SALT, firstLsn: 1, rootSequence: 0 },
  globalLsn: 0,
  nextTableId: 1,
  tables: [],
  sequences: [],
};

interface TableState {
  version: number;
  flushed: number;
  durable: string[];
  memory: string[];
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** A minimal engine following the protocol in replay.ts and log-frame.ts. */
class Toy {
  readonly tables = new Map<number, TableState>();
  sequences = new Map<number, number>();
  nextTableId = 1;
  /** Global state as of the last checkpoint (what the root may claim). */
  checkpointed = {
    globalLsn: 0,
    nextTableId: 1,
    versions: new Map<number, number>(),
    sequences: new Map<number, number>(),
  };
  rootSequence = 0;
  incarnation: LogIncarnation = EMPTY_ROOT.log;
  lsn = 0;
  chain = FIRST_SALT;
  generation = 0;
  ack = { end: 0, chain: FIRST_SALT, lsn: 0 };

  constructor(readonly session: StoreSession) {}

  logical(): string {
    return JSON.stringify({
      tables: [...this.tables]
        .sort(([a], [b]) => a - b)
        .map(([id, t]) => [id, t.version, [...t.durable, ...t.memory]]),
      sequences: [...this.sequences].sort(([a], [b]) => a - b),
    });
  }

  #apply(body: Body, lsn: number): void {
    const change = body.catalog;
    if (change?.kind === "createTable") {
      this.tables.set(change.tableId, { version: 1, flushed: lsn, durable: [], memory: [] });
      this.nextTableId = change.tableId + 1;
    } else if (change?.kind === "alterTable") {
      const table = this.tables.get(change.tableId);
      if (table !== undefined) table.version = change.schemaVersion;
    } else if (change?.kind === "dropTable") this.tables.delete(change.tableId);
    for (const [id, value] of body.sequences) this.sequences.set(id, value);
    for (const change of body.tables) this.tables.get(change.tableId)?.memory.push(...change.rows);
  }

  /** Appends one transaction (split into `parts` frames), acknowledges the previous group, flushes. */
  async commit(
    body: Body,
    parts: number,
    snapshots: string[],
    acknowledged: { count: number },
  ): Promise<void> {
    const bytes = encoder.encode(JSON.stringify(body));
    const size = Math.ceil(bytes.length / parts);
    // The state if this commit lands, recorded before any of its bytes are written.
    this.#apply(body, this.lsn + parts);
    snapshots.push(this.logical());
    for (let part = 0; part < parts; part++) {
      const frame = encodeFrame(
        { salt: this.incarnation.salt, lsn: ++this.lsn, partIndex: part, partCount: parts },
        bytes.subarray(part * size, (part + 1) * size),
        this.chain,
      );
      this.chain = frame.chain;
      await this.session.appendLog(frame.bytes);
    }
    const { nextSlot } = newestLogHeader(
      await this.session.readLogHeaders(),
      this.incarnation.salt,
    );
    await this.session.writeLogHeader(
      nextSlot,
      encodeLogHeader({
        salt: this.incarnation.salt,
        generation: ++this.generation,
        ackEnd: this.ack.end,
        ackChain: this.ack.chain,
        ackLsn: this.ack.lsn,
        rootSequence: this.incarnation.rootSequence,
      }),
    );
    await this.session.flushLog();
    acknowledged.count = snapshots.length - 1;
    this.ack = { end: this.session.logLength(), chain: this.chain, lsn: this.lsn };
  }

  async #publish(log: LogIncarnation = this.incarnation): Promise<void> {
    const doc: RootDoc = {
      sequence: ++this.rootSequence,
      log,
      globalLsn: this.checkpointed.globalLsn,
      nextTableId: this.checkpointed.nextTableId,
      // The root lists the tables of the checkpointed catalog, with their own watermarks.
      tables: [...this.checkpointed.versions].map(([id, version]) => {
        const table = this.tables.get(id);
        return { id, version, flushed: table?.flushed ?? 0, rows: table?.durable ?? [] };
      }),
      sequences: [...this.checkpointed.sequences],
    };
    await this.session.publish(encoder.encode(JSON.stringify(doc)));
  }

  /** Global checkpoint: the root's catalog, sequences and globalLsn catch up. */
  async checkpoint(): Promise<void> {
    this.checkpointed = {
      globalLsn: this.lsn,
      nextTableId: this.nextTableId,
      versions: new Map([...this.tables].map(([id, t]) => [id, t.version])),
      sequences: new Map(this.sequences),
    };
    await this.#publish();
  }

  /** A memtable flush of one table (checkpointing global state first if the table is newer). */
  async flushTable(id: number): Promise<void> {
    const table = this.tables.get(id);
    if (table === undefined) return;
    if (!this.checkpointed.versions.has(id)) {
      await this.checkpoint();
    }
    table.durable.push(...table.memory);
    table.memory = [];
    table.flushed = this.lsn;
    await this.#publish();
  }

  /** Flush everything, switch salt in a root, then reset the log (replay.ts obligation 3). */
  async recycleLog(salt: number): Promise<void> {
    for (const table of this.tables.values()) {
      table.durable.push(...table.memory);
      table.memory = [];
      table.flushed = this.lsn;
    }
    this.checkpointed = {
      globalLsn: this.lsn,
      nextTableId: this.nextTableId,
      versions: new Map([...this.tables].map(([id, t]) => [id, t.version])),
      sequences: new Map(this.sequences),
    };
    const incarnation = { salt, firstLsn: this.lsn + 1, rootSequence: this.rootSequence + 1 };
    await this.#publish(incarnation);
    this.incarnation = incarnation;
    await this.session.resetLog();
    this.chain = salt;
    this.generation = 0;
    this.ack = { end: 0, chain: salt, lsn: this.lsn };
  }

  /** Recovery: root, scan, coverage, replay; then truncate the invalid tail. */
  static async recover(session: StoreSession): Promise<Toy> {
    const rootBytes = await session.readRoot();
    const root =
      rootBytes === null ? EMPTY_ROOT : (JSON.parse(decoder.decode(rootBytes)) as RootDoc);
    const toy = new Toy(session);
    toy.rootSequence = root.sequence;
    toy.incarnation = root.log;
    toy.nextTableId = root.nextTableId;
    toy.sequences = new Map(root.sequences);
    for (const t of root.tables)
      toy.tables.set(t.id, {
        version: t.version,
        flushed: t.flushed,
        durable: [...t.rows],
        memory: [],
      });
    toy.checkpointed = {
      globalLsn: root.globalLsn,
      nextTableId: root.nextTableId,
      versions: new Map(root.tables.map((t) => [t.id, t.version])),
      sequences: new Map(root.sequences),
    };
    const checkpoint = {
      globalLsn: root.globalLsn,
      nextTableId: root.nextTableId,
      tables: new Map(
        root.tables.map((t) => [t.id, { flushedThroughLsn: t.flushed, schemaVersion: t.version }]),
      ),
    };
    const scan = scanLog(
      await session.readLog(0, session.logLength()),
      await session.readLogHeaders(),
      root.log,
    );
    Replayer.checkCoverage(checkpoint, scan.lastLsn);
    const replayer = new Replayer(checkpoint);
    let parts: Uint8Array[] = [];
    for (const frame of scan.frames) {
      parts.push(frame.body.slice());
      if (frame.partIndex !== frame.partCount - 1) continue;
      const body = JSON.parse(decoder.decode(concat(parts))) as Body;
      parts = [];
      const effects: TransactionEffects = {
        catalog: body.catalog,
        otherGlobal: body.sequences.length > 0,
        tables: body.tables.map(({ tableId, schemaVersion }) => ({ tableId, schemaVersion })),
      };
      const decision = replayer.next(frame.lsn, effects);
      const change = body.catalog;
      if (decision.global) {
        if (change?.kind === "createTable")
          toy.tables.set(change.tableId, {
            version: 1,
            flushed: frame.lsn,
            durable: [],
            memory: [],
          });
        if (change?.kind === "alterTable") {
          const table = toy.tables.get(change.tableId);
          if (table !== undefined) table.version = change.schemaVersion;
        }
        if (change?.kind === "createTable") toy.nextTableId = change.tableId + 1;
        for (const [id, value] of body.sequences) toy.sequences.set(id, value);
      }
      if (decision.dropped !== null) toy.tables.delete(decision.dropped);
      for (const tableId of decision.tables)
        toy.tables
          .get(tableId)
          ?.memory.push(...(body.tables.find((t) => t.tableId === tableId)?.rows ?? []));
    }
    await session.truncateLog(scan.validEnd);
    toy.lsn = scan.lastLsn;
    toy.chain = scan.chain;
    toy.generation = scan.header?.generation ?? 0;
    toy.ack = { end: scan.validEnd, chain: scan.chain, lsn: scan.lastLsn };
    return toy;
  }
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

const rows = (prefix: string, count: number) =>
  Array.from({ length: count }, (_, i) => `${prefix}${String(i)}`);
const insert = (tableId: number, schemaVersion: number, values: string[]) => ({
  tableId,
  schemaVersion,
  rows: values,
});
const body = (fields: Partial<Body>): Body => ({
  catalog: null,
  sequences: [],
  tables: [],
  ...fields,
});

/** The workload: every protocol boundary at least once. */
async function workload(
  toy: Toy,
  snapshots: string[],
  acknowledged: { count: number },
): Promise<void> {
  const commit = (b: Body, parts = 1) => toy.commit(b, parts, snapshots, acknowledged);
  await commit(body({ catalog: { kind: "createTable", tableId: 1 } }));
  await commit(body({ catalog: { kind: "createTable", tableId: 2 } }));
  await commit(body({ tables: [insert(1, 1, rows("a", 2))] }));
  await commit(body({ tables: [insert(1, 1, ["m1"]), insert(2, 1, ["m2"])] }));
  await commit(body({ sequences: [[7, 32]] }));
  await toy.checkpoint();
  await commit(body({ tables: [insert(2, 1, rows("b", 40))] }), 3);
  await toy.flushTable(1);
  await commit(body({ tables: [insert(1, 1, ["c"]), insert(2, 1, ["d"])] }));
  // ALTER: flush first (replay.ts obligation 1), then the DDL frame, then new-schema rows.
  await toy.flushTable(1);
  await commit(body({ catalog: { kind: "alterTable", tableId: 1, schemaVersion: 2 } }));
  await commit(body({ tables: [insert(1, 2, ["e"])], sequences: [[7, 64]] }));
  await commit(body({ catalog: { kind: "dropTable", tableId: 2 } }));
  await commit(body({ catalog: { kind: "createTable", tableId: 3 } }));
  await commit(body({ tables: [insert(3, 1, rows("f", 30)), insert(1, 2, ["g"])] }), 2);
  await toy.recycleLog(0x2222);
  await commit(body({ tables: [insert(3, 1, ["h"])] }));
  await toy.flushTable(3);
  await commit(body({ tables: [insert(1, 2, ["i"])] }));
}

const EMPTY_STATE = JSON.stringify({ tables: [], sequences: [] });

/** Open, recover (an empty store), then the workload: every physical step is a crash point. */
async function run(backend: SimBackend, snapshots: string[], acknowledged: { count: number }) {
  snapshots.push(EMPTY_STATE);
  const toy = await Toy.recover(await backend.open(await backend.acquire()));
  await workload(toy, snapshots, acknowledged);
}

describe("log, checkpoint and replay under every physical crash point", () => {
  for (const flush of ["honest", "lying"] as const)
    it(`${flush} flush`, async () => {
      const cleanDisk = new SimDisk();
      const cleanBackend = new SimBackend(cleanDisk);
      const cleanSnapshots: string[] = [];
      await run(cleanBackend, cleanSnapshots, { count: 0 });
      const totalSteps = cleanDisk.steps;
      // The clean run recovers to its final state.
      cleanDisk.powerLoss();
      cleanBackend.restart();
      const final = await Toy.recover(await cleanBackend.open(await cleanBackend.acquire()));
      expect(final.logical()).toBe(cleanSnapshots.at(-1));
      expect(totalSteps).toBeGreaterThan(60);

      let detected = 0;
      for (let step = 0; step < totalSteps; step++)
        for (let seed = 1; seed <= SEEDS; seed++) {
          const disk = new SimDisk({ crashAtStep: step, seed: seed * 104_729 + step, flush });
          const backend = new SimBackend(disk);
          const snapshots: string[] = [];
          const acknowledged = { count: 0 };
          await expect(run(backend, snapshots, acknowledged)).rejects.toThrow(SimulatedCrash);
          backend.restart();
          const label = `${flush} step ${String(step)} seed ${String(seed)}`;
          const reopened = await backend.open(await backend.acquire());
          let recovered: Toy;
          try {
            recovered = await Toy.recover(reopened);
          } catch (error) {
            if (flush === "lying" && error instanceof LogCorruptionError) {
              detected += 1;
              continue;
            }
            throw new Error(`${label}: ${String(error)}`, { cause: error });
          }
          const state = recovered.logical();
          const index = snapshots.lastIndexOf(state);
          expect(
            index,
            `${label}: recovered a state no prefix of commits produces`,
          ).toBeGreaterThanOrEqual(0);
          if (flush === "honest")
            expect(index, `${label}: lost an acknowledged commit`).toBeGreaterThanOrEqual(
              acknowledged.count,
            );
          // Resume after recovery: an independent commit lands after any abandoned prefix.
          const resumed: string[] = [state];
          await recovered.commit(body({ sequences: [[99, step + 1]] }), 1, resumed, { count: 0 });
          disk.powerLoss();
          backend.restart();
          let again: string;
          try {
            again = (await Toy.recover(await backend.open(await backend.acquire()))).logical();
          } catch (error) {
            if (flush === "lying" && error instanceof LogCorruptionError) continue;
            throw error;
          }
          // Honest: the resumed commit survives. Lying: it may be lost, nothing else may appear.
          if (flush === "honest") expect(again, `${label}: resumed commit`).toBe(resumed.at(-1));
          else expect(resumed, `${label}: resumed commit`).toContain(again);
        }
      if (flush === "lying") expect(detected).toBeGreaterThan(0);
    });
});
