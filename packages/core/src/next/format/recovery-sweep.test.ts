/**
 * Crash sweeps for the log, checkpoint, replay and ledger protocol, on the real store logic over
 * the simulated disk, crashing at every physical step. A minimal reference engine ("toy") drives
 * transactions made of ordered effects: table creation with rows in the same transaction,
 * several DDL statements in one transaction, a schema change between two row changes, create +
 * insert + drop in one transaction, multi-table and multipart commits, sequence reservations,
 * exactly-once request outcomes, per-table flushes, global checkpoints and a log recycle.
 * Recovery reads the log with the incremental scanner. After each crash the recovered state
 * (rows, versions, sequences, ledger) must equal the state after some prefix of commits that
 * includes every acknowledged one (honest flush), or after some prefix or a detected corruption
 * (lying flush). Recovery then resumes and a further commit must survive.
 */
import { describe, expect, it } from "vitest";
import { SimBackend, SimDisk, SimulatedCrash } from "../storage/sim-disk.js";
import type { StoreSession } from "../storage/contract.js";
import { RequestLedger, type ClientLedgerEntry } from "./ledger.js";
import {
  LogCorruptionError,
  encodeFrame,
  encodeLogHeader,
  newestLogHeader,
  scanLogIncremental,
  type LogIncarnation,
} from "./log-frame.js";
import { Replayer, type Effect } from "./replay.js";

const SEEDS = 10;
const FIRST_SALT = 0x1111;
const NOW = 1_000_000;

type ToyEffect =
  | { kind: "createTable"; tableId: number }
  | { kind: "alterTable"; tableId: number; schemaVersion: number }
  | { kind: "dropTable"; tableId: number }
  | { kind: "sequence"; sequenceId: number; value: number }
  | { kind: "rows"; tableId: number; schemaVersion: number; rows: string[] };

interface Body {
  readonly request: { clientId: string; sequence: number; rowCount: number } | null;
  readonly effects: ToyEffect[];
}

interface RootDoc {
  readonly sequence: number;
  readonly log: LogIncarnation;
  readonly globalLsn: number;
  readonly nextTableId: number;
  readonly tables: Array<{ id: number; version: number; flushed: number; rows: string[] }>;
  readonly sequences: Array<[number, number]>;
  readonly clients: ClientLedgerEntry[];
}

const EMPTY_ROOT: RootDoc = {
  sequence: 0,
  log: { salt: FIRST_SALT, firstLsn: 1, rootSequence: 0 },
  globalLsn: 0,
  nextTableId: 1,
  tables: [],
  sequences: [],
  clients: [],
};

interface TableState {
  version: number;
  flushed: number;
  /** LSN of the last schema change, for writer obligation 1. */
  alteredAt: number;
  durable: string[];
  memory: string[];
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function concat(parts: readonly Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function replayEffect(effect: ToyEffect): Effect {
  if (effect.kind === "sequence") return { kind: "sequence" };
  if (effect.kind === "rows")
    return { kind: "rows", tableId: effect.tableId, schemaVersion: effect.schemaVersion };
  return effect;
}

/** A minimal engine following replay.ts, log-frame.ts and ledger.ts. */
class Toy {
  readonly tables = new Map<number, TableState>();
  sequences = new Map<number, number>();
  nextTableId = 1;
  ledger = new RequestLedger();
  checkpointed = {
    globalLsn: 0,
    nextTableId: 1,
    versions: new Map<number, number>(),
    sequences: new Map<number, number>(),
    clients: [] as ClientLedgerEntry[],
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
      ledger: this.ledger
        .checkpoint()
        .map((client) => [client.clientId, client.outcomes.map((o) => [o.sequence, o.counts])]),
    });
  }

  /** Applies one effect to the live state (commit and replay share it). */
  apply(effect: ToyEffect, lsn: number): void {
    switch (effect.kind) {
      case "createTable":
        this.tables.set(effect.tableId, {
          version: 1,
          flushed: lsn - 1,
          alteredAt: 0,
          durable: [],
          memory: [],
        });
        this.nextTableId = effect.tableId + 1;
        return;
      case "alterTable": {
        const table = this.tables.get(effect.tableId);
        if (table !== undefined) {
          table.version = effect.schemaVersion;
          table.alteredAt = lsn;
        }
        return;
      }
      case "dropTable":
        this.tables.delete(effect.tableId);
        return;
      case "sequence":
        this.sequences.set(effect.sequenceId, effect.value);
        return;
      case "rows":
        this.tables.get(effect.tableId)?.memory.push(...effect.rows);
        return;
    }
  }

  #record(body: Body, lsn: number): void {
    if (body.request === null) return;
    const { clientId, sequence, rowCount } = body.request;
    this.ledger.admit({ clientId, sequence, sentAtMs: NOW, resolvedThrough: 0 }, NOW);
    this.ledger.record(clientId, sequence, {
      kind: "committed",
      lsn,
      counts: { rowCount },
      recordedAtMs: NOW,
    });
  }

  async commit(
    body: Body,
    parts: number,
    snapshots: string[],
    acknowledged: { count: number },
  ): Promise<void> {
    const bytes = encoder.encode(JSON.stringify(body));
    const size = Math.ceil(bytes.length / parts);
    // The state if this commit lands, recorded before any of its bytes are written.
    const lsn = this.lsn + parts;
    for (const effect of body.effects) this.apply(effect, lsn);
    this.#record(body, lsn);
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
      tables: [...this.checkpointed.versions].map(([id, version]) => {
        const table = this.tables.get(id);
        return { id, version, flushed: table?.flushed ?? 0, rows: table?.durable ?? [] };
      }),
      sequences: [...this.checkpointed.sequences],
      clients: this.checkpointed.clients,
    };
    await this.session.publish(encoder.encode(JSON.stringify(doc)));
  }

  #flushRows(table: TableState): void {
    table.durable.push(...table.memory);
    table.memory = [];
    table.flushed = this.lsn;
  }

  /** Global checkpoint. Obligation 1: tables altered since their last flush are flushed first. */
  async checkpoint(options: { skipObligation?: boolean } = {}): Promise<void> {
    if (options.skipObligation !== true)
      for (const table of this.tables.values())
        if (table.alteredAt > table.flushed) this.#flushRows(table);
    this.checkpointed = {
      globalLsn: this.lsn,
      nextTableId: this.nextTableId,
      versions: new Map([...this.tables].map(([id, t]) => [id, t.version])),
      sequences: new Map(this.sequences),
      clients: this.ledger.checkpoint(),
    };
    await this.#publish();
  }

  /** A memtable flush of one table (checkpointing global state first if the table is newer). */
  async flushTable(id: number): Promise<void> {
    const table = this.tables.get(id);
    if (table === undefined) return;
    if (this.checkpointed.versions.get(id) !== table.version) return this.checkpoint();
    this.#flushRows(table);
    await this.#publish();
  }

  /** Flush everything, switch salt in a root, then reset the log (replay.ts obligation 3). */
  async recycleLog(salt: number): Promise<void> {
    for (const table of this.tables.values()) this.#flushRows(table);
    this.checkpointed = {
      globalLsn: this.lsn,
      nextTableId: this.nextTableId,
      versions: new Map([...this.tables].map(([id, t]) => [id, t.version])),
      sequences: new Map(this.sequences),
      clients: this.ledger.checkpoint(),
    };
    const incarnation = { salt, firstLsn: this.lsn + 1, rootSequence: this.rootSequence + 1 };
    await this.#publish(incarnation);
    this.incarnation = incarnation;
    await this.session.resetLog();
    this.chain = salt;
    this.generation = 0;
    this.ack = { end: 0, chain: salt, lsn: this.lsn };
  }

  /** Recovery: root, incremental scan, coverage, replay; then truncate the invalid tail. */
  static async recover(session: StoreSession): Promise<Toy> {
    const rootBytes = await session.readRoot();
    const root =
      rootBytes === null ? EMPTY_ROOT : (JSON.parse(decoder.decode(rootBytes)) as RootDoc);
    const toy = new Toy(session);
    toy.rootSequence = root.sequence;
    toy.incarnation = root.log;
    toy.nextTableId = root.nextTableId;
    toy.sequences = new Map(root.sequences);
    toy.ledger = RequestLedger.restore(root.clients, NOW);
    for (const t of root.tables)
      toy.tables.set(t.id, {
        version: t.version,
        flushed: t.flushed,
        alteredAt: 0,
        durable: [...t.rows],
        memory: [],
      });
    toy.checkpointed = {
      globalLsn: root.globalLsn,
      nextTableId: root.nextTableId,
      versions: new Map(root.tables.map((t) => [t.id, t.version])),
      sequences: new Map(root.sequences),
      clients: root.clients,
    };
    const checkpoint = {
      globalLsn: root.globalLsn,
      nextTableId: root.nextTableId,
      tables: new Map(
        root.tables.map((t) => [t.id, { flushedThroughLsn: t.flushed, schemaVersion: t.version }]),
      ),
    };
    const replayer = new Replayer(checkpoint);
    let staged: Uint8Array[] = [];
    const summary = await scanLogIncremental(
      { length: session.logLength(), read: (offset, length) => session.readLog(offset, length) },
      await session.readLogHeaders(),
      root.log,
      {
        part(frame) {
          staged.push(frame.body.slice());
        },
        commit(_transaction, lsn) {
          const body = JSON.parse(decoder.decode(concat(staged))) as Body;
          staged = [];
          const decisions = replayer.next(lsn, body.effects.map(replayEffect));
          body.effects.forEach((effect, index) => {
            if (decisions[index] === true) toy.apply(effect, lsn);
          });
          if (body.request !== null && lsn > root.globalLsn) toy.#record(body, lsn);
        },
        abandon() {
          staged = [];
        },
      },
    );
    Replayer.checkCoverage(checkpoint, summary.lastLsn);
    await session.truncateLog(summary.validEnd);
    toy.lsn = summary.lastLsn;
    toy.chain = summary.chain;
    toy.generation = summary.header?.generation ?? 0;
    toy.ack = { end: summary.validEnd, chain: summary.chain, lsn: summary.lastLsn };
    return toy;
  }
}

const rows = (prefix: string, count: number) =>
  Array.from({ length: count }, (_, i) => `${prefix}${String(i)}`);
const insert = (tableId: number, schemaVersion: number, values: string[]): ToyEffect => ({
  kind: "rows",
  tableId,
  schemaVersion,
  rows: values,
});
let requestSequence = 0;
const tx = (effects: ToyEffect[], request = false): Body => ({
  request: request
    ? { clientId: "c1", sequence: ++requestSequence, rowCount: effects.length }
    : null,
  effects,
});

interface Options {
  readonly skipObligation?: boolean;
}

/** The workload: every protocol boundary at least once. */
async function workload(
  toy: Toy,
  snapshots: string[],
  acknowledged: { count: number },
  options: Options,
): Promise<void> {
  requestSequence = 0;
  const commit = (b: Body, parts = 1) => toy.commit(b, parts, snapshots, acknowledged);
  // Create and insert in one transaction (review A1-2 finding 4).
  await commit(tx([{ kind: "createTable", tableId: 1 }, insert(1, 1, rows("a", 2))], true));
  await commit(tx([{ kind: "createTable", tableId: 2 }]));
  await commit(tx([insert(1, 1, ["m1"]), insert(2, 1, ["m2"])], true));
  await commit(tx([{ kind: "sequence", sequenceId: 7, value: 32 }]));
  await toy.checkpoint(options);
  await commit(tx([insert(2, 1, rows("b", 40))]), 3);
  await toy.flushTable(1);
  // A schema change between two row changes of one transaction.
  await commit(
    tx(
      [
        insert(1, 1, ["c"]),
        { kind: "alterTable", tableId: 1, schemaVersion: 2 },
        insert(1, 2, ["e"]),
        { kind: "sequence", sequenceId: 7, value: 64 },
      ],
      true,
    ),
  );
  // Several DDL statements in one transaction, with rows.
  await commit(
    tx([
      { kind: "createTable", tableId: 3 },
      { kind: "createTable", tableId: 4 },
      { kind: "alterTable", tableId: 3, schemaVersion: 2 },
      insert(3, 2, ["x"]),
      insert(4, 1, ["y"]),
    ]),
  );
  await toy.checkpoint(options);
  // Create, insert and drop in one transaction; then a drop of an older table.
  await commit(
    tx([
      { kind: "createTable", tableId: 5 },
      insert(5, 1, ["gone"]),
      { kind: "dropTable", tableId: 5 },
    ]),
  );
  await commit(tx([{ kind: "dropTable", tableId: 2 }], true));
  await commit(tx([insert(3, 2, rows("f", 30)), insert(1, 2, ["g"])]), 2);
  await toy.recycleLog(0x2222);
  await commit(tx([insert(3, 2, ["h"]), { kind: "alterTable", tableId: 4, schemaVersion: 2 }]));
  await toy.flushTable(3);
  await commit(tx([insert(1, 2, ["i"]), insert(4, 2, ["z"])], true));
  await toy.checkpoint(options);
  await commit(tx([insert(4, 2, ["w"])]));
}

const EMPTY_STATE = JSON.stringify({ tables: [], sequences: [], ledger: [] });

async function run(
  backend: SimBackend,
  snapshots: string[],
  acknowledged: { count: number },
  options: Options = {},
) {
  snapshots.push(EMPTY_STATE);
  const toy = await Toy.recover(await backend.open(await backend.acquire()));
  await workload(toy, snapshots, acknowledged, options);
}

async function sweep(flush: "honest" | "lying", options: Options = {}): Promise<number> {
  const cleanDisk = new SimDisk();
  const cleanBackend = new SimBackend(cleanDisk);
  const cleanSnapshots: string[] = [];
  await run(cleanBackend, cleanSnapshots, { count: 0 }, options);
  const totalSteps = cleanDisk.steps;
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
      await expect(run(backend, snapshots, acknowledged, options)).rejects.toThrow(SimulatedCrash);
      backend.restart();
      const label = `${flush} step ${String(step)} seed ${String(seed)}`;
      let recovered: Toy;
      try {
        recovered = await Toy.recover(await backend.open(await backend.acquire()));
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
      const resumed: string[] = [state];
      await recovered.commit(
        tx([{ kind: "sequence", sequenceId: 99, value: step + 1 }]),
        1,
        resumed,
        {
          count: 0,
        },
      );
      disk.powerLoss();
      backend.restart();
      let again: string;
      try {
        again = (await Toy.recover(await backend.open(await backend.acquire()))).logical();
      } catch (error) {
        if (flush === "lying" && error instanceof LogCorruptionError) continue;
        throw error;
      }
      if (flush === "honest") expect(again, `${label}: resumed commit`).toBe(resumed.at(-1));
      else expect(resumed, `${label}: resumed commit`).toContain(again);
    }
  return detected;
}

describe("log, checkpoint, replay and ledger under every physical crash point", () => {
  it("honest flush: every acknowledged commit survives, nothing else appears", async () => {
    await sweep("honest");
  });

  it("lying flush: a prefix survives or the loss is detected", async () => {
    expect(await sweep("lying")).toBeGreaterThan(0);
  });

  it("a checkpoint that skips writer obligation 1 is caught as corruption, not silent", async () => {
    await expect(sweep("honest", { skipObligation: true })).rejects.toThrow(
      /written under schema/u,
    );
  });
});
