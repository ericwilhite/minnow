/**
 * How the consuming tracks call the storage access contracts (access/): runnable examples over
 * small stand-ins. The executor reads (point read, index nested-loop join), writes (upsert), and
 * the owner runs an autocommit write and a read with the delivery rule.
 */
import { describe, expect, it } from "vitest";
import type { CatalogView, IndexDef } from "../access/catalog.js";
import {
  type IndexReader,
  type MaybePromise,
  type ProbeOutput,
  type ProbeResult,
} from "../access/key-index.js";
import {
  CardinalityViolationError,
  ConflictKind,
  type ConflictOutput,
  type Statement,
  type TableRules,
  type TableWriter,
} from "../access/modify.js";
import {
  MEMORY_CHUNK_ROWS,
  readBound,
  resumeRow,
  type ChunkOutput,
  type ChunkResult,
  type DeletionVector,
  type MemorySegment,
  type Snapshot,
  type TableSnapshot,
} from "../access/snapshot.js";
import type {
  CommitReceipt,
  CommitRequest,
  SequenceAccess,
  WritePath,
  WriteTransaction,
} from "../access/write-path.js";
import { MemoryRowFetcher, type RowFetcher } from "../exec/fetch.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import { packLocator } from "../vector/locator.js";
import { allocateSelection } from "../vector/validity.js";
import { decodeUtf8, VarlenBuilder } from "../vector/varlen.js";
import { allocateFlat, type Batch, type FlatVector, type Selection } from "../vector/vector.js";

const INT: PhysicalType = { kind: "int32", bits: 32 };
const TABLE = 1;
const PRIMARY = 10;
const BY_CUSTOMER = 11;

function ints(values: readonly number[]): FlatVector {
  return {
    form: "flat",
    type: INT,
    length: values.length,
    validity: null,
    data: { layout: "i32", values: Int32Array.from(values) },
  };
}

function locators(values: Float64Array, length: number): FlatVector {
  return {
    form: "flat",
    type: { kind: "locator", tableId: TABLE },
    length,
    validity: null,
    data: { layout: "f64", values },
  };
}

/** Key buffers in these examples are tiny; a real operator charges its grant. */
const keyCharge = { allocate: () => true, free: () => undefined };

const settle = async <T>(value: MaybePromise<T>): Promise<T> => value;

// ---------------------------------------------------------------------------------------------
// Stand-ins: one table (id INT PRIMARY KEY, customer INT) in one memory segment.

const ROWS: ReadonlyArray<readonly [number, number]> = [
  [1, 7],
  [2, 7],
  [3, 8],
  [4, 7],
];
const SEGMENT = new Map<number, number[]>([
  [0, ROWS.map(([id]) => id)],
  [1, ROWS.map(([, customer]) => customer)],
]);

function indexDef(id: number, column: number, unique: boolean): IndexDef {
  return {
    id,
    name: `index${String(id)}`,
    tableId: TABLE,
    columnIds: [column],
    unique,
    kind: "ordered",
    role: unique ? "primary" : "secondary",
  };
}

/** An index over `SEGMENT[column]`, honouring the probe contract (output capacity, resume). */
function memoryIndex(def: IndexDef): IndexReader {
  const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");
  const byKey = new Map<string, number[]>();
  const probeKeys = new KeyBatch(keyCharge);
  const column = SEGMENT.get(def.columnIds[0] ?? 0) ?? [];
  encodeKeyBatch(
    "equality",
    [{ vector: ints(column), direction: "asc", nulls: "last" }],
    {
      length: column.length,
      selection: null,
    },
    probeKeys,
  );
  column.forEach((_, position) => {
    const key = hex(probeKeys.key(position));
    byKey.set(key, [...(byKey.get(key) ?? []), packLocator(0, position)]);
  });
  return {
    def,
    probe(keys, out, resume): ProbeResult {
      const capacity = Math.min(out.probes.length, out.locators.length);
      out.count = 0;
      for (let k = resume?.key ?? 0; k < keys.count; k++) {
        const matches = byKey.get(hex(keys.key(k))) ?? [];
        if (out.count + matches.length > capacity) return { done: false, resume: { key: k } };
        for (const locator of matches) {
          out.probes[out.count] = k;
          out.locators[out.count++] = locator;
        }
      }
      return { done: true };
    },
    range() {
      throw new Error("not used by these examples");
    },
  };
}

const catalog: CatalogView = {
  version: 1,
  tables: [],
  sequences: [],
  views: [],
  types: [],
  table: () => undefined,
  tableByName: () => undefined,
  sequence: () => undefined,
  type: () => undefined,
  typeByName: () => undefined,
};

/** Deleted positions of the stand-in segment: row 1 (id 2) was deleted. */
const DELETED = new Set([1]);
const deletions: DeletionVector = {
  deletedCount: DELETED.size,
  isDeleted: (position) => DELETED.has(position),
  liveRows(from, length, input, out) {
    let count = 0;
    const total = input === null ? length : input.count;
    for (let k = 0; k < total; k++) {
      const row = input === null ? k : (input.rows[k] ?? 0);
      if (!DELETED.has(from + row)) out.rows[count++] = row;
    }
    out.count = count;
  },
};

function memoryTable(): TableSnapshot {
  const segment: MemorySegment = {
    kind: "memory",
    readHint: () => {
      throw new TypeError("This legacy scan fixture does not implement bounded reads");
    },
    index: 0,
    rowCount: ROWS.length,
    liveRowCount: ROWS.length - DELETED.size,
    deletions: () => deletions,
    statistics: () => null,
    pages: () => [
      {
        firstRow: 0,
        rowCount: ROWS.length,
        nullCount: 0,
        min: null,
        max: null,
        dictionarySize: null,
      },
    ],
    chunk(columnId): ChunkResult {
      // Resident: a borrowed vector of the whole chunk (valid until another chunk index is read).
      return {
        vector: ints(SEGMENT.get(columnId) ?? []),
        firstRow: 0,
        rowCount: ROWS.length,
        complete: true,
        reason: null,
      };
    },
  };
  return {
    tableId: TABLE,
    schemaVersion: 1,
    columns: [],
    liveRowCount: segment.liveRowCount,
    segments: [segment],
  };
}

function readSnapshot(): Snapshot {
  const indexes = new Map([
    [PRIMARY, memoryIndex(indexDef(PRIMARY, 0, true))],
    [BY_CUSTOMER, memoryIndex(indexDef(BY_CUSTOMER, 1, false))],
  ]);
  return {
    rootSequence: 1,
    released: false,
    bound: readBound(5),
    observedLsn: 5,
    catalog,
    pages: {
      read: () => Promise.reject(new Error("The stand-in has no groups")),
      peek: () => null,
    },
    table: () => memoryTable(),
    index: (_table, indexId) => {
      const reader = indexes.get(indexId);
      if (reader === undefined) throw new Error("unknown index");
      return reader;
    },
    release() {
      // A stand-in pin holds nothing.
    },
  };
}

async function smallGrant(): Promise<Grant> {
  return new Governor(64 << 20).admit({
    label: "example",
    minimumBytes: 1 << 16,
    spillable: false,
  });
}

// ---------------------------------------------------------------------------------------------
// Executor: `SELECT customer FROM t WHERE id = $1` → keyLookup on the primary key, then fetch.

async function pointRead(
  view: Snapshot,
  fetcher: RowFetcher,
  grant: Grant,
  id: number,
): Promise<number | null> {
  const keys = new KeyBatch(keyCharge); // reused across executions of the prepared plan
  encodeKeyBatch(
    "equality",
    [{ vector: ints([id]), direction: "asc", nulls: "last" }],
    {
      length: 1,
      selection: null,
    },
    keys,
  );
  const out: ProbeOutput = { probes: new Int32Array(1), locators: new Float64Array(1), count: 0 };
  await settle(view.index(TABLE, PRIMARY).probe(keys, out)); // unique, capacity ≥ keys: one call
  if (out.count === 0) return null;
  const customer = ints([0]);
  await fetcher.fetch({
    snapshot: view, // the access snapshot the locators came from
    tableId: TABLE,
    locators: locators(out.locators, out.count),
    length: out.count,
    selection: null,
    from: 0,
    capacity: 1,
    columns: [1],
    output: [customer],
    varlen: [null],
    grant,
  });
  return customer.data.layout === "i32" ? (customer.data.values[0] ?? null) : null;
}

// Executor: index nested-loop join `orders o JOIN t ON t.customer = o.customer`, probing a
// non-unique index with a deliberately small output, resuming until done.

async function joinProbe(view: Snapshot, probeCustomers: readonly number[]): Promise<number[][]> {
  const keys = new KeyBatch(keyCharge);
  encodeKeyBatch(
    "equality",
    [{ vector: ints(probeCustomers), direction: "asc", nulls: "last" }],
    {
      length: probeCustomers.length,
      selection: null,
    },
    keys,
  );
  const out: ProbeOutput = { probes: new Int32Array(3), locators: new Float64Array(3), count: 0 };
  const pairs: number[][] = [];
  const reader = view.index(TABLE, BY_CUSTOMER);
  let result = await settle(reader.probe(keys, out));
  for (;;) {
    for (let m = 0; m < out.count; m++) pairs.push([out.probes[m] ?? -1, out.locators[m] ?? -1]);
    // …emit the joined batch downstream here, then reuse `out`…
    if (result.done) return pairs;
    result = await settle(reader.probe(keys, out, result.resume));
  }
}

// Executor: `SELECT sum(id) FROM t`: a scan over the segments in locator order, filtering
// deleted rows. Group segments go through the format track's chunk reader over `view.pages`
// (not shown: the stand-in has none); memory chunks are borrowed, zero-copy vectors.

async function sumIds(view: Snapshot, budget: Grant): Promise<number> {
  const table = await settle(view.table(TABLE, { budget }));
  const out = { flat: allocateFlat(INT), codes: new Int32Array(MEMORY_CHUNK_ROWS), varlen: null };
  const live = allocateSelection(MEMORY_CHUNK_ROWS);
  let sum = 0;
  for (const segment of table.segments) {
    if (segment.kind === "group") throw new Error("read through the format chunk reader");
    const deleted = await settle(segment.deletions());
    for (let chunk = 0; chunk * MEMORY_CHUNK_ROWS < segment.rowCount; chunk++) {
      const from = chunk * MEMORY_CHUNK_ROWS;
      const length = Math.min(MEMORY_CHUNK_ROWS, segment.rowCount - from);
      const result = await settle(segment.chunk(0, chunk, out));
      const vector = result.vector;
      // An int32 column never stops early: only varlen output can be partial (see below).
      if (!result.complete || vector.form !== "flat" || vector.data.layout !== "i32")
        throw new Error("an int32 column reads as one complete flat int32 vector here");
      if (deleted === null) live.count = 0;
      else deleted.liveRows(from, length, null, live);
      const count = deleted === null ? length : live.count;
      for (let k = 0; k < count; k++)
        sum += vector.data.values[deleted === null ? k : (live.rows[k] ?? 0)] ?? 0;
    }
  }
  return sum;
}

// A re-pointed memory segment: `chunk()` decodes text into the caller's varlen builder, which
// may fill up mid-chunk. The executor emits what it has and resumes at `resumeRow(result)`.

const TEXT: PhysicalType = { kind: "text" };
const WORDS = ["aaaa", "bbbb", "cccc", "dddd"];

/** The stand-in's decode path (what the write path does after a re-point). */
function repointedChunk(out: ChunkOutput, startRow = 0): Promise<ChunkResult> {
  const builder = out.varlen;
  if (builder === null) throw new TypeError("text needs a varlen builder");
  if (!(startRow >= 0 && startRow < WORDS.length)) throw new RangeError("startRow out of range");
  let row = startRow;
  let reason: "full" | "denied" | null = null;
  for (; row < WORDS.length; row++) {
    const appended = builder.appendString(WORDS[row] ?? "");
    if (appended !== "ok") {
      reason = appended;
      break;
    }
  }
  const rowCount = row - startRow;
  const vector: FlatVector = {
    form: "flat",
    type: TEXT,
    length: rowCount,
    validity: null,
    data: builder.data(),
  };
  return Promise.resolve({
    vector,
    firstRow: startRow,
    rowCount,
    complete: reason === null,
    reason,
  });
}

/** The executor's loop over one re-pointed chunk: emit each part, reset, resume. */
async function readTextChunk(
  read: (out: ChunkOutput, startRow: number) => MaybePromise<ChunkResult>,
  out: ChunkOutput,
  emit: (firstRow: number, values: string[]) => void,
): Promise<number> {
  let calls = 0;
  let startRow: number | null = 0;
  while (startRow !== null) {
    const result = await settle(read(out, startRow));
    calls += 1;
    const vector = result.vector;
    if (vector.form !== "flat" || vector.data.layout !== "varlen") throw new TypeError("text");
    const data = vector.data;
    emit(
      result.firstRow,
      Array.from({ length: result.rowCount }, (_, k) => decodeUtf8(data, k)),
    );
    // `denied` would borrow a progress loan here before resuming.
    out.varlen?.reset();
    startRow = resumeRow(result);
  }
  return calls;
}

describe("executor reads through the access contracts", () => {
  it("point read: probe the primary key, fetch through the same pin", async () => {
    const view = readSnapshot();
    const fetcher = new MemoryRowFetcher([SEGMENT]);
    const grant = await smallGrant();
    expect(await pointRead(view, fetcher, grant, 3)).toBe(8);
    expect(await pointRead(view, fetcher, grant, 99)).toBeNull();
  });

  it("memory chunk: a partial read resumes at its start row in a second call", async () => {
    // An 8-byte batch target: two 4-byte words fit, the third makes the builder full.
    const varlen = new VarlenBuilder(MEMORY_CHUNK_ROWS, await smallGrant(), 8, 16);
    const out: ChunkOutput = {
      flat: allocateFlat(TEXT),
      codes: new Int32Array(MEMORY_CHUNK_ROWS),
      varlen,
    };
    const parts: Array<[number, string[]]> = [];
    const calls = await readTextChunk(repointedChunk, out, (firstRow, values) =>
      parts.push([firstRow, values]),
    );
    expect(calls).toBe(2);
    expect(parts).toEqual([
      [0, ["aaaa", "bbbb"]],
      [2, ["cccc", "dddd"]],
    ]);
    const first = await repointedChunk(out, 0);
    expect([first.complete, first.reason, resumeRow(first)]).toEqual([false, "full", 2]);
    varlen.reset();
    const last = await repointedChunk(out, 2);
    expect([last.complete, last.reason, resumeRow(last)]).toEqual([true, null, null]);
  });

  it("scan: segments in locator order, deleted rows filtered", async () => {
    expect(await sumIds(readSnapshot(), await smallGrant())).toBe(1 + 3 + 4);
  });

  it("join probe: matches in probe order, resumed when the output fills", async () => {
    const pairs = await joinProbe(readSnapshot(), [8, 7, 9]);
    expect(pairs).toEqual([
      [0, packLocator(0, 2)],
      [1, packLocator(0, 0)],
      [1, packLocator(0, 1)],
      [1, packLocator(0, 3)],
    ]);
  });
});

// ---------------------------------------------------------------------------------------------
// Executor: `INSERT INTO t VALUES … ON CONFLICT (id) DO UPDATE SET customer = EXCLUDED.customer`.

async function upsert(statement: Statement, proposed: Batch): Promise<Selection> {
  const writer = statement.table(TABLE, { assigned: [1], onRepeat: "error" });
  const n = proposed.length;
  const out: ConflictOutput = {
    kinds: new Uint8Array(n),
    locators: new Float64Array(n),
    earlier: new Int32Array(n),
    indexIds: new Int32Array(n),
  };
  await settle(writer.conflicts(proposed, [PRIMARY], out));
  const inserts = allocateSelection(n);
  const updates = allocateSelection(n);
  for (let row = 0; row < n; row++) {
    const kind = out.kinds[row];
    if (kind === ConflictKind.none) inserts.rows[inserts.count++] = row;
    else if (kind === ConflictKind.existing) updates.rows[updates.count++] = row;
    else throw new CardinalityViolationError(TABLE, row); // DO NOTHING would skip instead
  }
  const written = allocateSelection(n);
  await settle(writer.insert({ ...proposed, selection: inserts }, written));
  // DO UPDATE: evaluate SET (and WHERE) over the existing rows (fetched by `out.locators`) and
  // EXCLUDED; here SET customer = EXCLUDED.customer needs no fetch.
  const target = locators(out.locators, n);
  const update: Batch = {
    length: n,
    columns: [target, proposed.columns[1] ?? ints([])],
    selection: updates,
  };
  const updated = allocateSelection(n);
  await settle(writer.update(update, updated));
  // RETURNING is evaluated by the executor over `written` ∪ `updated` after `end()` succeeds.
  for (let k = 0; k < updated.count; k++) written.rows[written.count++] = updated.rows[k] ?? 0;
  return written;
}

/** A statement over the stand-in table that records the calls the example makes. */
function recordingStatement(existing: ReadonlyMap<number, number>): {
  statement: Statement;
  calls: string[];
} {
  const calls: string[] = [];
  const slots = (selection: Selection | null) =>
    selection === null ? [] : Array.from(selection.rows.subarray(0, selection.count));
  const record = (name: string, batch: Batch, written: Selection | null) => {
    calls.push(`${name} ${slots(batch.selection).join(",")}`);
    for (const row of slots(batch.selection)) if (written) written.rows[written.count++] = row;
    return batch.selection?.count ?? batch.length;
  };
  const writer: TableWriter = {
    tableId: TABLE,
    insert: (batch, written) => record("insert", batch, written),
    update: (batch, written) => record("update", batch, written),
    delete: (batch, written) => record("delete", batch, written),
    conflicts(batch, _arbiters, out) {
      const ids = batch.columns[0];
      const seen = new Map<number, number>();
      for (let row = 0; row < batch.length; row++) {
        const id =
          ids?.form === "flat" && ids.data.layout === "i32" ? (ids.data.values[row] ?? 0) : 0;
        const locator = existing.get(id);
        const first = seen.get(id);
        out.indexIds[row] = PRIMARY;
        if (locator !== undefined) {
          out.kinds[row] = ConflictKind.existing;
          out.locators[row] = locator;
        } else if (first !== undefined) {
          out.kinds[row] = ConflictKind.batch;
          out.earlier[row] = first;
        } else {
          out.kinds[row] = ConflictKind.none;
          seen.set(id, row);
        }
      }
    },
  };
  const statement: Statement = {
    ordinal: 1,
    view: readSnapshot(),
    catalog,
    table: () => writer,
    changeCatalog: () => ({ id: null }),
    end: async () => {
      calls.push("end");
    },
    rollback: () => {
      calls.push("rollback");
    },
  };
  return { statement, calls };
}

describe("executor writes through the access contracts", () => {
  const proposed = (ids: number[], customers: number[]): Batch => ({
    length: ids.length,
    columns: [ints(ids), ints(customers)],
    selection: null,
  });

  it("upsert: inserts new keys, updates existing ones by locator", async () => {
    const { statement, calls } = recordingStatement(new Map([[3, packLocator(0, 2)]]));
    const written = await upsert(statement, proposed([5, 3, 6], [1, 2, 3]));
    await statement.end();
    expect(calls).toEqual(["insert 0,2", "update 1", "end"]);
    expect(Array.from(written.rows.subarray(0, written.count))).toEqual([0, 2, 1]);
  });

  it("upsert: a key proposed twice in one statement is a cardinality violation", async () => {
    const { statement } = recordingStatement(new Map());
    await expect(upsert(statement, proposed([5, 5], [1, 2]))).rejects.toThrow(
      CardinalityViolationError,
    );
    statement.rollback();
  });
});

// ---------------------------------------------------------------------------------------------
// Owner: an autocommit INSERT, acknowledged per durability; a read that follows the delivery rule.

const noRules: TableRules = {
  check: () => null,
  generate: () => {
    // No generated columns.
  },
};

async function autocommitInsert(
  writePath: Pick<WritePath, "begin" | "durability">,
  batch: Batch,
  request: Omit<CommitRequest, "counts">,
): Promise<number> {
  const transaction = await writePath.begin({ label: "client#1/request#7" });
  try {
    const statement = transaction.statement({ rules: () => noRules });
    let inserted: number;
    try {
      inserted = await settle(statement.table(TABLE).insert(batch, null));
      await statement.end();
    } catch (error) {
      statement.rollback();
      throw error;
    }
    const receipt = await transaction.commit({ ...request, counts: { inserted } });
    // `receipt.wrote` false: nothing committed; the owner settles the request (ledger.settle).
    if (writePath.durability === "strict") await receipt.durable; // relaxed: acknowledge now
    return inserted;
  } finally {
    transaction.rollback(); // no-op after commit; frees the writer slot on any failure
  }
}

async function readQuery<T>(
  writePath: Pick<WritePath, "pin" | "durable">,
  run: (view: Snapshot) => Promise<T>,
): Promise<T> {
  const view = writePath.pin({ label: "client#1/query#8" });
  try {
    const result = await run(view);
    const wait = writePath.durable(view.observedLsn); // null in the common case: no await
    if (wait !== null) await wait;
    return result;
  } finally {
    view.release();
  }
}

// Executor and owner: `INSERT … RETURNING id` with id from nextval inside an explicit
// transaction. The block's frame is standalone, so its durability never waits for COMMIT.

function drawIds(sequences: SequenceAccess, count: number): { ids: number[]; waitLsn: number } {
  const out = { repr: "safe" as const, values: new Float64Array(count) };
  const waitLsn = sequences.next(42, count, out, 0);
  return { ids: Array.from(out.values), waitLsn };
}

describe("owner composition", () => {
  it("autocommit: begin, statement, end, commit, then wait for durability under strict", async () => {
    const { statement, calls } = recordingStatement(new Map());
    let durable = false;
    const receipt: CommitReceipt = {
      wrote: true,
      lsn: 6,
      durable: Promise.resolve().then(() => {
        durable = true;
      }),
    };
    const transaction: Pick<WriteTransaction, "statement" | "commit" | "rollback"> = {
      statement: () => statement,
      commit: (request) => {
        calls.push(`commit ${JSON.stringify(request?.counts)}`);
        return Promise.resolve(receipt);
      },
      rollback: () => {
        // The example commits; rollback after commit is a no-op.
      },
    };
    const writePath = {
      durability: "strict" as const,
      begin: () => Promise.resolve(transaction as WriteTransaction),
    };
    const inserted = await autocommitInsert(
      writePath,
      { length: 2, columns: [ints([8, 9]), ints([1, 1])], selection: null },
      {
        clientId: new Uint8Array(16),
        sequence: 7,
        sentAtMs: 0,
      },
    );
    expect(inserted).toBe(2);
    expect(durable).toBe(true);
    expect(calls).toEqual(["insert ", "end", 'commit {"inserted":2}']);
  });

  it("sequences: deliver after the block frame's flush, not after COMMIT", async () => {
    let next = 1;
    const sequences: SequenceAccess = {
      next: (_id, count, out) => {
        if (out.repr === "safe") for (let k = 0; k < count; k++) out.values[k] = next++;
        return 9; // the standalone block frame
      },
      set: () => 0,
    };
    const flushed: number[] = [];
    const durable = (lsn: number) => {
      flushed.push(lsn);
      return Promise.resolve();
    };
    const { ids, waitLsn } = drawIds(sequences, 3);
    await durable(Math.max(5, waitLsn)); // view.observedLsn 5; no COMMIT involved
    expect(ids).toEqual([1, 2, 3]);
    expect(flushed).toEqual([9]);
  });

  it("read: pin, run, deliver after durability, release", async () => {
    const view = readSnapshot();
    let released = false;
    const writePath = {
      pin: () => ({ ...view, release: () => (released = true) }),
      durable: () => null,
    };
    expect(await readQuery(writePath, (pinned) => Promise.resolve(pinned.observedLsn))).toBe(5);
    expect(released).toBe(true);
  });
});
