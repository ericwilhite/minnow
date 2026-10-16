/**
 * Interaction-plan simulator: a seeded, replayable workload over the whole SQL surface an
 * application uses, checked against a shadow model and a set of properties.
 *
 * `simulator.ts` is the storage-completion simulator: one table, keyed upserts and deletes, a
 * seeded completion order for every block-store call. This one sits above it, in the spirit of
 * Turso's simulator. A plan is a sequence of *interactions* -- DDL, inserts, predicate updates
 * and deletes, selects, indexes, SQL transactions, concurrent rounds across several connections,
 * faults, reopens, maintenance -- generated from a seed and serialized as JSON, so a failing CI
 * seed replays exactly and can be checked in. Every interaction is executed through the real
 * SQL entry points and judged against an in-memory shadow model that applies three-valued
 * predicate logic, plus the properties a database must honour whatever its internals do:
 *
 *   - insert-select: a row just inserted comes back exactly, and a rejected insert leaves none
 *     of its rows behind (statement atomicity)
 *   - update-count and delete-count: the reported row count is the model's matched count
 *   - delete-select: nothing matching a delete's predicate survives it
 *   - drop-select and double-create-failure: dropped tables are gone, duplicates are refused
 *   - select-limit: LIMIT returns exactly min(limit, matching) rows in ORDER BY order
 *   - where-true-false-null: COUNT(p) + COUNT(NOT p) + COUNT(p IS NULL) = COUNT(*)
 *   - union-all-cardinality: a UNION ALL has the sum of its members' cardinalities
 *   - transaction-isolation: uncommitted rows are visible on their own connection and invisible
 *     to another; ROLLBACK discards them and COMMIT keeps them
 *   - concurrent-explicability: a read taken while a round of commuting writes is in flight
 *     shows every untouched key exactly and every touched key at either its old or new value
 *   - writer-turns: a keyed write issued on another connection while a transaction is open
 *     waits for it rather than landing under it or losing a commit race, then lands exactly
 *     once with its own outcome; concurrent rounds among cooperating connections lose no race
 *   - fault-atomicity: a mutation interrupted by a storage fault or a crash lands entirely or
 *     not at all, and a reported success is durable
 *   - agreement: at every checkpoint every connection reads the same database as the model
 *   - rekey-forward (plans with `rekeying`): a rekey onto a live, repeated, or retired key is
 *     refused whole; references follow the moved rows; a retired key reads, writes, and compares
 *     as its row's current key on every connection, and stays reserved against inserts
 *   - constraints (plans with `constraints`): each statement a UNIQUE column or index, or an
 *     enforced foreign key, refuses fails with that constraint's error and changes nothing;
 *     deletes cascade, set NULL, or are restricted down the tree of children
 *   - upsert-arbiter: a proposal colliding on its conflict target updates the row holding the
 *     value under that row's own key, and any other collision is refused
 *   - unique-race: concurrent writes contending for one unique value land as some serial order
 *   - index-lookup: selects shaped for the secondary-index path return the model's rows
 *   - schema-change (plans with `schemaChanges`): an accepted ALTER TABLE lands for every
 *     connection, and one the engine must refuse changes nothing
 *   - full-text (plans with `fullText`): MATCH returns exactly the rows holding every term
 *   - fold-progress (plans with `burstCommits`): a long run of single-row commits keeps the
 *     table's visible segments bounded, so automatic folds keep up
 *
 * The runner is driver-agnostic. `SimulationDriver` opens connections over one shared database;
 * the Node driver wraps `MinnowDatabase` over any block store, and the Playwright driver wraps
 * one real browser tab per connection over IndexedDB or OPFS, so the same plan runs in both.
 */

import { MinnowDatabase, type MinnowDatabaseOptions } from "../engine/database.js";
import type { BlockStore } from "../storage/index.js";
import { FaultInjectingBlockStore, type FaultPoint } from "./index.js";

// --- Plan ----------------------------------------------------------------------------------------

export type ColumnType = "integer" | "real" | "text" | "boolean";

export interface PlanColumn {
  readonly name: string;
  readonly type: ColumnType;
  readonly nullable: boolean;
  /**
   * `"self"` declares the column `REFERENCES <its table> (id) NOT ENFORCED`: an integer column
   * that a rekey of the row it names moves, and whose written values follow retired keys. A
   * foreign key declares an enforced `REFERENCES <table> (id) ON DELETE <action>` column, which
   * every write checks against the parent and which follows the parent's rekeys too.
   */
  readonly references?: "self" | PlanForeignKey;
  /** Declared `UNIQUE` inline: no two rows hold one non-null value. */
  readonly unique?: boolean;
  /**
   * Generated values come from `0 .. domain - 1` (as numbers, or `k<n>` as text) instead of the
   * type's small default range, so a column meant to be unique collides now and then rather
   * than on almost every write, and a foreign key names a parent key most of the time.
   */
  readonly domain?: number;
  /** A constant `DEFAULT`; only columns added by `ALTER TABLE ... ADD COLUMN` carry one. */
  readonly default?: PlanValue;
}

export interface PlanForeignKey {
  readonly table: string;
  readonly onDelete: "cascade" | "set null" | "restrict";
}

export interface PlanTable {
  readonly name: string;
  /** Every table has `id INTEGER PRIMARY KEY`; these are the other columns. */
  readonly columns: readonly PlanColumn[];
  /**
   * A keyless table (`id INTEGER` with no key): rows are only ever appended, which is what lets
   * a full-text index cover it. Plans give every appended row a distinct `id`.
   */
  readonly appendOnly?: boolean;
}

export type PlanValue = number | string | boolean | null;

export type PlanRow = Readonly<Record<string, PlanValue>>;

export type CompareOperator = "=" | "<>" | "<" | "<=" | ">" | ">=";

export type Predicate =
  | {
      readonly kind: "compare";
      readonly column: string;
      readonly op: CompareOperator;
      readonly value: PlanValue;
    }
  | { readonly kind: "isNull"; readonly column: string; readonly negated: boolean }
  /** `column IN (values)`; the values are never NULL. */
  | { readonly kind: "in"; readonly column: string; readonly values: readonly PlanValue[] }
  | { readonly kind: "and"; readonly left: Predicate; readonly right: Predicate }
  | { readonly kind: "or"; readonly left: Predicate; readonly right: Predicate }
  | { readonly kind: "not"; readonly inner: Predicate }
  | { readonly kind: "literal"; readonly value: boolean };

export type Assignment =
  | { readonly kind: "set"; readonly column: string; readonly value: PlanValue }
  | { readonly kind: "increment"; readonly column: string; readonly by: number };

/**
 * The new primary key a rekey gives each row it matches: a constant, the old key shifted, or
 * the old key reflected about a point (`around - id`), which swaps two keys that sum to it.
 */
export type KeyAssignment =
  | { readonly kind: "to"; readonly value: number }
  | { readonly kind: "shift"; readonly by: number }
  | { readonly kind: "reflect"; readonly around: number };

export type KeyedMutation =
  | { readonly kind: "insert"; readonly row: PlanRow }
  | { readonly kind: "upsert"; readonly row: PlanRow }
  | { readonly kind: "updateKey"; readonly id: number; readonly assignments: readonly Assignment[] }
  | { readonly kind: "deleteKey"; readonly id: number };

export type TransactionStatement =
  | { readonly kind: "insert"; readonly rows: readonly PlanRow[] }
  | {
      readonly kind: "update";
      readonly predicate: Predicate;
      readonly assignments: readonly Assignment[];
    }
  | { readonly kind: "delete"; readonly predicate: Predicate }
  | {
      readonly kind: "rekey";
      readonly predicate: Predicate;
      readonly key: KeyAssignment;
      readonly assignments: readonly Assignment[];
    };

export type Interaction =
  | {
      readonly kind: "createTable";
      readonly connection: number;
      readonly table: PlanTable;
      readonly expectExisting: boolean;
    }
  | {
      readonly kind: "insert";
      readonly connection: number;
      readonly table: string;
      readonly rows: readonly PlanRow[];
      readonly viaParameters: boolean;
    }
  | {
      readonly kind: "update";
      readonly connection: number;
      readonly table: string;
      readonly predicate: Predicate;
      readonly assignments: readonly Assignment[];
    }
  | {
      readonly kind: "delete";
      readonly connection: number;
      readonly table: string;
      readonly predicate: Predicate;
    }
  | {
      /**
       * `UPDATE t SET id = <key>[, assignments] WHERE predicate`: every matched row moves to its
       * new key, references to it follow, and each key it gave up forwards to the new one.
       */
      readonly kind: "rekey";
      readonly connection: number;
      readonly table: string;
      readonly predicate: Predicate;
      readonly key: KeyAssignment;
      readonly assignments: readonly Assignment[];
    }
  | {
      readonly kind: "select";
      readonly connection: number;
      readonly table: string;
      readonly predicate: Predicate;
      readonly limit: number | null;
      readonly descending: boolean;
    }
  | {
      readonly kind: "partition";
      readonly connection: number;
      readonly table: string;
      readonly predicate: Predicate;
    }
  | {
      readonly kind: "unionAll";
      readonly connection: number;
      readonly table: string;
      readonly left: Predicate;
      readonly right: Predicate;
    }
  | {
      readonly kind: "createIndex";
      readonly connection: number;
      readonly table: string;
      readonly name: string;
      readonly columns: readonly string[];
      /**
       * `CREATE UNIQUE INDEX`: refused when the rows already hold a duplicate, enforced on every
       * write once built. Absent on an ordinary index.
       */
      readonly unique?: boolean;
    }
  | {
      readonly kind: "dropIndex";
      readonly connection: number;
      readonly table: string;
      readonly name: string;
      /** `DROP INDEX IF EXISTS`, for an index whose build the generator cannot know succeeded. */
      readonly ifExists: boolean;
    }
  | {
      readonly kind: "dropTable";
      readonly connection: number;
      readonly table: string;
      readonly expectMissing: boolean;
      /** Another table's enforced foreign key references this one, so the drop is refused. */
      readonly expectRefused?: boolean;
    }
  | {
      /** `ALTER TABLE`: a schema change interleaved with the writes around it. */
      readonly kind: "alterTable";
      readonly connection: number;
      readonly table: string;
      readonly change: SchemaChange;
      /** The engine must refuse the change (an index or foreign key still uses the column). */
      readonly expectRefused: boolean;
    }
  | {
      /**
       * `INSERT ... ON CONFLICT (<unique columns>) DO UPDATE | DO NOTHING`, or a targetless
       * `ON CONFLICT DO NOTHING` when `target` is null. A collision on the target updates the
       * row that holds the value, under its own key; a collision on any other constraint is an
       * error.
       */
      readonly kind: "upsertUnique";
      readonly connection: number;
      readonly table: string;
      readonly row: PlanRow;
      readonly target: readonly string[] | null;
      /** Null for `DO NOTHING`; otherwise how each assigned column takes its new value. */
      readonly assignments: readonly UpsertAssignment[] | null;
    }
  | {
      /**
       * A select shaped for an index: equality, `IN`, or a range on an indexed column, ordered by
       * that column, so the answer comes through the secondary-index path as well as a scan.
       */
      readonly kind: "lookup";
      readonly connection: number;
      readonly table: string;
      readonly predicate: Predicate;
      /** Ordered by this column, then `id`; null orders by `id` alone. */
      readonly orderBy: string | null;
      readonly descending: boolean;
      readonly limit: number | null;
    }
  | {
      /** `SELECT * ... WHERE MATCH(columns) AGAINST 'query' [AND predicate] ORDER BY id`. */
      readonly kind: "match";
      readonly connection: number;
      readonly table: string;
      readonly columns: readonly string[];
      readonly query: string;
      readonly and: Predicate | null;
    }
  | {
      /**
       * A long run of single-row commits to one table from random connections: enough of them
       * that automatic folds run underneath, so the rows and the table's segment count are
       * judged across them. The commits are drawn from `seed` and the table as it stands.
       */
      readonly kind: "burst";
      readonly table: string;
      readonly seed: number;
      readonly commits: number;
      readonly keySpace: number;
    }
  | {
      readonly kind: "transaction";
      readonly connection: number;
      readonly observer: number;
      readonly table: string;
      readonly statements: readonly TransactionStatement[];
      readonly outcome: "commit" | "rollback";
    }
  | {
      readonly kind: "concurrent";
      readonly table: string;
      readonly operations: ReadonlyArray<{
        readonly connection: number;
        readonly mutation: KeyedMutation;
      }>;
      readonly readers: readonly number[];
    }
  | {
      readonly kind: "fault";
      readonly connection: number;
      readonly table: string;
      readonly mutation: KeyedMutation;
      readonly point: FaultPointName;
    }
  | {
      /**
       * A transaction on one connection holds the database's writer turn while another
       * connection issues a keyed write: that write must wait, not conflict, and land right after
       * the transaction ends, whichever way it ends.
       */
      readonly kind: "queuedWrite";
      readonly connection: number;
      readonly waiter: number;
      readonly table: string;
      readonly rows: readonly PlanRow[];
      readonly mutation: KeyedMutation;
      readonly outcome: "commit" | "rollback";
    }
  | { readonly kind: "reopen"; readonly connection: number }
  | { readonly kind: "maintenance"; readonly connection: number; readonly table: string }
  | { readonly kind: "checkpoint" };

export type SchemaChange =
  | { readonly kind: "addColumn"; readonly column: PlanColumn }
  | { readonly kind: "dropColumn"; readonly column: string }
  | { readonly kind: "renameColumn"; readonly column: string; readonly to: string };

/** `column = EXCLUDED.column`, or `column = <alias>.column + 1` from the stored row. */
export interface UpsertAssignment {
  readonly column: string;
  readonly from: "excluded" | "increment";
}

export type FaultPointName =
  | "beforeBlockWrite"
  | "afterBlockWrite"
  | "beforeTransactionCommit"
  | "afterTransactionCommit"
  | "crash";

export interface InteractionPlan {
  readonly version: 1;
  readonly seed: number;
  readonly connections: number;
  readonly interactions: readonly Interaction[];
}

export interface InteractionPlanOptions {
  /** Interactions to generate, checkpoints and DDL included. */
  readonly length?: number;
  readonly connections?: number;
  /** Tables the plan may create at once. */
  readonly tables?: number;
  /** Primary-key space per table; small enough that inserts collide and updates hit. */
  readonly keySpace?: number;
  /**
   * Fault points the plan may draw from. A driver without storage fault injection (a real
   * browser) generates crash faults only, so every fault step is one it can exercise.
   */
  readonly faultPoints?: readonly FaultPointName[];
  /**
   * Adds primary-key changes: rekey statements and transactions that rekey, and tables with a
   * self-referencing column. They are drawn from a second random stream, and only when this is
   * set, so every plan generated without it is unchanged and every recorded seed replays.
   */
  readonly rekeying?: boolean;
  /**
   * Adds constraints: tables with an inline `UNIQUE` column and a wide-domain column that may
   * get a `UNIQUE` index, composite unique indexes, upserts that target a unique column, child
   * tables with an enforced foreign key (`ON DELETE CASCADE`, `SET NULL`, or `RESTRICT`), and
   * selects shaped for the secondary-index path. Like every option below, it draws from its own
   * random stream and leaves plans generated without it unchanged.
   */
  readonly constraints?: boolean;
  /**
   * Adds `ALTER TABLE` interleaved with the writes: `ADD COLUMN` with a constant `DEFAULT`
   * (sometimes reusing a dropped column's name), `DROP COLUMN`, `RENAME COLUMN`, and
   * `CREATE` / `DROP INDEX`, including changes the engine must refuse. Columns are added and
   * dropped only on tables nothing has written yet; `"populated"` lifts that, which currently
   * reproduces tables that can no longer fold (see the generator's `addColumn`).
   */
  readonly schemaChanges?: boolean | "populated";
  /** Adds full-text `MATCH` queries, over keyed tables and over append-only tables it indexes. */
  readonly fullText?: boolean;
  /**
   * Adds write bursts of up to this many single-row commits to one table, enough to drive it
   * through automatic folds; every plan with this set holds at least one burst.
   */
  readonly burstCommits?: number;
}

// --- Driver ----------------------------------------------------------------------------------------

export interface SimulatedExecuteResult {
  readonly kind: string;
  readonly rowCount?: number;
}

export interface SimulatedQueryResult {
  readonly columns: readonly string[];
  readonly rows: ReadonlyArray<Readonly<Record<string, unknown>>>;
}

export interface SimulatedConnection {
  execute(sql: string, params?: readonly PlanValue[]): Promise<SimulatedExecuteResult>;
  query(sql: string, params?: readonly PlanValue[]): Promise<SimulatedQueryResult>;
  /** Closes and reopens this connection over the same database. */
  reopen(): Promise<void>;
  /** Runs the store's maintenance (compaction and collection) for one table. */
  maintain?(table: string): Promise<void>;
  /** The engine's plan text for a SELECT; used to count the reads a secondary index served. */
  explain?(sql: string): Promise<string>;
  /** Visible segments of one table now: what automatic folding keeps bounded. */
  visibleSegments?(table: string): Promise<number>;
  /**
   * Kills the connection's process while a call may be in flight; the pending call must fail
   * with an unknown-outcome or lost-connection error, and `reopen` must bring the connection
   * back. A driver without a process boundary omits it and the plan's crash faults are skipped.
   */
  crash?(): Promise<void>;
}

export interface SimulatedFaults {
  /** Fails the next `occurrence`-th storage call at `point` on any connection. */
  arm(point: Exclude<FaultPointName, "crash">, occurrence: number): void;
  disarm(): void;
  fired(): boolean;
}

export interface SimulationDriver {
  open(connection: number): Promise<SimulatedConnection>;
  readonly faults?: SimulatedFaults;
  /** Whether errors the driver reports carry `name`; a driver over a text channel may not. */
  close?(): Promise<void>;
}

export interface InteractionRunOptions {
  /** Bounded trace of executed SQL kept for the failure report. */
  readonly traceLength?: number;
}

export interface InteractionRunResult {
  readonly seed: number;
  /** Interactions judged. Short of the plan's length only when `transientsAccepted` is not zero. */
  readonly interactions: number;
  readonly statements: number;
  readonly queries: number;
  readonly acceptedWrites: number;
  /**
   * Refusals the engine documents and the runner models rather than reports: a write that lost a
   * conflict, and a transaction whose COMMIT lost the race to another connection's data commit.
   * The run continues from the state they leave behind.
   */
  readonly rejectedConflicts: number;
  /**
   * Refusals the plan expected: a duplicate insert, a double create, and a transaction the engine
   * rolled back on its own idle deadline, which the runner acknowledges with a `ROLLBACK`. These
   * do not stop the run either.
   */
  readonly expectedFailures: number;
  readonly faultsInjected: number;
  readonly faultsSkipped: number;
  readonly reopens: number;
  readonly checkpoints: number;
  readonly tablesAtEnd: number;
  readonly rowsAtEnd: number;
  /** Index-shaped lookups whose plan, where the driver can explain one, used a secondary index. */
  readonly indexPathQueries: number;
  /** Single-row commits that write bursts attempted. */
  readonly burstCommits: number;
  /** The most visible segments any table held at the end of a burst. */
  readonly peakBurstSegments: number;
  /**
   * The one transient that ends a run instead of being modelled: a store that stopped answering.
   * A browser can wedge a whole database beyond any engine's reach (a worker terminated with a
   * write in flight leaves WebKit's IndexedDB connection registered until its document goes away),
   * and the engine then bounds the wait and reports `StorageUnresponsiveError` rather than hanging.
   * Nothing further can be driven through that store, so the run stops at that interaction and
   * `interactions` counts only the steps it judged.
   */
  readonly transientsAccepted: number;
  /** What ended the run early, when `transientsAccepted` is not zero. */
  readonly stoppedBy: string | undefined;
}

export class InteractionFailure extends Error {
  constructor(
    message: string,
    readonly index: number,
    readonly interaction: Interaction,
    readonly trace: readonly string[],
    options?: ErrorOptions,
  ) {
    super(
      `${message}\n  at interaction ${String(index)} (${interaction.kind})\n  recent SQL:\n    ${trace.join("\n    ")}`,
      options,
    );
    this.name = "InteractionFailure";
  }
}

// --- Generation -------------------------------------------------------------------------------------

const TEXT_POOL = [
  "alpha",
  "bravo",
  "charlie",
  "delta",
  "echo",
  "fox trot",
  "Golf",
  "hotel",
  "o'clock",
  "zulu",
  "",
  "42",
] as const;

const FAULT_POINTS: readonly FaultPointName[] = [
  "beforeBlockWrite",
  "afterBlockWrite",
  "beforeTransactionCommit",
  "afterTransactionCommit",
  "crash",
];

interface Random {
  next(): number;
  int(maximum: number): number;
  pick<T>(values: readonly T[]): T;
  chance(probability: number): boolean;
}

function createRandom(seed: number): Random {
  const next = mulberry32(seed);
  return {
    next,
    int: (maximum) => Math.floor(next() * maximum),
    pick: (values) => {
      const value = values[Math.floor(next() * values.length)];
      if (value === undefined) throw new Error("pick from an empty list");
      return value;
    },
    chance: (probability) => next() < probability,
  };
}

/** Generates a bounded, replayable plan; the same seed and options always produce the same plan. */
export function generateInteractionPlan(
  seed: number,
  options: InteractionPlanOptions = {},
): InteractionPlan {
  if (!Number.isSafeInteger(seed)) throw new RangeError("Plan seed must be a whole number");
  const length = options.length ?? 120;
  const connections = options.connections ?? 3;
  const tableLimit = options.tables ?? 2;
  const keySpace = options.keySpace ?? 24;
  const faultPoints = options.faultPoints ?? FAULT_POINTS;
  if (faultPoints.length === 0 || faultPoints.some((point) => !FAULT_POINTS.includes(point))) {
    throw new RangeError("Plan faultPoints must name at least one known fault point");
  }
  checkRange("length", length, 1, 100_000);
  checkRange("connections", connections, 1, 16);
  checkRange("tables", tableLimit, 1, 8);
  checkRange("keySpace", keySpace, 2, 10_000);

  const random = createRandom(seed);
  // Rekeying draws from its own stream, so the primary stream -- and every plan generated
  // without rekeying -- stays exactly what it was.
  const rekeyRandom =
    options.rekeying === true ? createRandom((seed ^ 0x4e4b4559) >>> 0) : undefined;
  // Each later option draws from a stream of its own in the same way.
  const constraintRandom =
    options.constraints === true ? createRandom((seed ^ 0x434f4e53) >>> 0) : undefined;
  const schemaRandom =
    options.schemaChanges === true || options.schemaChanges === "populated"
      ? createRandom((seed ^ 0x414c5452) >>> 0)
      : undefined;
  const populatedSchemaChanges = options.schemaChanges === "populated";
  const textRandom =
    options.fullText === true ? createRandom((seed ^ 0x46545331) >>> 0) : undefined;
  const burstCommits = options.burstCommits;
  if (burstCommits !== undefined) checkRange("burstCommits", burstCommits, 1, 100_000);
  const burstRandom =
    burstCommits === undefined ? undefined : createRandom((seed ^ 0x42525354) >>> 0);
  const interactions: Interaction[] = [];
  // The generator keeps its own shadow of the schema so predicates name real columns and keyed
  // rounds target keys that can commute. Values are not tracked: the runner's model does that.
  const live = new Map<string, PlanTable>();
  const indexCount = new Map<string, number>();
  // Schema the options add, tracked without drawing from any stream: the indexes on each table
  // (an index whose build may meet a duplicate is not `definite`), which tables reference each
  // table, the names a table's dropped columns had, and the append-only tables `MATCH` reads.
  const indexes = new Map<string, GeneratedIndex[]>();
  const children = new Map<string, Set<string>>();
  const droppedColumns = new Map<string, string[]>();
  const logs = new Map<string, PlanTable>();
  // Tables some generated step has written to, brought up to date from `scanned` on demand.
  const written = new Set<string>();
  let scanned = 0;
  let columnSerial = 0;
  let indexSerial = 0;
  let logSerial = 0;
  let logRowSerial = 0;
  let bursts = 0;
  let tableSerial = 0;
  const connection = (): number => random.int(connections);
  const anyTable = (): PlanTable | undefined => {
    const tables = [...live.values()];
    return tables.length === 0 ? undefined : random.pick(tables);
  };
  const trackIndex = (table: string, index: GeneratedIndex): void => {
    indexes.set(table, [...(indexes.get(table) ?? []), index]);
  };
  const forgetTable = (name: string): void => {
    indexes.delete(name);
    children.delete(name);
    droppedColumns.delete(name);
    for (const set of children.values()) set.delete(name);
  };

  const createTable = (): void => {
    const columns: PlanColumn[] = [];
    const count = 2 + random.int(3);
    for (let index = 0; index < count; index++) {
      columns.push({
        name: `c${String(index)}`,
        type: random.pick<ColumnType>(["integer", "real", "text", "boolean"]),
        nullable: random.chance(0.5),
      });
    }
    if (rekeyRandom?.chance(0.6) === true) {
      columns.push({ name: "p", type: "integer", nullable: true, references: "self" });
    }
    const name = `t${String(tableSerial++)}`;
    if (constraintRandom !== undefined) {
      if (constraintRandom.chance(0.6)) {
        columns.push({
          name: "u",
          type: constraintRandom.pick<ColumnType>(["integer", "text"]),
          nullable: constraintRandom.chance(0.5),
          unique: true,
          domain: keySpace * 3,
        });
      }
      if (constraintRandom.chance(0.4)) {
        columns.push({
          name: "v",
          type: constraintRandom.pick<ColumnType>(["integer", "text"]),
          nullable: true,
          domain: keySpace * 4,
        });
      }
      const parents = [...live.values()];
      if (parents.length > 0 && constraintRandom.chance(0.6)) {
        const parent = constraintRandom.pick(parents);
        columns.push({
          name: "f",
          type: "integer",
          nullable: true,
          references: {
            table: parent.name,
            onDelete: constraintRandom.pick(["cascade", "set null", "restrict"] as const),
          },
          domain: keySpace + 2,
        });
        children.set(parent.name, (children.get(parent.name) ?? new Set()).add(name));
      }
    }
    const table: PlanTable = { name, columns };
    live.set(table.name, table);
    interactions.push({
      kind: "createTable",
      connection: connection(),
      table,
      expectExisting: false,
    });
    if (random.chance(0.3)) {
      interactions.push({
        kind: "createTable",
        connection: connection(),
        table,
        expectExisting: true,
      });
    }
    // A fresh table is where ADD COLUMN may go (see addColumn), so it often gets one at once.
    if (schemaRandom?.chance(0.5) === true) addColumn(schemaRandom, table.name);
  };

  /**
   * `ALTER TABLE ... ADD COLUMN`, sometimes reusing a dropped column's name, `NOT NULL` with a
   * constant default, nullable with one, or nullable without.
   *
   * Unless `schemaChanges` is `"populated"`, a column is added -- or dropped -- only on a table
   * nothing has written yet. On a written table both currently leave it unable to fold: a fold
   * refuses a full-row source segment that predates an added column, and an update segment
   * whose only changed column was dropped, so its level-zero segments grow until writes are
   * refused. Both fixes change what a stored compaction plan may hold or how a column drop
   * retires segments; `"populated"` reproduces them until they land.
   */
  const refreshWritten = (): void => {
    for (; scanned < interactions.length; scanned++) {
      const interaction = interactions[scanned];
      if (
        interaction !== undefined &&
        "table" in interaction &&
        typeof interaction.table === "string" &&
        WRITING_KINDS.has(interaction.kind)
      ) {
        written.add(interaction.table);
      }
    }
  };
  const addColumn = (stream: Random, name: string): void => {
    refreshWritten();
    const table = live.get(name);
    if (table === undefined || (written.has(name) && !populatedSchemaChanges)) return;
    const dropped = (droppedColumns.get(name) ?? []).filter(
      (column) => !table.columns.some((candidate) => candidate.name === column),
    );
    const column = dropped.length > 0 && stream.chance(0.4) ? stream.pick(dropped) : undefined;
    const added = column ?? `a${String(columnSerial++)}`;
    const type = stream.pick<ColumnType>(["integer", "real", "text", "boolean"]);
    const mode = stream.next();
    const value = generateValue(stream, { name: added, type, nullable: false });
    const definition: PlanColumn =
      mode < 0.4
        ? { name: added, type, nullable: false, default: value }
        : mode < 0.7
          ? { name: added, type, nullable: true, default: value }
          : { name: added, type, nullable: true };
    interactions.push({
      kind: "alterTable",
      connection: stream.int(connections),
      table: name,
      change: { kind: "addColumn", column: definition },
      expectRefused: false,
    });
    live.set(name, { ...table, columns: [...table.columns, definition] });
  };

  /** Column sets an index serves on a table: its tracked indexes and its inline unique columns. */
  const indexedColumnSets = (table: PlanTable): Array<readonly string[]> => [
    ...(indexes.get(table.name) ?? []).map((index) => index.columns),
    ...table.columns.filter((column) => column.unique === true).map((column) => [column.name]),
  ];

  const constraintStep = (stream: Random, table: PlanTable): void => {
    const owner = stream.int(connections);
    const roll = stream.next();
    const uniqueColumns = table.columns.filter((column) => column.unique === true);
    if (roll < 0.35 && uniqueColumns.length > 0) {
      const target = stream.chance(0.15) ? null : [stream.pick(uniqueColumns).name];
      const assignments: UpsertAssignment[] | null =
        target === null || stream.chance(0.25)
          ? null
          : table.columns
              .filter((column) => !target.includes(column.name))
              .map((column) => ({
                column: column.name,
                from:
                  column.type === "integer" && column.references === undefined && stream.chance(0.3)
                    ? "increment"
                    : "excluded",
              }));
      interactions.push({
        kind: "upsertUnique",
        connection: owner,
        table: table.name,
        row: generateRow(stream, table, keySpace),
        target,
        assignments,
      });
      return;
    }
    const sets = indexedColumnSets(table);
    const columns = (): readonly string[] =>
      sets.length > 0 ? stream.pick(sets) : [stream.pick(table.columns).name];
    if (roll < 0.65) {
      interactions.push({
        kind: "lookup",
        connection: owner,
        table: table.name,
        ...generateLookup(stream, table, columns(), keySpace),
      });
      return;
    }
    if (roll < 0.75) {
      // A UNIQUE index on a wide column may meet a duplicate the rows already hold, so whether
      // it exists is the runner's to know; one that includes the key always builds.
      const name = `${table.name}_ux${String(indexSerial++)}`;
      const wide = table.columns.filter(
        (column) =>
          column.domain !== undefined && column.unique !== true && column.references === undefined,
      );
      const indexed =
        wide.length > 0 && stream.chance(0.5)
          ? { columns: [stream.pick(wide).name], definite: false }
          : { columns: [stream.pick(table.columns).name, "id"], definite: true };
      interactions.push({
        kind: "createIndex",
        connection: owner,
        table: table.name,
        name,
        columns: indexed.columns,
        unique: true,
      });
      trackIndex(table.name, { name, unique: true, ...indexed });
      return;
    }
    if (roll < 0.9) {
      const predicate = indexPredicate(stream, table, columns(), keySpace);
      if (stream.chance(0.6)) {
        interactions.push({
          kind: "update",
          connection: owner,
          table: table.name,
          predicate,
          assignments: generateAssignments(stream, table),
        });
      } else {
        interactions.push({ kind: "delete", connection: owner, table: table.name, predicate });
      }
      return;
    }
    interactions.push({ kind: "maintenance", connection: owner, table: table.name });
  };

  /** Some inserts this step made omit the columns that have a default, which then fills them. */
  const omitDefaults = (stream: Random, stepStart: number): void => {
    for (let index = stepStart; index < interactions.length; index++) {
      const interaction = interactions[index];
      if (interaction?.kind !== "insert") continue;
      const table = live.get(interaction.table);
      const defaulted = (table?.columns ?? []).filter((column) => column.default !== undefined);
      if (defaulted.length === 0 || !stream.chance(0.3)) continue;
      interactions[index] = {
        ...interaction,
        rows: interaction.rows.map((row) =>
          Object.fromEntries(
            Object.entries(row).filter(
              ([name]) => !defaulted.some((column) => column.name === name),
            ),
          ),
        ),
      };
    }
  };

  const schemaStep = (stream: Random, table: PlanTable): void => {
    const owner = stream.int(connections);
    const roll = stream.next();
    const alter = (change: SchemaChange, expectRefused: boolean): void => {
      interactions.push({
        kind: "alterTable",
        connection: owner,
        table: table.name,
        change,
        expectRefused,
      });
    };
    const tableIndexes = indexes.get(table.name) ?? [];
    if (roll < 0.25) {
      addColumn(stream, table.name);
      return;
    }
    if (roll < 0.55) {
      if (table.columns.length <= 1) return;
      // An index whose build may have failed leaves the drop's outcome unknown here; skip it.
      const uncertain = new Set(
        tableIndexes.filter((index) => !index.definite).flatMap((index) => index.columns),
      );
      const indexed = new Set(
        tableIndexes.filter((index) => index.definite).flatMap((index) => index.columns),
      );
      const candidates = table.columns.filter((column) => !uncertain.has(column.name));
      if (candidates.length === 0) return;
      const column = stream.pick(candidates);
      const refused =
        column.unique === true || column.references !== undefined || indexed.has(column.name);
      refreshWritten();
      if (!refused && written.has(table.name) && !populatedSchemaChanges) return;
      alter({ kind: "dropColumn", column: column.name }, refused);
      if (!refused) {
        live.set(table.name, {
          ...table,
          columns: table.columns.filter((candidate) => candidate !== column),
        });
        droppedColumns.set(table.name, [...(droppedColumns.get(table.name) ?? []), column.name]);
      }
      return;
    }
    if (roll < 0.75) {
      const column = stream.pick(table.columns);
      const refused = column.references !== undefined;
      const dropped = (droppedColumns.get(table.name) ?? []).filter(
        (name) => !table.columns.some((candidate) => candidate.name === name),
      );
      const to =
        dropped.length > 0 && stream.chance(0.3)
          ? stream.pick(dropped)
          : `r${String(columnSerial++)}`;
      alter({ kind: "renameColumn", column: column.name, to }, refused);
      if (!refused) {
        live.set(table.name, {
          ...table,
          columns: table.columns.map((candidate) =>
            candidate === column ? { ...candidate, name: to } : candidate,
          ),
        });
        indexes.set(
          table.name,
          tableIndexes.map((index) => ({
            ...index,
            columns: index.columns.map((name) => (name === column.name ? to : name)),
          })),
        );
      }
      return;
    }
    if (roll < 0.9) {
      const first = stream.pick(table.columns).name;
      const others = table.columns.filter((column) => column.name !== first);
      const columns =
        others.length > 0 && stream.chance(0.4) ? [first, stream.pick(others).name] : [first];
      const name = `${table.name}_ax${String(indexSerial++)}`;
      interactions.push({
        kind: "createIndex",
        connection: owner,
        table: table.name,
        name,
        columns,
      });
      trackIndex(table.name, { name, columns, unique: false, definite: true });
      return;
    }
    if (tableIndexes.length === 0) return;
    const index = stream.pick(tableIndexes);
    interactions.push({
      kind: "dropIndex",
      connection: owner,
      table: table.name,
      name: index.name,
      ifExists: !index.definite,
    });
    indexes.set(
      table.name,
      tableIndexes.filter((candidate) => candidate !== index),
    );
  };

  const textStep = (stream: Random): void => {
    const owner = stream.int(connections);
    if (logs.size === 0 || (logs.size < 2 && stream.chance(0.04))) {
      const log: PlanTable = {
        name: `l${String(logSerial++)}`,
        appendOnly: true,
        columns: [
          { name: "body", type: "text", nullable: true },
          { name: "tag", type: "text", nullable: true },
          { name: "n", type: "integer", nullable: true },
        ],
      };
      logs.set(log.name, log);
      interactions.push({
        kind: "createTable",
        connection: owner,
        table: log,
        expectExisting: false,
      });
      return;
    }
    const log = stream.pick([...logs.values()]);
    const roll = stream.next();
    if (roll < 0.45) {
      const rows = Array.from({ length: 1 + stream.int(6) }, () => ({
        id: ++logRowSerial,
        body: generatePhrase(stream),
        tag: generatePhrase(stream),
        n: stream.chance(0.2) ? null : stream.int(101) - 50,
      }));
      interactions.push({
        kind: "insert",
        connection: owner,
        table: log.name,
        rows,
        viaParameters: stream.chance(0.5),
      });
      return;
    }
    const keyed = [...live.values()].filter((table) =>
      table.columns.some((column) => column.type === "text"),
    );
    if (roll < 0.85 || keyed.length === 0) {
      interactions.push({
        kind: "match",
        connection: owner,
        table: log.name,
        columns: stream.pick([["body"], ["tag"], ["body", "tag"]]),
        query: generateTextQuery(stream),
        and: stream.chance(0.25)
          ? { kind: "compare", column: "id", op: ">", value: stream.int(logRowSerial + 1) }
          : null,
      });
      return;
    }
    const table = stream.pick(keyed);
    const text = table.columns
      .filter((column) => column.type === "text")
      .map((column) => column.name);
    const first = stream.pick(text);
    const second = stream.pick(text);
    interactions.push({
      kind: "match",
      connection: owner,
      table: table.name,
      columns: first === second ? [first] : [first, second],
      query: generateTextQuery(stream),
      and: stream.chance(0.25) ? generatePredicate(stream, table, keySpace, 0) : null,
    });
  };

  const burst = (stream: Random, table: PlanTable): void => {
    const maximum = burstCommits ?? 1;
    interactions.push({
      kind: "burst",
      table: table.name,
      seed: stream.int(0x7fff_ffff),
      commits: Math.ceil(maximum / 2) + stream.int(Math.floor(maximum / 2) + 1),
      keySpace,
    });
    bursts++;
  };

  createTable();
  for (let step = 0; step < length; step++) {
    const table = anyTable();
    if (table === undefined || (live.size < tableLimit && random.chance(0.04))) {
      createTable();
      continue;
    }
    const stepStart = interactions.length;
    const roll = random.next();
    if (roll < 0.2) {
      const rows = Array.from({ length: 1 + random.int(4) }, () =>
        generateRow(random, table, keySpace),
      );
      interactions.push({
        kind: "insert",
        connection: connection(),
        table: table.name,
        rows: distinctIds(rows),
        viaParameters: random.chance(0.5),
      });
    } else if (roll < 0.3) {
      interactions.push({
        kind: "update",
        connection: connection(),
        table: table.name,
        predicate: generatePredicate(random, table, keySpace, 2),
        assignments: generateAssignments(random, table),
      });
    } else if (roll < 0.37) {
      interactions.push({
        kind: "delete",
        connection: connection(),
        table: table.name,
        predicate: generatePredicate(random, table, keySpace, 2),
      });
    } else if (roll < 0.52) {
      interactions.push({
        kind: "select",
        connection: connection(),
        table: table.name,
        predicate: generatePredicate(random, table, keySpace, 3),
        limit: random.chance(0.4) ? random.int(6) : null,
        descending: random.chance(0.3),
      });
    } else if (roll < 0.6) {
      interactions.push({
        kind: "partition",
        connection: connection(),
        table: table.name,
        predicate: generatePredicate(random, table, keySpace, 3),
      });
    } else if (roll < 0.65) {
      interactions.push({
        kind: "unionAll",
        connection: connection(),
        table: table.name,
        left: generatePredicate(random, table, keySpace, 2),
        right: generatePredicate(random, table, keySpace, 2),
      });
    } else if (roll < 0.69) {
      const serial = (indexCount.get(table.name) ?? 0) + 1;
      indexCount.set(table.name, serial);
      const first = random.pick(table.columns).name;
      const columns = random.chance(0.4)
        ? [
            first,
            random.pick(
              table.columns
                .filter((column) => column.name !== first)
                .map((column) => column.name)
                .concat("id"),
            ),
          ]
        : [first];
      interactions.push({
        kind: "createIndex",
        connection: connection(),
        table: table.name,
        name: `${table.name}_ix${String(serial)}`,
        columns,
      });
      trackIndex(table.name, {
        name: `${table.name}_ix${String(serial)}`,
        columns,
        unique: false,
        definite: true,
      });
    } else if (roll < 0.72 && live.size > 1 && (children.get(table.name)?.size ?? 0) > 0) {
      // Another table's enforced foreign key references this one: the drop is refused and the
      // table stays. The draws match the unrefused branch's.
      interactions.push({
        kind: "dropTable",
        connection: connection(),
        table: table.name,
        expectMissing: false,
        expectRefused: true,
      });
      random.chance(0.4);
    } else if (roll < 0.72 && live.size > 1) {
      live.delete(table.name);
      forgetTable(table.name);
      interactions.push({
        kind: "dropTable",
        connection: connection(),
        table: table.name,
        expectMissing: false,
      });
      if (random.chance(0.4)) {
        interactions.push({
          kind: "dropTable",
          connection: connection(),
          table: table.name,
          expectMissing: true,
        });
      }
    } else if (roll < 0.79) {
      const statements: TransactionStatement[] = [];
      const count = 1 + random.int(3);
      for (let index = 0; index < count; index++) {
        const inner = random.next();
        if (inner < 0.5) {
          statements.push({
            kind: "insert",
            rows: distinctIds(
              Array.from({ length: 1 + random.int(2) }, () => generateRow(random, table, keySpace)),
            ),
          });
        } else if (inner < 0.8) {
          statements.push({
            kind: "update",
            predicate: generatePredicate(random, table, keySpace, 2),
            assignments: generateAssignments(random, table),
          });
        } else {
          statements.push({
            kind: "delete",
            predicate: generatePredicate(random, table, keySpace, 1),
          });
        }
      }
      const owner = connection();
      interactions.push({
        kind: "transaction",
        connection: owner,
        observer:
          connections === 1 ? owner : (owner + 1 + random.int(connections - 1)) % connections,
        table: table.name,
        statements,
        outcome: random.chance(0.5) ? "commit" : "rollback",
      });
    } else if (roll < 0.89 && connections > 1) {
      const operations: Array<{ connection: number; mutation: KeyedMutation }> = [];
      const used = new Set<number>();
      const count = Math.min(connections * 2, 2 + random.int(connections * 2));
      for (let index = 0; index < count; index++) {
        const id = 1 + random.int(keySpace);
        if (used.has(id)) continue;
        used.add(id);
        operations.push({
          connection: index % connections,
          mutation: generateKeyedMutation(random, table, id),
        });
      }
      const readers = Array.from({ length: connections }, (_, index) => index).filter(() =>
        random.chance(0.4),
      );
      interactions.push({ kind: "concurrent", table: table.name, operations, readers });
    } else if (roll < 0.92) {
      interactions.push({
        kind: "fault",
        connection: connection(),
        table: table.name,
        mutation: generateKeyedMutation(random, table, 1 + random.int(keySpace)),
        point: random.pick(faultPoints),
      });
    } else if (roll < 0.95) {
      interactions.push({ kind: "reopen", connection: connection() });
    } else if (roll < 0.97) {
      interactions.push({ kind: "maintenance", connection: connection(), table: table.name });
    } else {
      interactions.push({ kind: "checkpoint" });
    }
    if (rekeyRandom !== undefined && live.size > 0 && rekeyRandom.chance(0.15)) {
      const target = rekeyRandom.pick([...live.values()]);
      if (rekeyRandom.chance(0.75)) {
        const connection = rekeyRandom.int(connections);
        interactions.push({
          ...generateRekey(rekeyRandom, target, keySpace),
          connection,
          table: target.name,
        });
      } else {
        const statements: TransactionStatement[] = [];
        const count = 1 + rekeyRandom.int(3);
        for (let index = 0; index < count; index++) {
          const inner = rekeyRandom.next();
          if (inner < 0.5) statements.push(generateRekey(rekeyRandom, target, keySpace));
          else if (inner < 0.7) {
            statements.push({
              kind: "insert",
              rows: distinctIds([generateRow(rekeyRandom, target, keySpace)]),
            });
          } else if (inner < 0.9) {
            statements.push({
              kind: "update",
              predicate: generatePredicate(rekeyRandom, target, keySpace, 1),
              assignments: generateAssignments(rekeyRandom, target),
            });
          } else {
            statements.push({
              kind: "delete",
              predicate: generatePredicate(rekeyRandom, target, keySpace, 1),
            });
          }
        }
        const owner = rekeyRandom.int(connections);
        interactions.push({
          kind: "transaction",
          connection: owner,
          observer:
            connections === 1
              ? owner
              : (owner + 1 + rekeyRandom.int(connections - 1)) % connections,
          table: target.name,
          statements,
          outcome: rekeyRandom.chance(0.6) ? "commit" : "rollback",
        });
      }
    }
    if (constraintRandom !== undefined && live.size > 0 && constraintRandom.chance(0.12)) {
      constraintStep(constraintRandom, constraintRandom.pick([...live.values()]));
    }
    if (schemaRandom !== undefined) {
      omitDefaults(schemaRandom, stepStart);
      if (live.size > 0 && schemaRandom.chance(0.07)) {
        schemaStep(schemaRandom, schemaRandom.pick([...live.values()]));
      }
    }
    if (textRandom?.chance(0.08) === true) textStep(textRandom);
    if (burstRandom !== undefined && live.size > 0 && burstRandom.chance(0.004)) {
      burst(burstRandom, burstRandom.pick([...live.values()]));
    }
    if (step % 25 === 24) interactions.push({ kind: "checkpoint" });
  }
  // Random fault steps deliberately retain mutation diversity, including duplicates and
  // missing-key updates/deletes that never reach storage. End with one known-effective insert
  // per configured point so every supported fault mechanism gets an effective write to interrupt.
  // These suffix steps consume no RNG, preserving the random prefix for every recorded seed.
  const faultTable = live.values().next().value;
  if (faultTable === undefined) throw new Error("Plan generation ended without a live table");
  if (burstRandom !== undefined && bursts === 0) burst(burstRandom, faultTable);
  // With two or more connections, one transaction holds the writer turn while another
  // connection's write queues behind it; the keys sit past every fault probe's.
  if (connections >= 2) {
    interactions.push({
      kind: "queuedWrite",
      connection: 0,
      waiter: 1,
      table: faultTable.name,
      rows: [faultProbeRow(faultTable, keySpace + 64 + 1)],
      mutation: { kind: "insert", row: faultProbeRow(faultTable, keySpace + 64 + 2) },
      outcome: "commit",
    });
    interactions.push({
      kind: "queuedWrite",
      connection: 1,
      waiter: 0,
      table: faultTable.name,
      rows: [faultProbeRow(faultTable, keySpace + 64 + 3)],
      mutation: { kind: "upsert", row: faultProbeRow(faultTable, keySpace + 64 + 2) },
      outcome: "rollback",
    });
  }
  for (const [index, point] of [...new Set(faultPoints)].entries()) {
    interactions.push({
      kind: "fault",
      connection: index % connections,
      table: faultTable.name,
      mutation: {
        kind: "insert",
        row: faultProbeRow(faultTable, keySpace + index + 1),
      },
      point,
    });
  }
  interactions.push({ kind: "checkpoint" });
  return { version: 1, seed, connections, interactions };
}

function checkRange(name: string, value: number, minimum: number, maximum: number): void {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new RangeError(
      `Plan ${name} must be a whole number from ${String(minimum)} through ${String(maximum)}`,
    );
  }
}

function distinctIds(rows: readonly PlanRow[]): PlanRow[] {
  const seen = new Set<number>();
  return rows.filter((row) => {
    const id = idOf(row);
    if (seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}

function generateValue(random: Random, column: PlanColumn): PlanValue {
  if (column.nullable && random.chance(0.15)) return null;
  if (column.domain !== undefined && (column.type === "integer" || column.type === "text")) {
    const value = random.int(column.domain);
    return column.type === "integer" ? value : `k${String(value)}`;
  }
  switch (column.type) {
    case "integer":
      return random.int(101) - 50;
    case "real":
      // Quarters stay exact through every decimal rendering and every float accumulation.
      return (random.int(161) - 80) / 4;
    case "text":
      return random.pick(TEXT_POOL);
    case "boolean":
      return random.chance(0.5);
  }
}

function generateRow(random: Random, table: PlanTable, keySpace: number): PlanRow {
  const row: Record<string, PlanValue> = { id: 1 + random.int(keySpace) };
  for (const column of table.columns) row[column.name] = generateValue(random, column);
  return row;
}

function faultProbeRow(table: PlanTable, id: number): PlanRow {
  const row: Record<string, PlanValue> = { id };
  for (const column of table.columns) {
    // A probe must be an effective write: it names no parent, and its value in a column that
    // is or may become unique is its own, outside every generated domain.
    if (column.references !== undefined && column.references !== "self") {
      row[column.name] = null;
      continue;
    }
    if (
      (column.domain !== undefined || column.unique === true) &&
      (column.type === "integer" || column.type === "text")
    ) {
      row[column.name] = column.type === "integer" ? 1_000_000 + id : `probe-${String(id)}`;
      continue;
    }
    switch (column.type) {
      case "integer":
      case "real":
        row[column.name] = 0;
        break;
      case "text":
        row[column.name] = "fault-probe";
        break;
      case "boolean":
        row[column.name] = false;
        break;
    }
  }
  return row;
}

function generatePredicate(
  random: Random,
  table: PlanTable,
  keySpace: number,
  depth: number,
): Predicate {
  const roll = random.next();
  if (depth > 0 && roll < 0.25) {
    return {
      kind: random.chance(0.5) ? "and" : "or",
      left: generatePredicate(random, table, keySpace, depth - 1),
      right: generatePredicate(random, table, keySpace, depth - 1),
    };
  }
  if (depth > 0 && roll < 0.35) {
    return { kind: "not", inner: generatePredicate(random, table, keySpace, depth - 1) };
  }
  if (roll < 0.4) return { kind: "literal", value: random.chance(0.5) };
  if (roll < 0.55) {
    return { kind: "isNull", column: random.pick(table.columns).name, negated: random.chance(0.5) };
  }
  if (roll < 0.7) {
    return {
      kind: "compare",
      column: "id",
      op: random.pick<CompareOperator>(["=", "<>", "<", "<=", ">", ">="]),
      value: 1 + random.int(keySpace),
    };
  }
  const column = random.pick(table.columns);
  const value = random.chance(0.05) ? null : generateValue(random, { ...column, nullable: false });
  const operators: readonly CompareOperator[] =
    column.type === "boolean" ? ["=", "<>"] : ["=", "<>", "<", "<=", ">", ">="];
  return { kind: "compare", column: column.name, op: random.pick(operators), value };
}

function generateAssignments(random: Random, table: PlanTable): Assignment[] {
  const count = 1 + random.int(Math.min(2, table.columns.length));
  const chosen = new Set<string>();
  const assignments: Assignment[] = [];
  while (assignments.length < count) {
    const column = random.pick(table.columns);
    if (chosen.has(column.name)) continue;
    chosen.add(column.name);
    if (column.type === "integer" && random.chance(0.5)) {
      assignments.push({ kind: "increment", column: column.name, by: random.int(21) - 10 });
    } else {
      assignments.push({ kind: "set", column: column.name, value: generateValue(random, column) });
    }
  }
  return assignments;
}

/**
 * A rekey statement. New keys come from `[-2k, k]`, which overlaps the keys inserts use, so
 * rekeys collide with live rows and with keys earlier rekeys retired, and from the negative
 * range an offline app gives its temporary keys. None reaches the fault probes past `k`.
 */
function generateRekey(
  random: Random,
  table: PlanTable,
  keySpace: number,
): Extract<TransactionStatement, { kind: "rekey" }> {
  const key = (): number => {
    const value = random.int(keySpace * 3) - keySpace * 2;
    return value >= 0 ? value + 1 : value;
  };
  const idIs = (value: number): Predicate => ({
    kind: "compare",
    column: "id",
    op: "=",
    value,
  });
  const plain = { ...table, columns: table.columns.filter((column) => !column.references) };
  const assignments =
    plain.columns.length > 0 && random.chance(0.3) ? generateAssignments(random, plain) : [];
  const roll = random.next();
  if (roll < 0.4) {
    return {
      kind: "rekey",
      predicate: idIs(key()),
      key: { kind: "to", value: key() },
      assignments,
    };
  }
  if (roll < 0.6) {
    // A range moving up or down by a step: every row but the last lands on a key another
    // matched row gives up, so nothing forwards from those keys.
    const by = (1 + random.int(3)) * (random.chance(0.5) ? 1 : -1);
    const predicate: Predicate = random.chance(0.6)
      ? {
          kind: "compare",
          column: "id",
          op: random.pick<CompareOperator>(["<", "<=", ">", ">="]),
          value: key(),
        }
      : generatePredicate(random, table, keySpace, 1);
    return { kind: "rekey", predicate, key: { kind: "shift", by }, assignments };
  }
  if (roll < 0.75) {
    // Every matched key negated: the shape of moving server keys into the temporary range.
    return {
      kind: "rekey",
      predicate: generatePredicate(random, table, keySpace, 1),
      key: { kind: "reflect", around: 0 },
      assignments,
    };
  }
  const left = key();
  let right = key();
  if (right === left) right = left === 1 ? 2 : 1;
  return {
    kind: "rekey",
    predicate: { kind: "or", left: idIs(left), right: idIs(right) },
    key: { kind: "reflect", around: left + right },
    assignments,
  };
}

/** Interaction kinds that can store rows in their table. */
const WRITING_KINDS: ReadonlySet<Interaction["kind"]> = new Set([
  "insert",
  "update",
  "delete",
  "rekey",
  "transaction",
  "concurrent",
  "fault",
  "queuedWrite",
  "upsertUnique",
  "burst",
]);

/** An index the generator created: `definite` unless its build may meet a duplicate. */
interface GeneratedIndex {
  readonly name: string;
  readonly columns: readonly string[];
  readonly unique: boolean;
  readonly definite: boolean;
}

/**
 * A predicate the secondary-index path can serve: equality, `IN`, or a range on the leading
 * column, or equality on it and a range on the next, sometimes with an unindexed conjunct.
 */
function indexPredicate(
  random: Random,
  table: PlanTable,
  columns: readonly string[],
  keySpace: number,
): Predicate {
  const columnOf = (name: string | undefined): PlanColumn =>
    table.columns.find((column) => column.name === name) ?? {
      name: "id",
      type: "integer",
      nullable: false,
    };
  const value = (column: PlanColumn): PlanValue =>
    column.name === "id"
      ? 1 + random.int(keySpace)
      : generateValue(random, { ...column, nullable: false });
  const compare = (column: PlanColumn, op: CompareOperator, at: PlanValue): Predicate => ({
    kind: "compare",
    column: column.name,
    op,
    value: at,
  });
  const leading = columnOf(columns[0]);
  const roll = random.next();
  let predicate: Predicate;
  if (roll < 0.4 || (leading.type === "boolean" && roll < 0.65)) {
    predicate = compare(leading, "=", value(leading));
  } else if (roll < 0.65) {
    const values = new Map<string, PlanValue>();
    const count = 1 + random.int(4);
    for (let index = 0; index < count; index++) {
      const at = value(leading);
      values.set(canonicalValue(at), at);
    }
    predicate = { kind: "in", column: leading.name, values: [...values.values()] };
  } else if (roll < 0.9 || columns.length < 2) {
    if (leading.type === "boolean") {
      predicate = { kind: "in", column: leading.name, values: [true, false] };
    } else {
      const [low, high] = [value(leading), value(leading)].sort(compareValues);
      const lower = compare(leading, random.pick<CompareOperator>([">", ">="]), low ?? null);
      const upper = compare(leading, random.pick<CompareOperator>(["<", "<="]), high ?? null);
      const shape = random.next();
      predicate =
        shape < 0.3 ? lower : shape < 0.6 ? upper : { kind: "and", left: lower, right: upper };
    }
  } else {
    const next = columnOf(columns[1]);
    predicate = {
      kind: "and",
      left: compare(leading, "=", value(leading)),
      right:
        next.type === "boolean"
          ? compare(next, "=", value(next))
          : compare(next, random.pick<CompareOperator>(["<", "<=", ">", ">="]), value(next)),
    };
  }
  if (random.chance(0.3)) {
    predicate = {
      kind: "and",
      left: predicate,
      right: generatePredicate(random, table, keySpace, 1),
    };
  }
  return predicate;
}

function generateLookup(
  random: Random,
  table: PlanTable,
  columns: readonly string[],
  keySpace: number,
): Pick<
  Extract<Interaction, { kind: "lookup" }>,
  "predicate" | "orderBy" | "descending" | "limit"
> {
  const predicate = indexPredicate(random, table, columns, keySpace);
  const leading = columns[0] ?? "id";
  return {
    predicate,
    orderBy: leading !== "id" && random.chance(0.6) ? leading : null,
    descending: random.chance(0.3),
    limit: random.chance(0.3) ? 1 + random.int(5) : null,
  };
}

/** Words a full-text column holds and a query asks for: cases, punctuation, and NFKC forms. */
const TEXT_VOCABULARY = [
  "alpha",
  "alphabet",
  "alp",
  "bravo",
  "Charlie",
  "delta",
  "echo",
  "Echo-location",
  "fox trot",
  "Golf",
  "ＧＯＬＦ",
  "hotel",
  "o'clock",
  "zulu",
  "42",
] as const;

function generatePhrase(random: Random): string | null {
  if (random.chance(0.1)) return null;
  const words = Array.from({ length: 1 + random.int(3) }, (): string =>
    random.pick(TEXT_VOCABULARY),
  );
  return words.reduce(
    (phrase, word) => `${phrase}${random.pick([" ", " ", ", ", "-", "/"])}${word}`,
  );
}

/** One or two terms, sometimes a prefix (`go*`), sometimes a word no row holds. */
function generateTextQuery(random: Random): string {
  const chunks = Array.from({ length: 1 + random.int(2) }, () => {
    const roll = random.next();
    if (roll < 0.1) return "xylophone";
    const word = random.pick(random.pick(TEXT_VOCABULARY).split(" "));
    const cased = random.chance(0.3) ? word.toUpperCase() : word;
    return roll < 0.35 ? `${cased.slice(0, 1 + random.int(3))}*` : cased;
  });
  return chunks.join(" ");
}

function generateKeyedMutation(random: Random, table: PlanTable, id: number): KeyedMutation {
  const roll = random.next();
  if (roll < 0.35) return { kind: "insert", row: { ...generateRow(random, table, 1), id } };
  if (roll < 0.6) return { kind: "upsert", row: { ...generateRow(random, table, 1), id } };
  if (roll < 0.85)
    return { kind: "updateKey", id, assignments: generateAssignments(random, table) };
  return { kind: "deleteKey", id };
}

// --- Plan validation ----------------------------------------------------------------------------------

export function parseInteractionPlan(source: string): InteractionPlan {
  const value: unknown = JSON.parse(source);
  if (!isRecord(value)) throw new TypeError("Interaction plan must be an object");
  if (value.version !== 1) throw new TypeError("Interaction plan version must be 1");
  if (!Number.isSafeInteger(value.seed))
    throw new TypeError("Interaction plan seed must be a whole number");
  const connections = value.connections;
  if (
    typeof connections !== "number" ||
    !Number.isSafeInteger(connections) ||
    connections < 1 ||
    connections > 16
  ) {
    throw new TypeError("Interaction plan connection count is invalid");
  }
  const interactions = value.interactions;
  if (!Array.isArray(interactions) || interactions.length > 500_000) {
    throw new TypeError("Interaction plan interactions must be a bounded array");
  }
  interactions.forEach((interaction: unknown, index) => {
    validateInteraction(interaction, connections, index);
  });
  return value as unknown as InteractionPlan;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validateInteraction(value: unknown, connections: number, index: number): void {
  const where = `interaction ${String(index)}`;
  if (!isRecord(value)) throw new TypeError(`${where} must be an object`);
  const kind = value.kind;
  const checkConnection = (field: string): void => {
    const number = value[field];
    if (
      typeof number !== "number" ||
      !Number.isSafeInteger(number) ||
      number < 0 ||
      number >= connections
    ) {
      throw new TypeError(`${where} has an invalid ${field}`);
    }
  };
  const checkTableName = (field = "table"): void => {
    if (typeof value[field] !== "string" || !/^[a-z][a-z0-9_]*$/u.test(value[field])) {
      throw new TypeError(`${where} has an invalid ${field}`);
    }
  };
  const checkOptionalBoolean = (field: string): void => {
    if (value[field] !== undefined && typeof value[field] !== "boolean") {
      throw new TypeError(`${where} ${field} must be boolean`);
    }
  };
  switch (kind) {
    case "createTable": {
      checkConnection("connection");
      validateTable(value.table, where);
      if (typeof value.expectExisting !== "boolean")
        throw new TypeError(`${where} expectExisting must be boolean`);
      return;
    }
    case "insert": {
      checkConnection("connection");
      checkTableName();
      if (!Array.isArray(value.rows) || value.rows.length > 1_000)
        throw new TypeError(`${where} rows are unbounded`);
      value.rows.forEach((row: unknown) => {
        validateRow(row, where);
      });
      if (typeof value.viaParameters !== "boolean")
        throw new TypeError(`${where} viaParameters must be boolean`);
      return;
    }
    case "update": {
      checkConnection("connection");
      checkTableName();
      validatePredicate(value.predicate, where, 0);
      validateAssignments(value.assignments, where);
      return;
    }
    case "delete":
    case "partition": {
      checkConnection("connection");
      checkTableName();
      validatePredicate(value.predicate, where, 0);
      return;
    }
    case "rekey": {
      checkConnection("connection");
      checkTableName();
      validateRekey(value, where);
      return;
    }
    case "select": {
      checkConnection("connection");
      checkTableName();
      validatePredicate(value.predicate, where, 0);
      const limit = value.limit;
      if (
        limit !== null &&
        (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 0)
      ) {
        throw new TypeError(`${where} limit is invalid`);
      }
      if (typeof value.descending !== "boolean")
        throw new TypeError(`${where} descending must be boolean`);
      return;
    }
    case "unionAll": {
      checkConnection("connection");
      checkTableName();
      validatePredicate(value.left, where, 0);
      validatePredicate(value.right, where, 0);
      return;
    }
    case "createIndex": {
      checkConnection("connection");
      checkTableName();
      checkTableName("name");
      if (
        !Array.isArray(value.columns) ||
        value.columns.length === 0 ||
        value.columns.length > 4 ||
        !value.columns.every((column) => isName(column))
      ) {
        throw new TypeError(`${where} index columns are invalid`);
      }
      checkOptionalBoolean("unique");
      return;
    }
    case "dropIndex": {
      checkConnection("connection");
      checkTableName();
      checkTableName("name");
      if (typeof value.ifExists !== "boolean")
        throw new TypeError(`${where} ifExists must be boolean`);
      return;
    }
    case "dropTable": {
      checkConnection("connection");
      checkTableName();
      if (typeof value.expectMissing !== "boolean")
        throw new TypeError(`${where} expectMissing must be boolean`);
      checkOptionalBoolean("expectRefused");
      return;
    }
    case "alterTable": {
      checkConnection("connection");
      checkTableName();
      if (typeof value.expectRefused !== "boolean")
        throw new TypeError(`${where} expectRefused must be boolean`);
      const change = value.change;
      if (!isRecord(change)) throw new TypeError(`${where} change must be an object`);
      if (change.kind === "addColumn") validateColumn(change.column, where);
      else if (change.kind === "dropColumn") {
        if (!isName(change.column)) throw new TypeError(`${where} column is invalid`);
      } else if (change.kind === "renameColumn") {
        if (!isName(change.column) || !isName(change.to) || change.to === "id") {
          throw new TypeError(`${where} rename is invalid`);
        }
      } else throw new TypeError(`${where} change kind is invalid`);
      return;
    }
    case "upsertUnique": {
      checkConnection("connection");
      checkTableName();
      validateRow(value.row, where);
      if (
        value.target !== null &&
        (!Array.isArray(value.target) ||
          value.target.length === 0 ||
          !value.target.every((column) => isName(column)))
      ) {
        throw new TypeError(`${where} conflict target is invalid`);
      }
      if (value.assignments !== null) {
        if (value.target === null || !Array.isArray(value.assignments)) {
          throw new TypeError(`${where} upsert assignments are invalid`);
        }
        for (const assignment of value.assignments as unknown[]) {
          if (
            !isRecord(assignment) ||
            !isName(assignment.column) ||
            assignment.column === "id" ||
            (assignment.from !== "excluded" && assignment.from !== "increment")
          ) {
            throw new TypeError(`${where} upsert assignment is invalid`);
          }
        }
      }
      return;
    }
    case "lookup": {
      checkConnection("connection");
      checkTableName();
      validatePredicate(value.predicate, where, 0);
      if (value.orderBy !== null && !isName(value.orderBy))
        throw new TypeError(`${where} orderBy is invalid`);
      if (typeof value.descending !== "boolean")
        throw new TypeError(`${where} descending must be boolean`);
      const limit = value.limit;
      if (
        limit !== null &&
        (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 0)
      ) {
        throw new TypeError(`${where} limit is invalid`);
      }
      return;
    }
    case "match": {
      checkConnection("connection");
      checkTableName();
      if (
        !Array.isArray(value.columns) ||
        value.columns.length === 0 ||
        value.columns.length > 8 ||
        !value.columns.every((column) => isName(column))
      ) {
        throw new TypeError(`${where} MATCH columns are invalid`);
      }
      if (typeof value.query !== "string" || value.query.length > 256)
        throw new TypeError(`${where} MATCH query is invalid`);
      if (value.and !== null) validatePredicate(value.and, where, 0);
      return;
    }
    case "burst": {
      checkTableName();
      for (const field of ["seed", "commits", "keySpace"] as const) {
        const number = value[field];
        if (typeof number !== "number" || !Number.isSafeInteger(number) || number < 0) {
          throw new TypeError(`${where} burst ${field} is invalid`);
        }
      }
      if ((value.commits as number) > 100_000 || (value.keySpace as number) < 1) {
        throw new TypeError(`${where} burst is out of range`);
      }
      return;
    }
    case "queuedWrite": {
      checkConnection("connection");
      checkConnection("waiter");
      checkTableName();
      if (value.connection === value.waiter) throw new TypeError(`${where} waiter is the owner`);
      if (!Array.isArray(value.rows) || value.rows.length === 0 || value.rows.length > 100) {
        throw new TypeError(`${where} rows are invalid`);
      }
      value.rows.forEach((row: unknown) => {
        validateRow(row, where);
      });
      validateKeyedMutation(value.mutation, where);
      if (value.outcome !== "commit" && value.outcome !== "rollback") {
        throw new TypeError(`${where} outcome is invalid`);
      }
      return;
    }
    case "transaction": {
      checkConnection("connection");
      checkConnection("observer");
      checkTableName();
      if (
        !Array.isArray(value.statements) ||
        value.statements.length === 0 ||
        value.statements.length > 100
      ) {
        throw new TypeError(`${where} statements are invalid`);
      }
      for (const statement of value.statements as unknown[]) {
        if (!isRecord(statement)) throw new TypeError(`${where} statement must be an object`);
        if (statement.kind === "insert") {
          if (!Array.isArray(statement.rows))
            throw new TypeError(`${where} insert rows are invalid`);
          statement.rows.forEach((row: unknown) => {
            validateRow(row, where);
          });
        } else if (statement.kind === "update") {
          validatePredicate(statement.predicate, where, 0);
          validateAssignments(statement.assignments, where);
        } else if (statement.kind === "delete") {
          validatePredicate(statement.predicate, where, 0);
        } else if (statement.kind === "rekey") {
          validateRekey(statement, where);
        } else throw new TypeError(`${where} statement kind is invalid`);
      }
      if (value.outcome !== "commit" && value.outcome !== "rollback") {
        throw new TypeError(`${where} outcome is invalid`);
      }
      return;
    }
    case "concurrent": {
      checkTableName();
      if (!Array.isArray(value.operations) || value.operations.length > connections * 8) {
        throw new TypeError(`${where} operations are unbounded`);
      }
      const ids = new Set<number>();
      for (const operation of value.operations as unknown[]) {
        if (!isRecord(operation)) throw new TypeError(`${where} operation must be an object`);
        const target = operation.connection;
        if (
          typeof target !== "number" ||
          !Number.isSafeInteger(target) ||
          target < 0 ||
          target >= connections
        ) {
          throw new TypeError(`${where} operation connection is invalid`);
        }
        const id = validateKeyedMutation(operation.mutation, where);
        if (ids.has(id))
          throw new TypeError(`${where} concurrent operations must target distinct keys`);
        ids.add(id);
      }
      if (!Array.isArray(value.readers)) throw new TypeError(`${where} readers must be an array`);
      for (const reader of value.readers as unknown[]) {
        if (
          typeof reader !== "number" ||
          !Number.isSafeInteger(reader) ||
          reader < 0 ||
          reader >= connections
        ) {
          throw new TypeError(`${where} reader is invalid`);
        }
      }
      return;
    }
    case "fault": {
      checkConnection("connection");
      checkTableName();
      validateKeyedMutation(value.mutation, where);
      if (
        typeof value.point !== "string" ||
        !FAULT_POINTS.includes(value.point as FaultPointName)
      ) {
        throw new TypeError(`${where} fault point is invalid`);
      }
      return;
    }
    case "reopen": {
      checkConnection("connection");
      return;
    }
    case "maintenance": {
      checkConnection("connection");
      checkTableName();
      return;
    }
    case "checkpoint":
      return;
    default:
      throw new TypeError(`${where} kind is invalid: ${String(kind)}`);
  }
}

function validateRekey(value: Record<string, unknown>, where: string): void {
  validatePredicate(value.predicate, where, 0);
  const key = value.key;
  if (
    !isRecord(key) ||
    !(
      (key.kind === "to" && Number.isSafeInteger(key.value)) ||
      (key.kind === "shift" && Number.isSafeInteger(key.by)) ||
      (key.kind === "reflect" && Number.isSafeInteger(key.around))
    )
  ) {
    throw new TypeError(`${where} rekey key is invalid`);
  }
  if (!Array.isArray(value.assignments)) throw new TypeError(`${where} assignments are invalid`);
  if (value.assignments.length > 0) validateAssignments(value.assignments, where);
}

function validateTable(value: unknown, where: string): void {
  if (
    !isRecord(value) ||
    typeof value.name !== "string" ||
    !/^[a-z][a-z0-9_]*$/u.test(value.name)
  ) {
    throw new TypeError(`${where} table is invalid`);
  }
  if (!Array.isArray(value.columns) || value.columns.length === 0 || value.columns.length > 32) {
    throw new TypeError(`${where} table columns are invalid`);
  }
  if (value.appendOnly !== undefined && typeof value.appendOnly !== "boolean") {
    throw new TypeError(`${where} appendOnly must be boolean`);
  }
  for (const column of value.columns as unknown[]) validateColumn(column, where);
}

function isName(value: unknown): value is string {
  return typeof value === "string" && /^[a-z][a-z0-9_]*$/u.test(value);
}

function validateColumn(column: unknown, where: string): void {
  if (
    !isRecord(column) ||
    !isName(column.name) ||
    column.name === "id" ||
    !["integer", "real", "text", "boolean"].includes(column.type as string) ||
    typeof column.nullable !== "boolean" ||
    (column.unique !== undefined && typeof column.unique !== "boolean") ||
    (column.domain !== undefined &&
      (typeof column.domain !== "number" ||
        !Number.isSafeInteger(column.domain) ||
        column.domain < 1))
  ) {
    throw new TypeError(`${where} table column is invalid`);
  }
  // A reference names a key: an integer column, nullable so a delete can clear it.
  const references = column.references;
  if (
    references !== undefined &&
    (column.type !== "integer" ||
      !column.nullable ||
      (references !== "self" &&
        (!isRecord(references) ||
          !isName(references.table) ||
          !["cascade", "set null", "restrict"].includes(references.onDelete as string))))
  ) {
    throw new TypeError(`${where} column reference is invalid`);
  }
  if (column.default !== undefined) validateValue(column.default, where);
}

function validateRow(value: unknown, where: string): void {
  if (!isRecord(value)) throw new TypeError(`${where} row must be an object`);
  if (typeof value.id !== "number" || !Number.isSafeInteger(value.id)) {
    throw new TypeError(`${where} row id must be a whole number`);
  }
  for (const cell of Object.values(value)) validateValue(cell, where);
}

function validateValue(value: unknown, where: string): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number" && Number.isFinite(value)) return;
  throw new TypeError(`${where} has an invalid value`);
}

function validatePredicate(value: unknown, where: string, depth: number): void {
  if (depth > 32) throw new TypeError(`${where} predicate is too deep`);
  if (!isRecord(value)) throw new TypeError(`${where} predicate must be an object`);
  switch (value.kind) {
    case "compare":
      if (typeof value.column !== "string")
        throw new TypeError(`${where} predicate column is invalid`);
      if (!["=", "<>", "<", "<=", ">", ">="].includes(value.op as string)) {
        throw new TypeError(`${where} predicate operator is invalid`);
      }
      validateValue(value.value, where);
      return;
    case "isNull":
      if (typeof value.column !== "string" || typeof value.negated !== "boolean") {
        throw new TypeError(`${where} IS NULL predicate is invalid`);
      }
      return;
    case "in":
      if (
        typeof value.column !== "string" ||
        !Array.isArray(value.values) ||
        value.values.length === 0 ||
        value.values.length > 64
      ) {
        throw new TypeError(`${where} IN predicate is invalid`);
      }
      for (const item of value.values as unknown[]) {
        validateValue(item, where);
        if (item === null) throw new TypeError(`${where} IN list holds NULL`);
      }
      return;
    case "and":
    case "or":
      validatePredicate(value.left, where, depth + 1);
      validatePredicate(value.right, where, depth + 1);
      return;
    case "not":
      validatePredicate(value.inner, where, depth + 1);
      return;
    case "literal":
      if (typeof value.value !== "boolean")
        throw new TypeError(`${where} literal predicate is invalid`);
      return;
    default:
      throw new TypeError(`${where} predicate kind is invalid`);
  }
}

function validateAssignments(value: unknown, where: string): void {
  if (!Array.isArray(value) || value.length === 0 || value.length > 32) {
    throw new TypeError(`${where} assignments are invalid`);
  }
  for (const assignment of value as unknown[]) {
    if (
      !isRecord(assignment) ||
      typeof assignment.column !== "string" ||
      assignment.column === "id"
    ) {
      throw new TypeError(`${where} assignment is invalid`);
    }
    if (assignment.kind === "set") validateValue(assignment.value, where);
    else if (assignment.kind === "increment") {
      if (typeof assignment.by !== "number" || !Number.isSafeInteger(assignment.by)) {
        throw new TypeError(`${where} increment is invalid`);
      }
    } else throw new TypeError(`${where} assignment kind is invalid`);
  }
}

function validateKeyedMutation(value: unknown, where: string): number {
  if (!isRecord(value)) throw new TypeError(`${where} mutation must be an object`);
  if (value.kind === "insert" || value.kind === "upsert") {
    validateRow(value.row, where);
    return (value.row as { id: number }).id;
  }
  if (value.kind === "updateKey" || value.kind === "deleteKey") {
    if (typeof value.id !== "number" || !Number.isSafeInteger(value.id)) {
      throw new TypeError(`${where} mutation id is invalid`);
    }
    if (value.kind === "updateKey") validateAssignments(value.assignments, where);
    return value.id;
  }
  throw new TypeError(`${where} mutation kind is invalid`);
}

// --- Shadow model ----------------------------------------------------------------------------------

type Truth = true | false | null;

/**
 * Why the engine must refuse a statement. A statement can break several constraints at once and
 * the engine reports whichever it checks first, so any listed identity is the right answer.
 */
type RefusalKind = "unique" | "foreignKey" | "restrict";
type Refusal = readonly RefusalKind[] | undefined;

interface ModelTable {
  readonly definition: PlanTable;
  readonly rows: Map<number, PlanRow>;
  /**
   * Retired key to the key its row moved to, one step per rekey, never rewritten. A retired key
   * names the row at the end of its chain, stays reserved, and outlives the row it led to.
   */
  readonly forwards: Map<number, number>;
}

/** A secondary index by name: what a `DROP COLUMN` must refuse, and a UNIQUE one enforces. */
interface ModelIndex {
  readonly table: string;
  readonly columns: readonly string[];
  readonly unique: boolean;
}

/** What a rekey statement does to the model, or why the engine must refuse it. */
interface RekeyOutcome {
  readonly accepted: boolean;
  readonly refusal: Refusal;
  readonly matched: number;
  /** Each key the statement retired, with the key its row moved to. */
  readonly retired: ReadonlyArray<readonly [number, number]>;
}

/** An enforced foreign key, seen from its parent. */
interface ModelChild {
  readonly table: string;
  readonly column: string;
  readonly onDelete: PlanForeignKey["onDelete"];
}

class ShadowModel {
  readonly tables = new Map<string, ModelTable>();
  readonly indexes = new Map<string, ModelIndex>();

  clone(): ShadowModel {
    const copy = new ShadowModel();
    for (const [name, table] of this.tables) {
      copy.tables.set(name, {
        definition: table.definition,
        rows: new Map(table.rows),
        forwards: new Map(table.forwards),
      });
    }
    for (const [name, index] of this.indexes) copy.indexes.set(name, index);
    return copy;
  }

  /** Every table's rows, to put back when a statement turns out to be refused. */
  #saveRows(): Map<string, Map<number, PlanRow>> {
    return new Map([...this.tables].map(([name, table]) => [name, new Map(table.rows)]));
  }

  #restoreRows(saved: ReadonlyMap<string, ReadonlyMap<number, PlanRow>>): void {
    for (const [name, rows] of saved) {
      const table = this.tables.get(name);
      if (table === undefined) continue;
      table.rows.clear();
      for (const [id, row] of rows) table.rows.set(id, row);
    }
  }

  /** The current key a key names: itself, or the end of the chain of forwards it starts. */
  resolve(name: string, key: number): number {
    const forwards = this.table(name).forwards;
    let current = key;
    const visited = new Set<number>([key]);
    for (;;) {
      const next = forwards.get(current);
      if (next === undefined || visited.has(next)) return current;
      visited.add(next);
      current = next;
    }
  }

  /** The table whose key a column of `name` holds, if it holds one. */
  #referenced(name: string, column: PlanColumn): string | undefined {
    if (column.references === undefined) return undefined;
    return column.references === "self" ? name : column.references.table;
  }

  /**
   * The predicate as the engine runs it: a key equality, inequality, or `IN` against constants
   * -- on the primary key or a column referencing a key -- names the row a retired key forwards
   * to in the table that key belongs to.
   */
  resolvePredicate(name: string, predicate: Predicate): Predicate {
    const keyColumns = new Map<string, string>([["id", name]]);
    for (const column of this.table(name).definition.columns) {
      const target = this.#referenced(name, column);
      if (target !== undefined && this.tables.has(target)) keyColumns.set(column.name, target);
    }
    if (![...keyColumns.values()].some((table) => this.table(table).forwards.size > 0)) {
      return predicate;
    }
    const rewrite = (node: Predicate): Predicate => {
      switch (node.kind) {
        case "compare": {
          const target = keyColumns.get(node.column);
          return target !== undefined &&
            (node.op === "=" || node.op === "<>") &&
            typeof node.value === "number"
            ? { ...node, value: this.resolve(target, node.value) }
            : node;
        }
        case "in": {
          const target = keyColumns.get(node.column);
          return target === undefined
            ? node
            : {
                ...node,
                values: node.values.map((value) =>
                  typeof value === "number" ? this.resolve(target, value) : value,
                ),
              };
        }
        case "not":
          return { kind: "not", inner: rewrite(node.inner) };
        case "and":
        case "or":
          return { kind: node.kind, left: rewrite(node.left), right: rewrite(node.right) };
        default:
          return node;
      }
    };
    return rewrite(predicate);
  }

  evaluate(name: string, predicate: Predicate, row: PlanRow): Truth {
    return evaluate(this.resolvePredicate(name, predicate), row);
  }

  /** A row as the engine stores it: a referencing value written as a retired key is resolved. */
  #stored(name: string, row: PlanRow): PlanRow {
    let stored: Record<string, PlanValue> | undefined;
    for (const column of this.table(name).definition.columns) {
      const target = this.#referenced(name, column);
      const value = row[column.name];
      if (target === undefined || typeof value !== "number") continue;
      const table = this.tables.get(target);
      if (table === undefined || table.forwards.size === 0) continue;
      const resolved = this.resolve(target, value);
      if (resolved === value) continue;
      stored ??= { ...row };
      stored[column.name] = resolved;
    }
    return stored ?? row;
  }

  /** Column sets no two rows may share a non-null value of: inline UNIQUE and UNIQUE indexes. */
  uniqueSets(name: string): Array<readonly string[]> {
    return [
      ...this.table(name)
        .definition.columns.filter((column) => column.unique === true)
        .map((column) => [column.name]),
      ...[...this.indexes.values()]
        .filter((index) => index.table === name && index.unique)
        .map((index) => index.columns),
    ];
  }

  #duplicates(rows: Iterable<PlanRow>, columns: readonly string[]): boolean {
    const seen = new Set<string>();
    for (const row of rows) {
      const values = columns.map((column) => row[column] ?? null);
      if (values.some((value) => value === null)) continue;
      const key = values.map(canonicalValue).join("\u0000");
      if (seen.has(key)) return true;
      seen.add(key);
    }
    return false;
  }

  #violatesUnique(name: string): boolean {
    const rows = [...this.table(name).rows.values()];
    return this.uniqueSets(name).some((columns) => this.#duplicates(rows, columns));
  }

  /** Whether a written row names a parent row that does not exist. */
  #missingParent(name: string, rows: readonly PlanRow[]): boolean {
    for (const column of this.table(name).definition.columns) {
      if (column.references === undefined || column.references === "self") continue;
      const parent = this.tables.get(column.references.table);
      for (const row of rows) {
        const value = row[column.name] ?? null;
        if (value !== null && (typeof value !== "number" || parent?.rows.has(value) !== true)) {
          return true;
        }
      }
    }
    return false;
  }

  /** The enforced foreign keys that reference `name`. */
  children(name: string): ModelChild[] {
    const children: ModelChild[] = [];
    for (const [table, model] of this.tables) {
      for (const column of model.definition.columns) {
        if (typeof column.references === "object" && column.references.table === name) {
          children.push({ table, column: column.name, onDelete: column.references.onDelete });
        }
      }
    }
    return children;
  }

  /** The constraints a statement that wrote `written` into `name` breaks, if any. */
  #check(name: string, written: readonly PlanRow[], kinds: Set<RefusalKind>): Refusal {
    if (this.#missingParent(name, written)) kinds.add("foreignKey");
    if (this.#violatesUnique(name)) kinds.add("unique");
    return kinds.size === 0 ? undefined : [...kinds];
  }

  /**
   * Removes rows by key and runs every `ON DELETE` action they set off, down the tree of
   * enforced foreign keys: a `RESTRICT` child still naming a removed row refuses the statement.
   */
  #removeRows(name: string, ids: readonly number[]): Refusal {
    const queue: Array<readonly [string, readonly number[]]> = [[name, ids]];
    for (let next = queue.shift(); next !== undefined; next = queue.shift()) {
      const [table, keys] = next;
      const rows = this.table(table).rows;
      for (const key of keys) rows.delete(key);
      const removed = new Set(keys);
      for (const child of this.children(table)) {
        const childRows = this.table(child.table).rows;
        const cascaded: number[] = [];
        for (const [id, row] of childRows) {
          const value = row[child.column];
          if (typeof value !== "number" || !removed.has(value)) continue;
          if (child.onDelete === "restrict") return ["restrict"];
          if (child.onDelete === "cascade") cascaded.push(id);
          else childRows.set(id, { ...row, [child.column]: null });
        }
        if (cascaded.length > 0) queue.push([child.table, cascaded]);
      }
    }
    return undefined;
  }

  /**
   * `UPDATE t SET id = <key>[, assignments] WHERE predicate`. Every matched row moves at once:
   * the new keys must be distinct, free of rows the statement does not move, and not retired.
   * References to a moved row follow it, all at once, and each key no row holds any more
   * forwards to the key its row moved to.
   */
  rekey(
    name: string,
    predicate: Predicate,
    key: KeyAssignment,
    assignments: readonly Assignment[],
  ): RekeyOutcome {
    const table = this.table(name);
    const matched = this.matching(name, predicate);
    const refused = (refusal: Refusal): RekeyOutcome => ({
      accepted: false,
      refusal,
      matched: matched.length,
      retired: [],
    });
    const oldKeys = new Set(matched.map(idOf));
    const moves = matched.map((row) => {
      const from = idOf(row);
      const to =
        key.kind === "to" ? key.value : key.kind === "shift" ? from + key.by : key.around - from;
      return { row, from, to };
    });
    const targets = new Set<number>();
    for (const { from, to } of moves) {
      if (targets.has(to)) return refused(["unique"]);
      targets.add(to);
      if (to === from) continue;
      if (table.rows.has(to) && !oldKeys.has(to)) return refused(["unique"]);
      if (table.forwards.has(to)) return refused(["unique"]);
    }
    const saved = this.#saveRows();
    const renamed = new Map<number, number>();
    for (const { from, to } of moves) if (from !== to) renamed.set(from, to);
    for (const { from } of moves) table.rows.delete(from);
    const written: PlanRow[] = [];
    for (const { row, to } of moves) {
      const next = this.#stored(name, { ...assign(row, assignments), id: to });
      table.rows.set(to, next);
      written.push(next);
    }
    if (renamed.size > 0) {
      for (const [other, model] of this.tables) {
        const references = model.definition.columns.filter(
          (column) => this.#referenced(other, column) === name,
        );
        if (references.length === 0) continue;
        for (const [id, row] of model.rows) {
          let moved: Record<string, PlanValue> | undefined;
          for (const column of references) {
            const value = row[column.name];
            const target = typeof value === "number" ? renamed.get(value) : undefined;
            if (target === undefined) continue;
            moved ??= { ...row };
            moved[column.name] = target;
          }
          if (moved !== undefined) model.rows.set(id, moved);
        }
      }
    }
    const refusal = this.#check(name, written, new Set());
    if (refusal !== undefined) {
      this.#restoreRows(saved);
      return refused(refusal);
    }
    const retired: Array<readonly [number, number]> = [];
    for (const [from, to] of renamed) {
      if (targets.has(from)) continue;
      table.forwards.set(from, to);
      retired.push([from, to]);
    }
    return { accepted: true, refusal: undefined, matched: matched.length, retired };
  }

  table(name: string): ModelTable {
    const table = this.tables.get(name);
    if (table === undefined) throw new Error(`Model has no table ${name}`);
    return table;
  }

  columns(name: string): string[] {
    return ["id", ...this.table(name).definition.columns.map((column) => column.name)];
  }

  /** Rows matching the predicate, in ascending id order. */
  matching(name: string, predicate: Predicate): PlanRow[] {
    const resolved = this.resolvePredicate(name, predicate);
    return this.sorted(name).filter((row) => evaluate(resolved, row) === true);
  }

  sorted(name: string): PlanRow[] {
    return [...this.table(name).rows.values()].sort((left, right) => idOf(left) - idOf(right));
  }

  /**
   * A retired key stays reserved: inserting it is a duplicate, like inserting a live key. An
   * append-only table has no key to collide on. The statement lands whole or not at all.
   */
  insert(name: string, rows: readonly PlanRow[]): Refusal {
    const table = this.table(name);
    const kinds = new Set<RefusalKind>();
    const staged = new Set<number>();
    for (const row of rows) {
      const id = idOf(row);
      if (table.definition.appendOnly === true) continue;
      if (table.rows.has(id) || table.forwards.has(id) || staged.has(id)) kinds.add("unique");
      staged.add(id);
    }
    const saved = this.#saveRows();
    const written = rows.map((row) => this.#stored(name, completeRow(table.definition, row)));
    if (kinds.size === 0) for (const row of written) table.rows.set(idOf(row), row);
    const refusal = this.#check(name, written, kinds);
    if (refusal !== undefined) this.#restoreRows(saved);
    return refusal;
  }

  update(
    name: string,
    predicate: Predicate,
    assignments: readonly Assignment[],
  ): { count: number; refusal: Refusal } {
    const table = this.table(name);
    const saved = this.#saveRows();
    const matched = this.matching(name, predicate);
    const written = matched.map((row) => this.#stored(name, assign(row, assignments)));
    for (const row of written) table.rows.set(idOf(row), row);
    const refusal = this.#check(name, written, new Set());
    if (refusal !== undefined) this.#restoreRows(saved);
    return { count: matched.length, refusal };
  }

  delete(name: string, predicate: Predicate): { count: number; refusal: Refusal } {
    const saved = this.#saveRows();
    const matched = this.matching(name, predicate);
    const refusal = this.#removeRows(name, matched.map(idOf));
    if (refusal !== undefined) this.#restoreRows(saved);
    return { count: matched.length, refusal };
  }

  /**
   * Applies a keyed mutation, or reports why the engine must refuse it. A key that was retired
   * names its row's current key, so an upsert of it writes that key.
   */
  applyKeyed(name: string, mutation: KeyedMutation): Refusal {
    const table = this.table(name);
    if (mutation.kind === "insert") return this.insert(name, [mutation.row]);
    const id = this.resolve(name, mutation.kind === "upsert" ? idOf(mutation.row) : mutation.id);
    const saved = this.#saveRows();
    let refusal: Refusal;
    switch (mutation.kind) {
      case "upsert": {
        const row = this.#stored(name, completeRow(table.definition, { ...mutation.row, id }));
        table.rows.set(id, row);
        refusal = this.#check(name, [row], new Set());
        break;
      }
      case "updateKey": {
        const row = table.rows.get(id);
        if (row === undefined) return undefined;
        const next = this.#stored(name, assign(row, mutation.assignments));
        table.rows.set(id, next);
        refusal = this.#check(name, [next], new Set());
        break;
      }
      case "deleteKey":
        if (!table.rows.has(id)) return undefined;
        refusal = this.#removeRows(name, [id]);
        break;
    }
    if (refusal !== undefined) this.#restoreRows(saved);
    return refusal;
  }

  /** The row a keyed mutation reaches: its key, followed through any forwards. */
  keyedTarget(name: string, mutation: KeyedMutation): number {
    return this.resolve(
      name,
      mutation.kind === "insert" || mutation.kind === "upsert" ? idOf(mutation.row) : mutation.id,
    );
  }

  /**
   * `INSERT ... ON CONFLICT (target) DO UPDATE | DO NOTHING`. A row holding the proposal's
   * non-null target values is updated under its own key, the proposal's key ignored; with no such
   * row the proposal inserts, and any other collision is an error. A targetless `DO NOTHING`
   * skips a proposal that collides on the key or on any unique constraint. A retired proposal
   * key names the key its row moved to, whether or not that row still exists.
   */
  upsertUnique(
    name: string,
    row: PlanRow,
    target: readonly string[] | null,
    assignments: readonly UpsertAssignment[] | null,
  ): { count: number; refusal: Refusal } {
    const table = this.table(name);
    // Like every INSERT ... ON CONFLICT, the proposal's key is followed through its forwards
    // first: a retired key proposes the row at the key it moved to.
    const resolved: PlanRow = { ...row, id: this.resolve(name, idOf(row)) };
    const proposal = this.#stored(name, completeRow(table.definition, resolved));
    const holderOf = (columns: readonly string[]): PlanRow | undefined =>
      [...table.rows.values()].find((stored) =>
        columns.every((column) => {
          const value = proposal[column] ?? null;
          return value !== null && canonicalValue(stored[column] ?? null) === canonicalValue(value);
        }),
      );
    const inserted = (): { count: number; refusal: Refusal } => {
      const refusal = this.insert(name, [resolved]);
      return { count: refusal === undefined ? 1 : 0, refusal };
    };
    if (target === null) {
      const id = idOf(proposal);
      const collides =
        table.rows.has(id) ||
        table.forwards.has(id) ||
        this.uniqueSets(name).some((columns) => holderOf(columns) !== undefined);
      return collides ? { count: 0, refusal: undefined } : inserted();
    }
    const holder = holderOf(target);
    if (holder === undefined) return inserted();
    // With nothing to assign the statement renders as DO NOTHING.
    if (assignments === null || assignments.length === 0) return { count: 0, refusal: undefined };
    const next: Record<string, PlanValue> = { ...holder };
    for (const assignment of assignments) {
      const stored = holder[assignment.column] ?? null;
      next[assignment.column] =
        assignment.from === "excluded"
          ? (proposal[assignment.column] ?? null)
          : typeof stored === "number"
            ? stored + 1
            : null;
    }
    const saved = this.#saveRows();
    const written = this.#stored(name, next);
    table.rows.set(idOf(holder), written);
    const refusal = this.#check(name, [written], new Set());
    if (refusal !== undefined) this.#restoreRows(saved);
    return { count: 1, refusal };
  }

  /** Creates a table, or names the enforced foreign key that refuses a drop. */
  createTable(definition: PlanTable): void {
    this.tables.set(definition.name, { definition, rows: new Map(), forwards: new Map() });
  }

  dropTable(name: string): void {
    this.tables.delete(name);
    for (const [index, model] of this.indexes) if (model.table === name) this.indexes.delete(index);
  }

  /** Whether `CREATE [UNIQUE] INDEX` is refused (a UNIQUE one over rows holding a duplicate). */
  createIndex(name: string, table: string, columns: readonly string[], unique: boolean): boolean {
    const known = new Set(this.columns(table));
    if (columns.some((column) => !known.has(column))) {
      throw new Error(`Model has no column ${columns.join(", ")} on ${table}`);
    }
    if (unique && this.#duplicates(this.table(table).rows.values(), columns)) return true;
    this.indexes.set(name, { table, columns, unique });
    return false;
  }

  /** Whether the engine must refuse the change; when it may, the change is applied. */
  alter(name: string, change: SchemaChange): boolean {
    const table = this.table(name);
    const definition = table.definition;
    const replace = (columns: readonly PlanColumn[], rewrite: (row: PlanRow) => PlanRow): void => {
      const rows = new Map([...table.rows].map(([id, row]) => [id, rewrite(row)]));
      this.tables.set(name, {
        definition: { ...definition, columns },
        rows,
        forwards: table.forwards,
      });
    };
    if (change.kind === "addColumn") {
      if (this.columns(name).includes(change.column.name)) {
        throw new Error(`Model already has column ${change.column.name} on ${name}`);
      }
      replace([...definition.columns, change.column], (row) => ({
        ...row,
        [change.column.name]: change.column.default ?? null,
      }));
      return false;
    }
    const column = definition.columns.find((candidate) => candidate.name === change.column);
    if (column === undefined) throw new Error(`Model has no column ${change.column} on ${name}`);
    if (change.kind === "dropColumn") {
      if (definition.columns.length <= 1) throw new Error("Plan drops a table's last column");
      const indexed = [...this.indexes.values()].some(
        (index) => index.table === name && index.columns.includes(column.name),
      );
      if (column.unique === true || column.references !== undefined || indexed) return true;
      replace(
        definition.columns.filter((candidate) => candidate !== column),
        (row) => Object.fromEntries(Object.entries(row).filter(([key]) => key !== column.name)),
      );
      return false;
    }
    if (column.references !== undefined) return true;
    if (this.columns(name).includes(change.to)) {
      throw new Error(`Model already has column ${change.to} on ${name}`);
    }
    replace(
      definition.columns.map((candidate) =>
        candidate === column ? { ...candidate, name: change.to } : candidate,
      ),
      (row) =>
        Object.fromEntries(
          Object.entries(row).map(([key, value]) => [key === column.name ? change.to : key, value]),
        ),
    );
    for (const [index, model] of this.indexes) {
      if (model.table !== name) continue;
      this.indexes.set(index, {
        ...model,
        columns: model.columns.map((candidate) =>
          candidate === column.name ? change.to : candidate,
        ),
      });
    }
    return false;
  }
}

function idOf(row: PlanRow): number {
  const id = row.id;
  if (typeof id !== "number") throw new Error("Row without a numeric id");
  return id;
}

/** A row as written: a column it leaves out takes the column's default, or NULL. */
function completeRow(definition: PlanTable, row: PlanRow): PlanRow {
  const complete: Record<string, PlanValue> = { id: idOf(row) };
  for (const column of definition.columns) {
    complete[column.name] = Object.hasOwn(row, column.name)
      ? (row[column.name] ?? null)
      : (column.default ?? null);
  }
  return complete;
}

function assign(row: PlanRow, assignments: readonly Assignment[]): PlanRow {
  const next: Record<string, PlanValue> = { ...row };
  for (const assignment of assignments) {
    if (assignment.kind === "set") next[assignment.column] = assignment.value;
    else {
      const current = row[assignment.column];
      next[assignment.column] = typeof current === "number" ? current + assignment.by : null;
    }
  }
  return next;
}

/** Full-text terms: NFKC, lowercased, runs of letters and digits. */
function textTokens(text: string): string[] {
  return (
    text
      .normalize("NFKC")
      .toLowerCase()
      .match(/[\p{L}\p{N}]+/gu) ?? []
  );
}

/**
 * Whether a row matches `MATCH(columns) AGAINST query`: every query term -- a chunk ending in
 * `*` makes its last term a prefix -- is a token of some listed column. No terms match nothing.
 */
function matchesText(row: PlanRow, columns: readonly string[], query: string): boolean {
  const terms: Array<{ term: string; prefix: boolean }> = [];
  for (const chunk of query.split(/\s+/u)) {
    if (chunk === "") continue;
    const prefix = chunk.endsWith("*");
    const tokens = textTokens(prefix ? chunk.slice(0, -1) : chunk);
    tokens.forEach((term, index) => {
      terms.push({ term, prefix: prefix && index === tokens.length - 1 });
    });
  }
  if (terms.length === 0) return false;
  const tokens = columns.flatMap((column) => {
    const value = row[column];
    return typeof value === "string" ? textTokens(value) : [];
  });
  return terms.every(({ term, prefix }) =>
    tokens.some((token) => (prefix ? token.startsWith(term) : token === term)),
  );
}

function compareValues(left: PlanValue, right: PlanValue): number {
  if (typeof left === "number" && typeof right === "number")
    return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "string" && typeof right === "string") return compareCodepoints(left, right);
  if (typeof left === "boolean" && typeof right === "boolean") return Number(left) - Number(right);
  throw new Error(`Model cannot compare ${typeof left} with ${typeof right}`);
}

function compareCodepoints(left: string, right: string): number {
  let leftIndex = 0;
  let rightIndex = 0;
  while (leftIndex < left.length && rightIndex < right.length) {
    const leftPoint = left.codePointAt(leftIndex) ?? 0;
    const rightPoint = right.codePointAt(rightIndex) ?? 0;
    if (leftPoint !== rightPoint) return leftPoint - rightPoint;
    leftIndex += leftPoint > 0xffff ? 2 : 1;
    rightIndex += rightPoint > 0xffff ? 2 : 1;
  }
  return left.length - leftIndex - (right.length - rightIndex);
}

/** SQL three-valued logic over one row. */
export function evaluate(predicate: Predicate, row: PlanRow): Truth {
  switch (predicate.kind) {
    case "literal":
      return predicate.value;
    case "isNull": {
      const isNull = (row[predicate.column] ?? null) === null;
      return predicate.negated ? !isNull : isNull;
    }
    case "compare": {
      const value = row[predicate.column] ?? null;
      if (value === null || predicate.value === null) return null;
      const order = compareValues(value, predicate.value);
      switch (predicate.op) {
        case "=":
          return order === 0;
        case "<>":
          return order !== 0;
        case "<":
          return order < 0;
        case "<=":
          return order <= 0;
        case ">":
          return order > 0;
        case ">=":
          return order >= 0;
      }
    }
    // eslint-disable-next-line no-fallthrough -- every operator above returns
    case "in": {
      const value = row[predicate.column] ?? null;
      if (value === null) return null;
      return predicate.values.some((candidate) => compareValues(value, candidate) === 0);
    }
    case "not": {
      const inner = evaluate(predicate.inner, row);
      return inner === null ? null : !inner;
    }
    case "and": {
      const left = evaluate(predicate.left, row);
      const right = evaluate(predicate.right, row);
      if (left === false || right === false) return false;
      if (left === null || right === null) return null;
      return true;
    }
    case "or": {
      const left = evaluate(predicate.left, row);
      const right = evaluate(predicate.right, row);
      if (left === true || right === true) return true;
      if (left === null || right === null) return null;
      return false;
    }
  }
}

// --- SQL rendering ----------------------------------------------------------------------------------

function quote(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

export function renderLiteral(value: PlanValue): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return String(value);
    const text = value.toFixed(2).replace(/0$/u, "");
    return text;
  }
  return `'${value.replaceAll("'", "''")}'`;
}

export function renderPredicate(predicate: Predicate): string {
  switch (predicate.kind) {
    case "literal":
      return predicate.value ? "TRUE" : "FALSE";
    case "isNull":
      return `${quote(predicate.column)} IS ${predicate.negated ? "NOT " : ""}NULL`;
    case "compare":
      return `${quote(predicate.column)} ${predicate.op} ${renderLiteral(predicate.value)}`;
    case "in":
      return `${quote(predicate.column)} IN (${predicate.values.map(renderLiteral).join(", ")})`;
    case "not":
      return `NOT (${renderPredicate(predicate.inner)})`;
    case "and":
      return `(${renderPredicate(predicate.left)} AND ${renderPredicate(predicate.right)})`;
    case "or":
      return `(${renderPredicate(predicate.left)} OR ${renderPredicate(predicate.right)})`;
  }
}

function renderAssignments(assignments: readonly Assignment[]): string {
  return assignments
    .map((assignment) =>
      assignment.kind === "set"
        ? `${quote(assignment.column)} = ${renderLiteral(assignment.value)}`
        : `${quote(assignment.column)} = ${quote(assignment.column)} + ${renderLiteral(assignment.by)}`,
    )
    .join(", ");
}

function renderKeyAssignment(key: KeyAssignment): string {
  switch (key.kind) {
    case "to":
      return `id = ${renderLiteral(key.value)}`;
    case "shift":
      return key.by < 0 ? `id = id - ${String(-key.by)}` : `id = id + ${String(key.by)}`;
    case "reflect":
      return `id = ${renderLiteral(key.around)} - id`;
  }
}

function renderRekey(
  table: string,
  statement: Pick<
    Extract<TransactionStatement, { kind: "rekey" }>,
    "predicate" | "key" | "assignments"
  >,
): string {
  const assignments = [
    renderKeyAssignment(statement.key),
    ...(statement.assignments.length === 0 ? [] : [renderAssignments(statement.assignments)]),
  ].join(", ");
  return `UPDATE ${quote(table)} SET ${assignments} WHERE ${renderPredicate(statement.predicate)}`;
}

function sqlType(type: ColumnType): string {
  switch (type) {
    case "integer":
      return "INTEGER";
    case "real":
      return "DOUBLE PRECISION";
    case "text":
      return "TEXT";
    case "boolean":
      return "BOOLEAN";
  }
}

function renderColumn(table: string, column: PlanColumn): string {
  const references = column.references;
  return (
    `${quote(column.name)} ${sqlType(column.type)}${column.nullable ? "" : " NOT NULL"}` +
    (column.default === undefined ? "" : ` DEFAULT ${renderLiteral(column.default)}`) +
    (column.unique === true ? " UNIQUE" : "") +
    (references === undefined
      ? ""
      : references === "self"
        ? ` REFERENCES ${quote(table)} (id) NOT ENFORCED`
        : ` REFERENCES ${quote(references.table)} (id) ON DELETE ${references.onDelete.toUpperCase()}`)
  );
}

function renderCreateTable(table: PlanTable): string {
  const columns = table.columns.map((column) => renderColumn(table.name, column));
  const key = table.appendOnly === true ? "id INTEGER NOT NULL" : "id INTEGER PRIMARY KEY";
  return `CREATE TABLE ${quote(table.name)} (${key}, ${columns.join(", ")})`;
}

function renderAlter(table: string, change: SchemaChange): string {
  const target = `ALTER TABLE ${quote(table)}`;
  switch (change.kind) {
    case "addColumn":
      return `${target} ADD COLUMN ${renderColumn(table, change.column)}`;
    case "dropColumn":
      return `${target} DROP COLUMN ${quote(change.column)}`;
    case "renameColumn":
      return `${target} RENAME COLUMN ${quote(change.column)} TO ${quote(change.to)}`;
  }
}

function renderUpsertUnique(
  interaction: Extract<Interaction, { kind: "upsertUnique" }>,
  columns: readonly string[],
): string {
  const insert = renderInsert(interaction.table, columns, [interaction.row], false).sql.replace(
    /^INSERT INTO "[^"]*"/u,
    `INSERT INTO ${quote(interaction.table)} AS existing`,
  );
  if (interaction.target === null) return `${insert} ON CONFLICT DO NOTHING`;
  const conflict = `${insert} ON CONFLICT (${interaction.target.map(quote).join(", ")})`;
  if (interaction.assignments === null || interaction.assignments.length === 0) {
    return `${conflict} DO NOTHING`;
  }
  const set = interaction.assignments
    .map((assignment) =>
      assignment.from === "excluded"
        ? `${quote(assignment.column)} = excluded.${quote(assignment.column)}`
        : `${quote(assignment.column)} = existing.${quote(assignment.column)} + 1`,
    )
    .join(", ");
  return `${conflict} DO UPDATE SET ${set}`;
}

/**
 * Columns a statement names: every column the rows all hold. A column every row leaves out is
 * omitted, so its default fills it.
 */
function insertColumns(columns: readonly string[], rows: readonly PlanRow[]): string[] {
  return columns.filter(
    (column) => column === "id" || rows.every((row) => Object.hasOwn(row, column)),
  );
}

function renderInsert(
  table: string,
  allColumns: readonly string[],
  rows: readonly PlanRow[],
  viaParameters: boolean,
): { sql: string; params: PlanValue[] } {
  const columns = insertColumns(allColumns, rows);
  const params: PlanValue[] = [];
  const tuples = rows.map((row) => {
    const cells = columns.map((column) => {
      const value = row[column] ?? null;
      if (!viaParameters) return renderLiteral(value);
      params.push(value);
      return "?";
    });
    return `(${cells.join(", ")})`;
  });
  return {
    sql: `INSERT INTO ${quote(table)} (${columns.map(quote).join(", ")}) VALUES ${tuples.join(", ")}`,
    params,
  };
}

function renderKeyed(table: string, columns: readonly string[], mutation: KeyedMutation): string {
  switch (mutation.kind) {
    case "insert":
      return renderInsert(table, columns, [mutation.row], false).sql;
    case "upsert": {
      const insert = renderInsert(table, columns, [mutation.row], false).sql;
      const updates = columns
        .filter((column) => column !== "id")
        .map((column) => `${quote(column)} = excluded.${quote(column)}`)
        .join(", ");
      return `${insert} ON CONFLICT (id) DO UPDATE SET ${updates}`;
    }
    case "updateKey":
      return `UPDATE ${quote(table)} SET ${renderAssignments(mutation.assignments)} WHERE id = ${String(mutation.id)}`;
    case "deleteKey":
      return `DELETE FROM ${quote(table)} WHERE id = ${String(mutation.id)}`;
  }
}

// --- Runner ----------------------------------------------------------------------------------------

export async function runInteractionPlan(
  plan: InteractionPlan,
  driver: SimulationDriver,
  options: InteractionRunOptions = {},
): Promise<InteractionRunResult> {
  parseInteractionPlan(JSON.stringify(plan));
  const runner = new PlanRunner(plan, driver, options.traceLength ?? 40);
  try {
    return await runner.run();
  } finally {
    await driver.close?.();
  }
}

class PlanRunner {
  readonly #plan: InteractionPlan;
  readonly #driver: SimulationDriver;
  readonly #traceLength: number;
  readonly #trace: string[] = [];
  readonly #connections: SimulatedConnection[] = [];
  #model = new ShadowModel();
  #index = 0;
  #statements = 0;
  #queries = 0;
  #acceptedWrites = 0;
  #rejectedConflicts = 0;
  #expectedFailures = 0;
  #faultsInjected = 0;
  #faultsSkipped = 0;
  #reopens = 0;
  #checkpoints = 0;
  #indexPathQueries = 0;
  #burstCommits = 0;
  #peakBurstSegments = 0;

  constructor(plan: InteractionPlan, driver: SimulationDriver, traceLength: number) {
    this.#plan = plan;
    this.#driver = driver;
    this.#traceLength = traceLength;
  }

  async run(): Promise<InteractionRunResult> {
    for (let index = 0; index < this.#plan.connections; index++) {
      this.#connections.push(await this.#driver.open(index));
    }
    let judged = 0;
    let stoppedBy: string | undefined;
    for (const [index, interaction] of this.#plan.interactions.entries()) {
      this.#index = index;
      try {
        await this.#step(interaction);
      } catch (error) {
        if (error instanceof AcceptedStoreUnresponsive) {
          stoppedBy = describeError(error.storageError);
          break;
        }
        if (error instanceof InteractionFailure) throw error;
        this.#fail(`${interaction.kind} failed: ${describeError(error)}`, error);
      }
      judged += 1;
    }
    let rows = 0;
    for (const table of this.#model.tables.values()) rows += table.rows.size;
    return {
      seed: this.#plan.seed,
      interactions: judged,
      statements: this.#statements,
      queries: this.#queries,
      acceptedWrites: this.#acceptedWrites,
      rejectedConflicts: this.#rejectedConflicts,
      expectedFailures: this.#expectedFailures,
      faultsInjected: this.#faultsInjected,
      faultsSkipped: this.#faultsSkipped,
      reopens: this.#reopens,
      checkpoints: this.#checkpoints,
      tablesAtEnd: this.#model.tables.size,
      rowsAtEnd: rows,
      indexPathQueries: this.#indexPathQueries,
      burstCommits: this.#burstCommits,
      peakBurstSegments: this.#peakBurstSegments,
      transientsAccepted: stoppedBy === undefined ? 0 : 1,
      stoppedBy,
    };
  }

  #connection(index: number): SimulatedConnection {
    const connection = this.#connections[index];
    if (connection === undefined) throw new Error(`No connection ${String(index)}`);
    return connection;
  }

  #record(sql: string): void {
    this.#trace.push(sql);
    if (this.#trace.length > this.#traceLength) this.#trace.shift();
  }

  /** `cause` keeps the engine's own stack when the failure wraps an error it threw. */
  #fail(message: string, cause?: unknown): never {
    const interaction = this.#plan.interactions[this.#index];
    if (interaction === undefined) throw new Error(message, { cause });
    throw new InteractionFailure(message, this.#index, interaction, this.#trace, { cause });
  }

  async #execute(
    connection: number,
    sql: string,
    params?: readonly PlanValue[],
  ): Promise<SimulatedExecuteResult> {
    this.#record(
      `[${String(connection)}] ${sql}${params === undefined ? "" : ` -- ${JSON.stringify(params)}`}`,
    );
    this.#statements++;
    return this.#connection(connection).execute(sql, params);
  }

  async #query(connection: number, sql: string): Promise<SimulatedQueryResult> {
    this.#record(`[${String(connection)}] ${sql}`);
    this.#queries++;
    const result = await this.#connection(connection).query(sql);
    if (new Set(result.columns).size !== result.columns.length) {
      this.#fail("query result has duplicate columns");
    }
    for (const row of result.rows) {
      const missing = result.columns.filter(
        (column) => !Object.hasOwn(row, column) || row[column] === undefined,
      );
      const unexpected = Object.keys(row).filter((column) => !result.columns.includes(column));
      if (missing.length > 0 || unexpected.length > 0) {
        this.#fail(
          `malformed query row: missing ${missing.join(", ")}; unexpected ${unexpected.join(", ")}`,
        );
      }
    }
    return result;
  }

  #expectColumns(actual: readonly string[], expected: readonly string[]): void {
    if (
      actual.length !== expected.length ||
      actual.some((column, index) => column !== expected[index])
    ) {
      this.#fail(
        `query result columns differ: observed ${JSON.stringify(actual)}; expected ${JSON.stringify(expected)}`,
      );
    }
  }

  async #step(interaction: Interaction): Promise<void> {
    switch (interaction.kind) {
      case "createTable":
        return this.#createTable(interaction);
      case "insert":
        return this.#insert(interaction);
      case "update":
        return this.#update(interaction);
      case "delete":
        return this.#delete(interaction);
      case "rekey":
        return this.#rekey(interaction);
      case "select":
        return this.#select(interaction);
      case "partition":
        return this.#partition(interaction);
      case "unionAll":
        return this.#unionAll(interaction);
      case "createIndex":
        return this.#createIndex(interaction);
      case "dropTable":
        return this.#dropTable(interaction);
      case "transaction":
        return this.#transaction(interaction);
      case "queuedWrite":
        return this.#queuedWrite(interaction);
      case "concurrent":
        return this.#concurrent(interaction);
      case "fault":
        return this.#fault(interaction);
      case "reopen":
        return this.#reopen(interaction.connection);
      case "maintenance":
        return this.#maintenance(interaction);
      case "checkpoint":
        return this.#checkpoint();
      case "dropIndex":
        return this.#dropIndex(interaction);
      case "alterTable":
        return this.#alterTable(interaction);
      case "upsertUnique":
        return this.#upsertUnique(interaction);
      case "lookup":
        return this.#lookup(interaction);
      case "match":
        return this.#match(interaction);
      case "burst":
        return this.#burst(interaction);
    }
  }

  /**
   * Judges a statement the model refused: it must fail with one of the refusal's identities,
   * and nothing of it may land. Returns the outcome so callers can add their own checks.
   */
  #expectRefusal(
    refusal: NonNullable<Refusal>,
    outcome: Outcome<unknown>,
    what: string,
    context = "",
  ): void {
    if (outcome.error === undefined) {
      this.#fail(
        `${what} was accepted, but the model refuses it (${refusal.join(" or ")})${context}`,
      );
    }
    if (!matchesRefusal(outcome.error, refusal)) {
      this.#fail(
        `${what} failed with the wrong error, expected ${refusal.join(" or ")}: ${describeError(outcome.error)}`,
        outcome.error,
      );
    }
    this.#expectedFailures++;
  }

  async #createTable(interaction: Extract<Interaction, { kind: "createTable" }>): Promise<void> {
    const sql = renderCreateTable(interaction.table);
    const exists = this.#model.tables.has(interaction.table.name);
    if (interaction.expectExisting !== exists) {
      this.#fail(`Plan expects table ${interaction.table.name} ${exists ? "absent" : "present"}`);
    }
    const outcome = await this.#attempt(interaction.connection, sql);
    if (exists) {
      if (outcome.error === undefined)
        this.#fail("double-create-failure: creating an existing table succeeded");
      if (!isTableAlreadyExists(outcome.error, interaction.table.name)) {
        this.#fail(
          `creating an existing table failed with the wrong error: ${describeError(outcome.error)}`,
          outcome.error,
        );
      }
      this.#expectedFailures++;
      return;
    }
    if (outcome.error !== undefined)
      this.#fail(`CREATE TABLE failed: ${describeError(outcome.error)}`, outcome.error);
    this.#model.createTable(interaction.table);
  }

  async #insert(interaction: Extract<Interaction, { kind: "insert" }>): Promise<void> {
    const columns = this.#model.columns(interaction.table);
    const rendered = renderInsert(
      interaction.table,
      columns,
      interaction.rows,
      interaction.viaParameters,
    );
    const refusal = this.#model.insert(interaction.table, interaction.rows);
    const outcome = await this.#attempt(interaction.connection, rendered.sql, rendered.params);
    if (refusal !== undefined) {
      this.#expectRefusal(
        refusal,
        outcome,
        refusal.includes("unique") ? "duplicate insert" : "refused insert",
        ` (insert-select)`,
      );
      // Statement atomicity: none of the rows may have landed, including the non-duplicates.
      await this.#expectRows(
        interaction.connection,
        interaction.table,
        this.#model.sorted(interaction.table),
        "after a rejected insert",
      );
      return;
    }
    if (outcome.error !== undefined)
      this.#fail(`INSERT failed: ${describeError(outcome.error)}`, outcome.error);
    if (outcome.result?.rowCount !== interaction.rows.length) {
      this.#fail(
        `INSERT reported ${String(outcome.result?.rowCount)} rows, expected ${String(interaction.rows.length)}`,
      );
    }
    this.#acceptedWrites++;
    const ids = interaction.rows.map((row) => String(idOf(row))).join(", ");
    const predicate = `id IN (${ids})`;
    const expected = this.#model
      .sorted(interaction.table)
      .filter((row) => interaction.rows.some((inserted) => idOf(inserted) === idOf(row)));
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      expected,
      "insert-select",
      predicate,
    );
  }

  async #update(interaction: Extract<Interaction, { kind: "update" }>): Promise<void> {
    const before = this.#model.sorted(interaction.table);
    const { count: expected, refusal } = this.#model.update(
      interaction.table,
      interaction.predicate,
      interaction.assignments,
    );
    const sql = `UPDATE ${quote(interaction.table)} SET ${renderAssignments(interaction.assignments)} WHERE ${renderPredicate(interaction.predicate)}`;
    if (refusal !== undefined) {
      this.#expectRefusal(
        refusal,
        await this.#attempt(interaction.connection, sql),
        "a refused UPDATE",
        `\n  model rows before: ${canonicalRows(before, this.#model.columns(interaction.table))}`,
      );
      await this.#expectRows(
        interaction.connection,
        interaction.table,
        this.#model.sorted(interaction.table),
        "after a refused UPDATE",
      );
      return;
    }
    const result = await this.#execute(interaction.connection, sql).catch((error: unknown) =>
      this.#fail(`UPDATE failed: ${describeError(error)}`, error),
    );
    if (result.rowCount !== expected) {
      this.#fail(
        `update-count: UPDATE reported ${String(result.rowCount)} rows, model matched ${String(expected)}\n  model rows before: ${canonicalRows(before, this.#model.columns(interaction.table))}`,
      );
    }
    this.#acceptedWrites++;
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      this.#model.sorted(interaction.table),
      "after UPDATE",
    );
  }

  async #delete(interaction: Extract<Interaction, { kind: "delete" }>): Promise<void> {
    const before = this.#model.sorted(interaction.table);
    const { count: expected, refusal } = this.#model.delete(
      interaction.table,
      interaction.predicate,
    );
    const where = renderPredicate(interaction.predicate);
    const sql = `DELETE FROM ${quote(interaction.table)} WHERE ${where}`;
    if (refusal !== undefined) {
      this.#expectRefusal(
        refusal,
        await this.#attempt(interaction.connection, sql),
        "a refused DELETE",
        `\n  model rows before: ${canonicalRows(before, this.#model.columns(interaction.table))}`,
      );
      await this.#expectRows(
        interaction.connection,
        interaction.table,
        this.#model.sorted(interaction.table),
        "after a refused DELETE",
      );
      return;
    }
    const result = await this.#execute(interaction.connection, sql).catch((error: unknown) =>
      this.#fail(`DELETE failed: ${describeError(error)}`, error),
    );
    if (result.rowCount !== expected) {
      this.#fail(
        `delete-count: DELETE reported ${String(result.rowCount)} rows, model matched ${String(expected)}\n  model rows before: ${canonicalRows(before, this.#model.columns(interaction.table))}`,
      );
    }
    this.#acceptedWrites++;
    const survivors = await this.#count(interaction.connection, interaction.table, where);
    if (survivors !== 0)
      this.#fail(`delete-select: ${String(survivors)} rows still match ${where}`);
  }

  /**
   * rekey-forward: a rekey the model accepts moves exactly the matched rows, every reference
   * follows, and each retired key then reads the moved row on another connection -- through
   * the stored forwards, not a notification. One the model refuses (a new key that is taken,
   * repeated, or retired) fails as a duplicate and changes nothing.
   */
  async #rekey(interaction: Extract<Interaction, { kind: "rekey" }>): Promise<void> {
    const before = this.#model.clone();
    const outcome = this.#model.rekey(
      interaction.table,
      interaction.predicate,
      interaction.key,
      interaction.assignments,
    );
    const sql = renderRekey(interaction.table, interaction);
    const result = await this.#attempt(interaction.connection, sql);
    if (!outcome.accepted) {
      this.#model = before;
      this.#expectRefusal(
        outcome.refusal ?? ["unique"],
        result,
        "rekey-forward: a rekey onto a taken, repeated, or retired key",
        `\n  model rows before: ${canonicalRows(before.sorted(interaction.table), before.columns(interaction.table))}`,
      );
    } else {
      if (result.error !== undefined) {
        this.#fail(`rekey failed: ${describeError(result.error)}`, result.error);
      }
      if (result.result?.rowCount !== outcome.matched) {
        this.#fail(
          `update-count: a rekey reported ${String(result.result?.rowCount)} rows, model matched ${String(outcome.matched)}\n  model rows before: ${canonicalRows(before.sorted(interaction.table), before.columns(interaction.table))}`,
        );
      }
      this.#acceptedWrites++;
    }
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      this.#model.sorted(interaction.table),
      outcome.accepted ? "after a rekey" : "after a refused rekey",
    );
    const reader = (interaction.connection + 1) % this.#plan.connections;
    for (const [from] of outcome.retired.slice(0, 3)) {
      const target = this.#model.resolve(interaction.table, from);
      const row = this.#model.table(interaction.table).rows.get(target);
      await this.#expectRows(
        reader,
        interaction.table,
        row === undefined ? [] : [row],
        `rekey-forward: reading retired key ${String(from)}`,
        `id = ${String(from)}`,
      );
    }
  }

  async #select(interaction: Extract<Interaction, { kind: "select" }>): Promise<void> {
    const matching = this.#model.matching(interaction.table, interaction.predicate);
    if (interaction.descending) matching.reverse();
    const expected = interaction.limit === null ? matching : matching.slice(0, interaction.limit);
    const order = `ORDER BY id${interaction.descending ? " DESC" : ""}`;
    const limit = interaction.limit === null ? "" : ` LIMIT ${String(interaction.limit)}`;
    const sql = `SELECT * FROM ${quote(interaction.table)} WHERE ${renderPredicate(interaction.predicate)} ${order}${limit}`;
    const result = await this.#query(interaction.connection, sql).catch((error: unknown) =>
      this.#fail(`SELECT failed: ${describeError(error)}`, error),
    );
    this.#compareRows(
      result,
      expected,
      this.#model.columns(interaction.table),
      interaction.limit === null ? "select" : "select-limit",
    );
  }

  async #partition(interaction: Extract<Interaction, { kind: "partition" }>): Promise<void> {
    const where = renderPredicate(interaction.predicate);
    const total = await this.#count(interaction.connection, interaction.table, "TRUE");
    const positive = await this.#count(interaction.connection, interaction.table, where);
    const negative = await this.#count(interaction.connection, interaction.table, `NOT (${where})`);
    const unknown = await this.#count(
      interaction.connection,
      interaction.table,
      `(${where}) IS NULL`,
    );
    if (positive + negative + unknown !== total) {
      this.#fail(
        `where-true-false-null: ${String(positive)} + ${String(negative)} + ${String(unknown)} != ${String(total)} for ${where}`,
      );
    }
    const rows = this.#model.sorted(interaction.table);
    const modelPositive = rows.filter(
      (row) => this.#model.evaluate(interaction.table, interaction.predicate, row) === true,
    ).length;
    const modelUnknown = rows.filter(
      (row) => this.#model.evaluate(interaction.table, interaction.predicate, row) === null,
    ).length;
    if (total !== rows.length || positive !== modelPositive || unknown !== modelUnknown) {
      this.#fail(
        `where-true-false-null: engine ${String(positive)}/${String(negative)}/${String(unknown)} of ${String(total)}, model ${String(modelPositive)}/${String(rows.length - modelPositive - modelUnknown)}/${String(modelUnknown)} of ${String(rows.length)} for ${where}`,
      );
    }
  }

  async #unionAll(interaction: Extract<Interaction, { kind: "unionAll" }>): Promise<void> {
    const left = renderPredicate(interaction.left);
    const right = renderPredicate(interaction.right);
    const table = quote(interaction.table);
    const sql = `SELECT id FROM ${table} WHERE ${left} UNION ALL SELECT id FROM ${table} WHERE ${right}`;
    const result = await this.#query(interaction.connection, sql).catch((error: unknown) =>
      this.#fail(`UNION ALL failed: ${describeError(error)}`, error),
    );
    const expected =
      this.#model.matching(interaction.table, interaction.left).length +
      this.#model.matching(interaction.table, interaction.right).length;
    if (result.rows.length !== expected) {
      this.#fail(
        `union-all-cardinality: ${String(result.rows.length)} rows, expected ${String(expected)}\n  received ids: ${result.rows.map((row) => String(row.id)).join(", ")}\n  model rows: ${canonicalRows(this.#model.sorted(interaction.table), this.#model.columns(interaction.table))}`,
      );
    }
  }

  async #createIndex(interaction: Extract<Interaction, { kind: "createIndex" }>): Promise<void> {
    const unique = interaction.unique === true;
    const sql = `CREATE ${unique ? "UNIQUE " : ""}INDEX ${quote(interaction.name)} ON ${quote(interaction.table)} (${interaction.columns.map(quote).join(", ")})`;
    const refused = this.#model.createIndex(
      interaction.name,
      interaction.table,
      interaction.columns,
      unique,
    );
    const outcome = await this.#attempt(interaction.connection, sql);
    if (refused) {
      if (outcome.error === undefined) {
        this.#fail("a UNIQUE index was built over rows that hold a duplicate");
      }
      if (
        !errorChainSome(
          outcome.error,
          (name, message) =>
            name === "TypeError" &&
            message === `UNIQUE index ${interaction.name} has a duplicate key`,
        )
      ) {
        this.#fail(
          `a UNIQUE index over a duplicate failed with the wrong error: ${describeError(outcome.error)}`,
          outcome.error,
        );
      }
      this.#expectedFailures++;
    } else if (outcome.error !== undefined) {
      this.#fail(`CREATE INDEX failed: ${describeError(outcome.error)}`, outcome.error);
    }
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      this.#model.sorted(interaction.table),
      "after CREATE INDEX",
    );
  }

  async #dropTable(interaction: Extract<Interaction, { kind: "dropTable" }>): Promise<void> {
    const exists = this.#model.tables.has(interaction.table);
    if (interaction.expectMissing === exists) {
      this.#fail(`Plan expects table ${interaction.table} ${exists ? "absent" : "present"}`);
    }
    const outcome = await this.#attempt(
      interaction.connection,
      `DROP TABLE ${quote(interaction.table)}`,
    );
    if (!exists) {
      if (outcome.error === undefined) this.#fail("dropping a missing table succeeded");
      if (!isUnknownTable(outcome.error, interaction.table)) {
        this.#fail(
          `dropping a missing table failed with the wrong error: ${describeError(outcome.error)}`,
          outcome.error,
        );
      }
      this.#expectedFailures++;
      return;
    }
    const children = this.#model.children(interaction.table);
    if ((interaction.expectRefused === true) !== children.length > 0) {
      this.#fail(
        `Plan expects dropping ${interaction.table} to be ${children.length > 0 ? "accepted" : "refused"}`,
      );
    }
    if (children.length > 0) {
      if (outcome.error === undefined) {
        this.#fail(
          `DROP TABLE removed ${interaction.table} while ${children[0]?.table ?? ""} references it`,
        );
      }
      if (
        !errorChainSome(
          outcome.error,
          (name, message) =>
            name === "TypeError" &&
            message?.startsWith(`Cannot drop ${interaction.table}: foreign key `) === true,
        )
      ) {
        this.#fail(
          `dropping a referenced table failed with the wrong error: ${describeError(outcome.error)}`,
          outcome.error,
        );
      }
      this.#expectedFailures++;
      await this.#expectRows(
        interaction.connection,
        interaction.table,
        this.#model.sorted(interaction.table),
        "after a refused DROP TABLE",
      );
      return;
    }
    if (outcome.error !== undefined)
      this.#fail(`DROP TABLE failed: ${describeError(outcome.error)}`, outcome.error);
    this.#model.dropTable(interaction.table);
    const select = await this.#attemptQuery(
      interaction.connection,
      `SELECT * FROM ${quote(interaction.table)}`,
    );
    if (select.error === undefined)
      this.#fail("drop-select: a dropped table still answers queries");
    if (!isUnknownTable(select.error, interaction.table)) {
      this.#fail(
        `querying a dropped table failed with the wrong error: ${describeError(select.error)}`,
        select.error,
      );
    }
  }

  /**
   * A transaction the engine refuses for a reason it documents ends this step instead of
   * reporting a defect: nothing of it published, so the committed state is what remains.
   */
  async #transaction(interaction: Extract<Interaction, { kind: "transaction" }>): Promise<void> {
    const committed = this.#model.clone();
    try {
      await this.#runTransaction(interaction, committed);
      return;
    } catch (error) {
      if (!(error instanceof RefusedTransaction)) throw error;
      this.#model = committed;
      if (error.expired) {
        this.#expectedFailures++;
        // An expired transaction stays failed until an explicit ROLLBACK acknowledges it; a lost
        // commit race already ended the transaction, so there is nothing left to roll back.
        await this.#execute(interaction.connection, "ROLLBACK").catch((rollback: unknown) =>
          this.#fail(
            `ROLLBACK after an expired transaction failed: ${describeError(rollback)}`,
            rollback,
          ),
        );
      } else {
        this.#rejectedConflicts++;
      }
      await this.#expectRows(
        interaction.connection,
        interaction.table,
        this.#model.sorted(interaction.table),
        `after a refused transaction (${error.reason})`,
      );
    }
  }

  /**
   * writer-turns: a transaction holds the database's one writer turn, so a keyed write issued
   * on another connection meanwhile stays pending — it neither lands under the transaction nor
   * loses a commit race — and completes right after COMMIT or ROLLBACK. Reads on the waiting
   * connection do not wait. Either end leaves the queued write's own outcome exact.
   */
  async #queuedWrite(interaction: Extract<Interaction, { kind: "queuedWrite" }>): Promise<void> {
    const columns = this.#model.columns(interaction.table);
    const committed = this.#model.clone();
    try {
      await this.#execute(interaction.connection, "BEGIN");
    } catch (error) {
      this.#refuseTransaction("BEGIN", error);
    }
    let poisoned = false;
    const refused = this.#model.insert(interaction.table, interaction.rows);
    const rendered = renderInsert(interaction.table, columns, interaction.rows, true);
    const staged = await this.#attempt(interaction.connection, rendered.sql, rendered.params);
    if (refused === undefined && staged.error !== undefined) {
      this.#refuseTransaction("INSERT inside a transaction", staged.error);
    }
    if (refused !== undefined) {
      this.#expectRefusal(refused, staged, "a refused insert inside a transaction");
      poisoned = true;
    }
    const sql = renderKeyed(interaction.table, columns, interaction.mutation);
    const queued = this.#attempt(interaction.waiter, sql);
    // The waiter's write must still be pending while the transaction holds the turn: give it
    // real time to land wrongly before checking, then read through the waiter (reads never
    // wait) to prove the transaction's rows are invisible there.
    const settledEarly = await Promise.race([
      queued.then(() => true),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 50)),
    ]);
    if (settledEarly) {
      this.#fail(
        "writer-turns: a write on another connection landed while a transaction held the turn",
      );
    }
    await this.#expectRows(
      interaction.waiter,
      interaction.table,
      committed.sorted(interaction.table),
      "on the waiting connection while a transaction holds the turn",
    );
    const end = poisoned ? "ROLLBACK" : interaction.outcome === "commit" ? "COMMIT" : "ROLLBACK";
    try {
      await this.#execute(interaction.connection, end);
    } catch (error) {
      this.#refuseTransaction(end, error);
    }
    if (end === "ROLLBACK") this.#model = committed;
    else this.#acceptedWrites++;
    const outcome = await queued;
    const refusal = this.#model.applyKeyed(interaction.table, interaction.mutation);
    if (outcome.error === undefined) {
      if (refusal !== undefined) {
        this.#fail("writer-turns: a queued write the model refuses was accepted");
      }
      this.#acceptedWrites++;
    } else if (refusal !== undefined && matchesRefusal(outcome.error, refusal)) {
      this.#expectedFailures++;
    } else if (refusal === undefined && isUniqueViolation(outcome.error)) {
      this.#fail(
        `writer-turns: a queued write was refused as a duplicate: ${describeError(outcome.error)}`,
      );
    } else {
      this.#fail(
        `writer-turns: a queued write failed after the transaction ended: ${describeError(outcome.error)}`,
        outcome.error,
      );
    }
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      this.#model.sorted(interaction.table),
      `after ${end} and the queued write`,
    );
  }

  /**
   * Inside a SQL transaction two refusals are documented engine behaviour rather than a defect:
   * another connection publishing a data commit while this transaction has writes staged, which
   * loses the commit race, and a transaction left idle past `transactionIdleTimeoutMs`, which
   * rolls itself back. Both discard the whole transaction. Anything else is a defect.
   */
  #refuseTransaction(step: string, error: unknown): never {
    if (isExpiredTransaction(error)) throw new RefusedTransaction("idle rollback", error);
    if (isConflict(error)) throw new RefusedTransaction("lost commit race", error);
    this.#fail(`${step} failed: ${describeError(error)}`, error);
  }

  /** A read inside a transaction meets the same two refusals its writes do. */
  async #expectRowsInTransaction(
    connection: number,
    table: string,
    expected: readonly PlanRow[],
    context: string,
  ): Promise<void> {
    try {
      await this.#expectRows(connection, table, expected, context);
    } catch (error) {
      if (isExpiredTransaction(error) || isConflict(error)) this.#refuseTransaction(context, error);
      throw error;
    }
  }

  async #runTransaction(
    interaction: Extract<Interaction, { kind: "transaction" }>,
    committed: ShadowModel,
  ): Promise<void> {
    const columns = this.#model.columns(interaction.table);
    await this.#execute(interaction.connection, "BEGIN").catch((error: unknown) =>
      this.#refuseTransaction("BEGIN", error),
    );
    let poisoned = false;
    for (const statement of interaction.statements) {
      // Each statement is judged against the model as it runs: one the model refuses must fail
      // with its identity, and leaves the transaction unusable -- the engine must refuse further
      // statements and COMMIT must roll back, like PostgreSQL.
      let refusal: Refusal;
      let label: string;
      let refused: string;
      let sql: string;
      let params: PlanValue[] | undefined;
      switch (statement.kind) {
        case "insert": {
          refusal = this.#model.insert(interaction.table, statement.rows);
          ({ sql, params } = renderInsert(interaction.table, columns, statement.rows, true));
          label = "INSERT";
          refused = `${refusal?.includes("unique") === true ? "duplicate" : "refused"} insert`;
          break;
        }
        case "rekey": {
          const outcome = this.#model.rekey(
            interaction.table,
            statement.predicate,
            statement.key,
            statement.assignments,
          );
          refusal = outcome.accepted ? undefined : (outcome.refusal ?? ["unique"]);
          sql = renderRekey(interaction.table, statement);
          label = "a rekey";
          refused = "a rekey onto a taken, repeated, or retired key";
          break;
        }
        case "update":
          refusal = this.#model.update(
            interaction.table,
            statement.predicate,
            statement.assignments,
          ).refusal;
          sql = `UPDATE ${quote(interaction.table)} SET ${renderAssignments(statement.assignments)} WHERE ${renderPredicate(statement.predicate)}`;
          label = "UPDATE";
          refused = "a refused UPDATE";
          break;
        case "delete":
          refusal = this.#model.delete(interaction.table, statement.predicate).refusal;
          sql = `DELETE FROM ${quote(interaction.table)} WHERE ${renderPredicate(statement.predicate)}`;
          label = "DELETE";
          refused = "a refused DELETE";
          break;
      }
      const outcome = await this.#attempt(interaction.connection, sql, params);
      if (refusal === undefined) {
        if (outcome.error !== undefined) {
          this.#refuseTransaction(`${label} inside a transaction`, outcome.error);
        }
        continue;
      }
      this.#expectRefusal(refusal, outcome, `${refused} inside a transaction`);
      poisoned = true;
      break;
    }
    if (!poisoned) {
      // transaction-isolation: the owner sees its own pending rows; an observer sees none.
      await this.#expectRowsInTransaction(
        interaction.connection,
        interaction.table,
        this.#model.sorted(interaction.table),
        "inside an open transaction (owner)",
      );
      if (interaction.observer !== interaction.connection) {
        await this.#expectRows(
          interaction.observer,
          interaction.table,
          committed.sorted(interaction.table),
          "inside an open transaction (observer)",
        );
      }
    }
    const end = poisoned ? "ROLLBACK" : interaction.outcome === "commit" ? "COMMIT" : "ROLLBACK";
    await this.#execute(interaction.connection, end).catch((error: unknown) =>
      this.#refuseTransaction(end, error),
    );
    if (end === "ROLLBACK") this.#model = committed;
    else this.#acceptedWrites++;
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      this.#model.sorted(interaction.table),
      `after ${end}`,
    );
    if (interaction.observer !== interaction.connection) {
      await this.#expectRows(
        interaction.observer,
        interaction.table,
        this.#model.sorted(interaction.table),
        `after ${end} (observer)`,
      );
    }
  }

  async #concurrent(interaction: Extract<Interaction, { kind: "concurrent" }>): Promise<void> {
    const columns = this.#model.columns(interaction.table);
    const before = this.#model.clone();
    // The plan's keys are distinct, but after a rekey two of them can name one row. Writes to
    // one row do not commute, so such a round runs its writes one at a time, in plan order.
    const targets = interaction.operations.map((operation) =>
      before.keyedTarget(interaction.table, operation.mutation),
    );
    const serial = new Set(targets).size !== targets.length;
    // Writes to distinct rows can still contend for one unique value. They run concurrently
    // all the same, and are judged by the serial order that explains their outcomes.
    const contended =
      !serial && !uniqueValuesCommute(before, interaction.table, interaction.operations, targets);
    const run = async (operation: (typeof interaction.operations)[number]) => {
      const sql = renderKeyed(interaction.table, columns, operation.mutation);
      const outcome = await this.#attempt(operation.connection, sql);
      return { operation, outcome };
    };
    const writes: Promise<Array<Awaited<ReturnType<typeof run>>>> = serial
      ? (async () => {
          const settled: Array<Awaited<ReturnType<typeof run>>> = [];
          for (const operation of interaction.operations) settled.push(await run(operation));
          return settled;
        })()
      : Promise.all(interaction.operations.map(run));
    const read = async (reader: number) => ({
      reader,
      outcome: await this.#attemptQuery(
        reader,
        `SELECT * FROM ${quote(interaction.table)} ORDER BY id`,
      ),
    });
    const [writeOutcomes, readOutcomes] = serial
      ? [await writes, await Promise.all(interaction.readers.map(read))]
      : await Promise.all([writes, Promise.all(interaction.readers.map(read))]);
    const touched = new Set<number>(targets);
    if (contended) this.#explainContendedRound(interaction.table, before, writeOutcomes);
    for (const [index, { operation, outcome }] of contended ? [] : writeOutcomes.entries()) {
      const id = keyOf(operation.mutation);
      touched.add(targets[index] ?? id);
      // A serial round's writes each see the ones before it.
      const refusal = (serial ? this.#model : before)
        .clone()
        .applyKeyed(interaction.table, operation.mutation);
      if (outcome.error === undefined) {
        if (refusal !== undefined) {
          this.#fail(
            `concurrent write on key ${String(id)} was accepted, but the model refuses it (${refusal.join(" or ")})`,
          );
        }
        if (this.#model.applyKeyed(interaction.table, operation.mutation) !== undefined) {
          this.#fail(`concurrent writes on key ${String(id)} did not commute in the model`);
        }
        this.#acceptedWrites++;
      } else if (refusal !== undefined && matchesRefusal(outcome.error, refusal)) {
        this.#expectedFailures++;
      } else if (refusal === undefined && isUniqueViolation(outcome.error)) {
        this.#fail(
          `concurrent write on key ${String(id)} was refused as a duplicate: ${describeError(outcome.error)}`,
        );
      } else if (isConflict(outcome.error)) {
        this.#rejectedConflicts++;
      } else {
        this.#fail(
          `concurrent write on key ${String(id)} failed unexpectedly: ${describeError(outcome.error)}`,
        );
      }
    }
    // concurrent-explicability: each in-flight read is some prefix-consistent state.
    for (const { reader, outcome } of readOutcomes) {
      if (outcome.error !== undefined)
        this.#fail(
          `a read during concurrent writes failed on connection ${String(reader)}: ${describeError(outcome.error)}`,
        );
      const seen = new Map<number, string>();
      for (const row of outcome.result?.rows ?? []) {
        const id = row.id;
        if (typeof id !== "number")
          this.#fail("a concurrent read returned a row without a numeric id");
        seen.set(id, canonical(row, columns));
      }
      const beforeRows = new Map(
        before.sorted(interaction.table).map((row) => [idOf(row), canonical(row, columns)]),
      );
      const afterRows = new Map(
        this.#model.sorted(interaction.table).map((row) => [idOf(row), canonical(row, columns)]),
      );
      const ids = new Set([...beforeRows.keys(), ...afterRows.keys(), ...seen.keys()]);
      for (const id of ids) {
        const observed = seen.get(id);
        if (touched.has(id)) {
          const allowed = [beforeRows.get(id), afterRows.get(id)];
          if (!allowed.includes(observed)) {
            this.#fail(
              `concurrent-explicability: reader ${String(reader)} saw key ${String(id)} as ${observed ?? "absent"}, neither before (${beforeRows.get(id) ?? "absent"}) nor after (${afterRows.get(id) ?? "absent"})`,
            );
          }
        } else if (observed !== beforeRows.get(id)) {
          this.#fail(
            `concurrent-explicability: reader ${String(reader)} saw untouched key ${String(id)} as ${observed ?? "absent"}, expected ${beforeRows.get(id) ?? "absent"}`,
          );
        }
      }
    }
    for (let connection = 0; connection < this.#plan.connections; connection++) {
      await this.#expectRows(
        connection,
        interaction.table,
        this.#model.sorted(interaction.table),
        "after a concurrent round",
      );
    }
  }

  /**
   * unique-race: writes to distinct rows that contend for one unique value ran concurrently.
   * Their outcomes must be those of some serial order -- of two writes claiming one free value,
   * exactly one lands -- and the model becomes the state that order leaves. Lost commit races
   * are counted, and any other error is a defect.
   */
  #explainContendedRound(
    table: string,
    before: ShadowModel,
    settled: ReadonlyArray<{
      readonly operation: { readonly mutation: KeyedMutation };
      readonly outcome: Outcome<SimulatedExecuteResult>;
    }>,
  ): void {
    const pending: number[] = [];
    for (const [index, { outcome }] of settled.entries()) {
      if (outcome.error === undefined) pending.push(index);
      else if (isConflict(outcome.error)) this.#rejectedConflicts++;
      else if (matchesRefusal(outcome.error, ["unique", "foreignKey", "restrict"])) {
        pending.push(index);
      } else {
        this.#fail(
          `a contended concurrent write failed unexpectedly: ${describeError(outcome.error)}`,
        );
      }
    }
    let budget = 20_000;
    const search = (model: ShadowModel, remaining: readonly number[]): ShadowModel | undefined => {
      if (remaining.length === 0) return model;
      for (const index of remaining) {
        if (--budget < 0) return undefined;
        const entry = settled[index];
        if (entry === undefined) continue;
        const next = model.clone();
        const refusal = next.applyKeyed(table, entry.operation.mutation);
        const error = entry.outcome.error;
        const explained =
          error === undefined
            ? refusal === undefined
            : refusal !== undefined && matchesRefusal(error, refusal);
        if (!explained) continue;
        const found = search(
          next,
          remaining.filter((candidate) => candidate !== index),
        );
        if (found !== undefined) return found;
      }
      return undefined;
    };
    const explained = search(before.clone(), pending);
    if (explained === undefined) {
      this.#fail(
        `unique-race: no serial order of the round's writes explains their outcomes\n  ${settled
          .map(
            ({ operation, outcome }) =>
              `${JSON.stringify(operation.mutation)} -> ${outcome.error === undefined ? "accepted" : describeError(outcome.error)}`,
          )
          .join("\n  ")}`,
      );
    }
    for (const index of pending) {
      if (settled[index]?.outcome.error === undefined) this.#acceptedWrites++;
      else this.#expectedFailures++;
    }
    this.#model = explained;
  }

  async #fault(interaction: Extract<Interaction, { kind: "fault" }>): Promise<void> {
    const connection = this.#connection(interaction.connection);
    const columns = this.#model.columns(interaction.table);
    const sql = renderKeyed(interaction.table, columns, interaction.mutation);
    const before = this.#model.clone();
    const after = this.#model.clone();
    const refusal = after.applyKeyed(interaction.table, interaction.mutation);
    if (refusal !== undefined) {
      // A refused write is refused before any storage write; nothing to interrupt.
      this.#expectRefusal(
        refusal,
        await this.#attempt(interaction.connection, sql),
        `${refusal.includes("unique") ? "duplicate" : "refused"} ${interaction.mutation.kind} during a fault step`,
      );
      return;
    }
    let outcome: Outcome<SimulatedExecuteResult>;
    if (interaction.point === "crash") {
      if (connection.crash === undefined) {
        this.#faultsSkipped++;
        return;
      }
      const pending = this.#attempt(interaction.connection, sql);
      await connection.crash();
      outcome = await pending;
      if (outcome.error === undefined) {
        // The crash landed after the reply; a durable success is the only acceptable reading.
        this.#model = after;
      }
    } else {
      const faults = this.#driver.faults;
      if (faults === undefined) {
        this.#faultsSkipped++;
        return;
      }
      faults.arm(interaction.point, 1);
      try {
        outcome = await this.#attempt(interaction.connection, sql);
      } finally {
        faults.disarm();
      }
      if (!faults.fired()) {
        // The store never reached the point (an update of a missing key writes nothing, say);
        // the mutation ran normally and is judged as such.
        if (outcome.error !== undefined)
          this.#fail(
            `mutation failed without its fault firing: ${describeError(outcome.error)}`,
            outcome.error,
          );
        this.#model = after;
        this.#acceptedWrites++;
        return;
      }
    }
    this.#faultsInjected++;
    // A fault after the commit point, or a crash after the reply, legitimately reports success:
    // the only requirement is then durability, checked below. A reported failure must have
    // left the table exactly as it was or exactly as the mutation would have made it.
    const expectedError =
      outcome.error === undefined
        ? true
        : interaction.point === "crash"
          ? isExpectedCrashOutcome(outcome.error)
          : isInjectedFault(outcome.error, interaction.point);
    if (!expectedError) {
      this.#fail(
        `fault ${interaction.point} surfaced an unexpected error: ${describeError(outcome.error)}`,
      );
    }
    // Reopen before judging: the model is settled only once the durable state has been read.
    let actual: Array<Readonly<Record<string, unknown>>>;
    try {
      await this.#reopenConnection(interaction.connection);
      actual = await this.#read(interaction.connection, interaction.table);
    } catch (error) {
      // Only crash recovery may meet the browser wedge documented by `StorageUnresponsiveError`.
      // Ordinary steps and the crashed mutation's own outcome still have to report their exact
      // contract; accepting this name globally would let an unrelated engine failure stop a run
      // successfully before the remaining interactions were judged.
      const storageError = errorInChainWithName(error, "StorageUnresponsiveError");
      if (interaction.point === "crash" && storageError !== undefined) {
        throw new AcceptedStoreUnresponsive(storageError);
      }
      throw error;
    }
    const beforeRows = canonicalRows(before.sorted(interaction.table), columns);
    const afterRows = canonicalRows(after.sorted(interaction.table), columns);
    const observed = canonicalRows(actual, columns);
    if (observed === afterRows) this.#model = after;
    else if (observed === beforeRows) {
      if (outcome.error === undefined)
        this.#fail(
          `fault-atomicity: ${interaction.point} reported success but the mutation is not durable`,
        );
      this.#model = before;
    } else {
      this.#fail(
        `fault-atomicity: after ${interaction.point} the table is neither before nor after the mutation\n  observed ${observed}\n  before   ${beforeRows}\n  after    ${afterRows}`,
      );
    }
  }

  /**
   * schema-change: an `ALTER TABLE` the model accepts lands for every connection -- the rows
   * read back under the new columns, a dropped column gone, an added one at its default -- and
   * one it refuses (a column an index or foreign key still uses) changes nothing.
   */
  async #alterTable(interaction: Extract<Interaction, { kind: "alterTable" }>): Promise<void> {
    const { change } = interaction;
    const refused = this.#model.alter(interaction.table, change);
    if (refused !== interaction.expectRefused) {
      this.#fail(
        `Plan expects ALTER TABLE ${change.kind} on ${interaction.table} to be ${interaction.expectRefused ? "refused" : "accepted"}`,
      );
    }
    const outcome = await this.#attempt(
      interaction.connection,
      renderAlter(interaction.table, change),
    );
    if (refused) {
      if (outcome.error === undefined) {
        this.#fail(`schema-change: ALTER TABLE ${change.kind} of a column in use was accepted`);
      }
      const column = change.kind === "addColumn" ? change.column.name : change.column;
      const expected = (name: string | undefined, message: string | undefined): boolean =>
        name === "TypeError" &&
        (change.kind === "dropColumn"
          ? message?.endsWith(` still uses this column: ${interaction.table}.${column}`) === true
          : message?.startsWith(`Cannot rename ${interaction.table}.${column}: `) === true);
      if (!errorChainSome(outcome.error, expected)) {
        this.#fail(
          `a refused ALTER TABLE ${change.kind} failed with the wrong error: ${describeError(outcome.error)}`,
          outcome.error,
        );
      }
      this.#expectedFailures++;
    } else if (outcome.error !== undefined) {
      this.#fail(
        `ALTER TABLE ${change.kind} failed: ${describeError(outcome.error)}`,
        outcome.error,
      );
    }
    const context = refused ? "after a refused ALTER TABLE" : "after ALTER TABLE";
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      this.#model.sorted(interaction.table),
      context,
    );
    await this.#expectRows(
      (interaction.connection + 1) % this.#plan.connections,
      interaction.table,
      this.#model.sorted(interaction.table),
      `${context} (another connection)`,
    );
  }

  async #dropIndex(interaction: Extract<Interaction, { kind: "dropIndex" }>): Promise<void> {
    if (!interaction.ifExists && !this.#model.indexes.has(interaction.name)) {
      this.#fail(`Plan drops index ${interaction.name}, which the model does not hold`);
    }
    const sql = `DROP INDEX ${interaction.ifExists ? "IF EXISTS " : ""}${quote(interaction.name)}`;
    await this.#execute(interaction.connection, sql).catch((error: unknown) =>
      this.#fail(`DROP INDEX failed: ${describeError(error)}`, error),
    );
    this.#model.indexes.delete(interaction.name);
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      this.#model.sorted(interaction.table),
      "after DROP INDEX",
    );
  }

  /**
   * upsert-arbiter: a proposal colliding on its conflict target updates the row holding the
   * value, under that row's own key; one colliding on anything else is refused; the reported
   * count is the rows written.
   */
  async #upsertUnique(interaction: Extract<Interaction, { kind: "upsertUnique" }>): Promise<void> {
    const columns = this.#model.columns(interaction.table);
    const before = this.#model.sorted(interaction.table);
    const sql = renderUpsertUnique(interaction, columns);
    const { count, refusal } = this.#model.upsertUnique(
      interaction.table,
      interaction.row,
      interaction.target,
      interaction.assignments,
    );
    const outcome = await this.#attempt(interaction.connection, sql);
    const rowsBefore = `\n  model rows before: ${canonicalRows(before, columns)}`;
    if (refusal !== undefined) {
      this.#expectRefusal(refusal, outcome, "upsert-arbiter: a refused upsert", rowsBefore);
    } else {
      if (outcome.error !== undefined) {
        this.#fail(`upsert failed: ${describeError(outcome.error)}${rowsBefore}`, outcome.error);
      }
      if (outcome.result?.rowCount !== count) {
        this.#fail(
          `upsert-arbiter: the upsert reported ${String(outcome.result?.rowCount)} rows, the model wrote ${String(count)}${rowsBefore}`,
        );
      }
      this.#acceptedWrites++;
    }
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      this.#model.sorted(interaction.table),
      refusal === undefined ? "after an upsert" : "after a refused upsert",
    );
  }

  /** index-lookup: a select shaped for the secondary-index path returns the model's rows. */
  async #lookup(interaction: Extract<Interaction, { kind: "lookup" }>): Promise<void> {
    const columns = this.#model.columns(interaction.table);
    const { orderBy, descending } = interaction;
    const matching = this.#model.matching(interaction.table, interaction.predicate);
    if (orderBy !== null) {
      matching.sort((left, right) => {
        const leftValue = left[orderBy] ?? null;
        const rightValue = right[orderBy] ?? null;
        // NULL sorts after every value ascending, as PostgreSQL orders it.
        const order =
          leftValue === null || rightValue === null
            ? Number(leftValue === null) - Number(rightValue === null)
            : compareValues(leftValue, rightValue);
        return order !== 0 ? order : idOf(left) - idOf(right);
      });
    }
    if (descending) matching.reverse();
    const expected = interaction.limit === null ? matching : matching.slice(0, interaction.limit);
    const direction = descending ? " DESC" : "";
    const order =
      orderBy === null
        ? `ORDER BY id${direction}`
        : `ORDER BY ${quote(orderBy)}${direction}, id${direction}`;
    const limit = interaction.limit === null ? "" : ` LIMIT ${String(interaction.limit)}`;
    const sql = `SELECT * FROM ${quote(interaction.table)} WHERE ${renderPredicate(interaction.predicate)} ${order}${limit}`;
    const result = await this.#query(interaction.connection, sql).catch((error: unknown) =>
      this.#fail(`SELECT failed: ${describeError(error)}`, error),
    );
    this.#compareRows(result, expected, columns, "index-lookup");
    const connection = this.#connection(interaction.connection);
    if (connection.explain !== undefined) {
      const plan = await connection
        .explain(sql)
        .catch((error: unknown) => this.#fail(`EXPLAIN failed: ${describeError(error)}`, error));
      if (plan.includes("secondary index")) this.#indexPathQueries++;
    }
  }

  /** full-text: `MATCH ... AGAINST` returns exactly the rows holding every query term. */
  async #match(interaction: Extract<Interaction, { kind: "match" }>): Promise<void> {
    const columns = this.#model.columns(interaction.table);
    const expected = this.#model
      .sorted(interaction.table)
      .filter(
        (row) =>
          matchesText(row, interaction.columns, interaction.query) &&
          (interaction.and === null ||
            this.#model.evaluate(interaction.table, interaction.and, row) === true),
      );
    const and = interaction.and === null ? "" : ` AND ${renderPredicate(interaction.and)}`;
    const sql = `SELECT * FROM ${quote(interaction.table)} WHERE MATCH(${interaction.columns.map(quote).join(", ")}) AGAINST ${renderLiteral(interaction.query)}${and} ORDER BY id`;
    const result = await this.#query(interaction.connection, sql).catch((error: unknown) =>
      this.#fail(`MATCH failed: ${describeError(error)}`, error),
    );
    this.#compareRows(result, expected, columns, "full-text");
  }

  /**
   * fold-progress: a long run of single-row commits from every connection, each judged as it
   * lands, keeps the table's visible segments bounded -- automatic folds absorb what the
   * commits add -- and leaves every connection reading the model's rows.
   */
  async #burst(interaction: Extract<Interaction, { kind: "burst" }>): Promise<void> {
    const random = createRandom(interaction.seed);
    const { table } = interaction;
    for (let index = 0; index < interaction.commits; index++) {
      const connection = random.int(this.#plan.connections);
      const mutation = generateKeyedMutation(
        random,
        this.#model.table(table).definition,
        1 + random.int(interaction.keySpace),
      );
      const sql = renderKeyed(table, this.#model.columns(table), mutation);
      const refusal = this.#model.applyKeyed(table, mutation);
      const outcome = await this.#attempt(connection, sql);
      this.#burstCommits++;
      if (refusal !== undefined) {
        this.#expectRefusal(refusal, outcome, `a refused ${mutation.kind} in a burst`);
      } else if (outcome.error !== undefined) {
        this.#fail(
          `a ${mutation.kind} in a burst failed after ${String(index)} commits: ${describeError(outcome.error)}`,
          outcome.error,
        );
      } else {
        this.#acceptedWrites++;
      }
      if (index % 128 === 127) {
        const key = 1 + random.int(interaction.keySpace);
        const row = this.#model.table(table).rows.get(this.#model.resolve(table, key));
        await this.#expectRows(
          random.int(this.#plan.connections),
          table,
          row === undefined ? [] : [row],
          `in a burst after ${String(index + 1)} commits, reading key ${String(key)}`,
          `id = ${String(key)}`,
        );
      }
    }
    for (let connection = 0; connection < this.#plan.connections; connection++) {
      await this.#expectRows(connection, table, this.#model.sorted(table), "after a burst");
    }
    const inspector = this.#connection(0);
    if (inspector.visibleSegments === undefined) return;
    const segments = await inspector
      .visibleSegments(table)
      .catch((error: unknown) =>
        this.#fail(`listing visible segments failed: ${describeError(error)}`, error),
      );
    this.#peakBurstSegments = Math.max(this.#peakBurstSegments, segments);
    if (interaction.commits >= BURST_SEGMENT_CEILING && segments > BURST_SEGMENT_CEILING) {
      this.#fail(
        `fold-progress: ${table} holds ${String(segments)} visible segments after ${String(interaction.commits)} single-row commits`,
      );
    }
  }

  async #reopenConnection(index: number): Promise<void> {
    this.#record(`[${String(index)}] -- reopen`);
    await this.#connection(index).reopen();
    this.#reopens++;
  }

  async #reopen(index: number): Promise<void> {
    await this.#reopenConnection(index);
    for (const table of this.#model.tables.keys()) {
      await this.#expectRows(index, table, this.#model.sorted(table), "after reopen");
    }
  }

  async #maintenance(interaction: Extract<Interaction, { kind: "maintenance" }>): Promise<void> {
    const connection = this.#connection(interaction.connection);
    if (connection.maintain === undefined) return;
    this.#record(`[${String(interaction.connection)}] -- maintenance ${interaction.table}`);
    await connection
      .maintain(interaction.table)
      .catch((error: unknown) => this.#fail(`maintenance failed: ${describeError(error)}`, error));
    await this.#expectRows(
      interaction.connection,
      interaction.table,
      this.#model.sorted(interaction.table),
      "after maintenance",
    );
  }

  async #checkpoint(): Promise<void> {
    this.#checkpoints++;
    for (let connection = 0; connection < this.#plan.connections; connection++) {
      for (const table of this.#model.tables.keys()) {
        await this.#expectRows(connection, table, this.#model.sorted(table), "at checkpoint");
      }
    }
  }

  async #count(connection: number, table: string, where: string): Promise<number> {
    const result = await this.#query(
      connection,
      `SELECT COUNT(*) AS n FROM ${quote(table)} WHERE ${where}`,
    ).catch((error: unknown) => this.#fail(`COUNT failed: ${describeError(error)}`, error));
    const value = result.rows[0]?.n;
    if (typeof value === "number") return value;
    if (typeof value === "bigint") return Number(value);
    this.#fail(`COUNT(*) returned ${JSON.stringify(value)}`);
  }

  async #read(
    connection: number,
    table: string,
    where = "TRUE",
  ): Promise<Array<Readonly<Record<string, unknown>>>> {
    const result = await this.#query(
      connection,
      `SELECT * FROM ${quote(table)} WHERE ${where} ORDER BY id`,
    ).catch((error: unknown) => this.#fail(`SELECT failed: ${describeError(error)}`, error));
    this.#expectColumns(result.columns, this.#model.columns(table));
    return [...result.rows];
  }

  async #expectRows(
    connection: number,
    table: string,
    expected: readonly PlanRow[],
    context: string,
    where = "TRUE",
  ): Promise<void> {
    const actual = await this.#read(connection, table, where);
    const columns = this.#model.columns(table);
    const observed = canonicalRows(actual, columns);
    const wanted = canonicalRows(expected, columns);
    if (observed !== wanted) {
      this.#fail(
        `${context}: connection ${String(connection)} disagrees with the model for ${table}\n  observed ${observed}\n  expected ${wanted}`,
      );
    }
  }

  #compareRows(
    result: SimulatedQueryResult,
    expected: readonly PlanRow[],
    columns: readonly string[],
    property: string,
  ): void {
    this.#expectColumns(result.columns, columns);
    const observed = result.rows.map((row) => canonical(row, columns)).join("\n");
    const wanted = expected.map((row) => canonical(row, columns)).join("\n");
    if (observed !== wanted) {
      this.#fail(
        `${property}: rows differ\n  observed ${observed.replaceAll("\n", " | ")}\n  expected ${wanted.replaceAll("\n", " | ")}`,
      );
    }
  }

  async #attempt(
    connection: number,
    sql: string,
    params?: readonly PlanValue[],
  ): Promise<Outcome<SimulatedExecuteResult>> {
    try {
      return { result: await this.#execute(connection, sql, params) };
    } catch (error) {
      return { error };
    }
  }

  async #attemptQuery(connection: number, sql: string): Promise<Outcome<SimulatedQueryResult>> {
    try {
      return { result: await this.#query(connection, sql) };
    } catch (error) {
      return { error };
    }
  }
}

interface Outcome<T> {
  readonly result?: T;
  readonly error?: unknown;
}

function keyOf(mutation: KeyedMutation): number {
  return mutation.kind === "insert" || mutation.kind === "upsert"
    ? idOf(mutation.row)
    : mutation.id;
}

function canonical(row: Readonly<Record<string, unknown>>, columns: readonly string[]): string {
  return columns.map((column) => `${column}=${canonicalValue(row[column])}`).join(",");
}

function canonicalValue(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "bigint") return String(value);
  if (typeof value === "number") return Object.is(value, -0) ? "0" : String(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "string") return JSON.stringify(value);
  return `?${typeof value}`;
}

function canonicalRows(
  rows: ReadonlyArray<Readonly<Record<string, unknown>>>,
  columns: readonly string[],
): string {
  return rows.map((row) => canonical(row, columns)).join(" | ");
}

export function describeError(error: unknown): string {
  if (error instanceof Error) return `${error.name}: ${error.message}`;
  if (isRecord(error) && typeof error.message === "string") {
    return `${typeof error.name === "string" ? error.name : "Error"}: ${error.message}`;
  }
  return String(error);
}

function isTableAlreadyExists(error: unknown, table: string): boolean {
  return errorChainSome(
    error,
    (name, message) =>
      (name === "Error" && message === `Table name already exists: ${table}`) ||
      (name === "TypeError" && message === `Table already exists: ${table}`),
  );
}

function isUnknownTable(error: unknown, table: string): boolean {
  return errorChainSome(
    error,
    (name, message) => name === "UnknownTableError" && message === `Unknown table: ${table}`,
  );
}

function isUniqueViolation(error: unknown): boolean {
  return errorChainSome(error, (name) => name === "UniqueConstraintError");
}

/** A write naming a parent row that does not exist: `FOREIGN KEY <name> has no <parent> row ...`. */
function isMissingParent(error: unknown): boolean {
  return errorChainSome(
    error,
    (name, message) =>
      name === "TypeError" && /^FOREIGN KEY \S+ has no \S+ row with /u.test(message ?? ""),
  );
}

/** A delete a `RESTRICT` child refuses: `FOREIGN KEY <name> still has <n> <child> row(s) ...`. */
function isRestricted(error: unknown): boolean {
  return errorChainSome(
    error,
    (name, message) =>
      name === "TypeError" &&
      /^FOREIGN KEY \S+ still has \d+ \S+ row\(s\) referencing /u.test(message ?? ""),
  );
}

function matchesRefusal(error: unknown, refusal: readonly RefusalKind[]): boolean {
  return refusal.some((kind) =>
    kind === "unique"
      ? isUniqueViolation(error)
      : kind === "foreignKey"
        ? isMissingParent(error)
        : isRestricted(error),
  );
}

/**
 * Visible segments a table may hold after a burst of single-row commits. A writer that outruns
 * the background fold lends it its own turn past 512 level-zero segments, so a table that folds
 * at all stays well under this; one whose folds are refused gains a segment per commit.
 */
const BURST_SEGMENT_CEILING = 640;

/**
 * Whether a round's keyed writes, each to its own row, reach the same outcome in any order: no
 * write gives a unique constraint a value another write in the round gives it, or that a row
 * other than its own holds now. Contended rounds run their writes one at a time instead.
 */
function uniqueValuesCommute(
  model: ShadowModel,
  table: string,
  operations: ReadonlyArray<{ readonly mutation: KeyedMutation }>,
  targets: readonly number[],
): boolean {
  const sets = model.uniqueSets(table);
  if (sets.length === 0) return true;
  const { definition, rows } = model.table(table);
  const tuple = (row: PlanRow, columns: readonly string[]): string | undefined => {
    const values = columns.map((column) => row[column] ?? null);
    return values.some((value) => value === null)
      ? undefined
      : values.map(canonicalValue).join("\u0000");
  };
  const written = operations.map(({ mutation }, index): PlanRow | undefined => {
    const target = targets[index] ?? keyOf(mutation);
    const current = rows.get(target);
    switch (mutation.kind) {
      case "insert":
        return current === undefined ? completeRow(definition, mutation.row) : undefined;
      case "upsert":
        return completeRow(definition, { ...mutation.row, id: target });
      case "updateKey":
        return current === undefined ? undefined : assign(current, mutation.assignments);
      case "deleteKey":
        return undefined;
    }
  });
  for (const columns of sets) {
    const claimed = new Set<string>();
    for (const [index, row] of written.entries()) {
      const key = row === undefined ? undefined : tuple(row, columns);
      if (key === undefined) continue;
      if (claimed.has(key)) return false;
      claimed.add(key);
      for (const [id, held] of rows) {
        if (id !== targets[index] && tuple(held, columns) === key) return false;
      }
    }
  }
  return true;
}

function isConflict(error: unknown): boolean {
  return errorChainSome(error, (name) => name === "WriteConflictError");
}

function isExpiredTransaction(error: unknown): boolean {
  return errorChainSome(error, (name) => name === "TransactionExpiredError");
}

/** A SQL transaction the engine refused for a documented reason; the plan step ends with it. */
class RefusedTransaction extends Error {
  constructor(
    readonly reason: "lost commit race" | "idle rollback",
    cause: unknown,
  ) {
    super(`SQL transaction refused: ${reason}`, { cause });
    this.name = "RefusedTransaction";
  }

  get expired(): boolean {
    return this.reason === "idle rollback";
  }
}

function errorChainSome(
  error: unknown,
  predicate: (name: string | undefined, message: string | undefined) => boolean,
): boolean {
  return findErrorInChain(error, predicate) !== undefined;
}

function findErrorInChain(
  error: unknown,
  predicate: (name: string | undefined, message: string | undefined) => boolean,
): Record<string, unknown> | undefined {
  const seen = new Set<unknown>();
  let current = error;
  while (isRecord(current) && !seen.has(current)) {
    seen.add(current);
    const name = typeof current.name === "string" ? current.name : undefined;
    const message = typeof current.message === "string" ? current.message : undefined;
    if (predicate(name, message)) return current;
    current = current.cause;
  }
  return undefined;
}

function isExpectedCrashOutcome(error: unknown): boolean {
  return errorChainSome(error, (name) => name === "DatabaseWorkerOutcomeUnknownError");
}

function isInjectedFault(error: unknown, point: Exclude<FaultPointName, "crash">): boolean {
  const expected = `injected ${point} #1`;
  return errorChainSome(error, (name, message) => name === "Error" && message === expected);
}

function errorInChainWithName(
  error: unknown,
  expectedName: string,
): Record<string, unknown> | undefined {
  return findErrorInChain(error, (name) => name === expectedName);
}

/** A deliberate crash reached recovery, where a browser-level storage wedge prevents judging it. */
class AcceptedStoreUnresponsive extends Error {
  constructor(readonly storageError: unknown) {
    super("The store stopped answering during deliberate crash recovery", {
      cause: storageError,
    });
    this.name = "AcceptedStoreUnresponsive";
  }
}

// Stream-identical copy of `mulberry32` in ./seeds.ts; see simulator.ts for why the published
// simulator cannot import the canonical copy.
function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296;
  };
}

// --- In-process driver ----------------------------------------------------------------------------------

export interface DatabaseDriverOptions {
  /** Options for every `MinnowDatabase` the driver opens; `now` and `createId` stay the engine's. */
  readonly databaseOptions?: MinnowDatabaseOptions;
}

/**
 * Where a connection's block store comes from: one shared instance, as several engines in one
 * process share it, or a factory that opens a separate instance over the same durable database
 * for every connection and every reopen -- the shape of separate tabs, each with its own
 * adapter caches, which is where a cross-tab cache that goes stale shows up.
 */
export type DriverStoreSource = BlockStore | (() => Promise<BlockStore>);

/**
 * A driver whose connections are `MinnowDatabase` instances over one database, with the storage
 * fault points armed through `FaultInjectingBlockStore`. Crash faults are skipped: there is no
 * process to kill.
 */
export function createDatabaseDriver(
  source: DriverStoreSource,
  options: DatabaseDriverOptions = {},
): SimulationDriver {
  const controller = new InjectedFaultController();
  const owned = new Set<BlockStore>();
  const wrap = (store: BlockStore): BlockStore =>
    new FaultInjectingBlockStore(store, (point) => {
      controller.inject(point);
    });
  const shared = typeof source === "function" ? undefined : wrap(source);
  const acquire = async (): Promise<BlockStore> => {
    if (shared !== undefined) return shared;
    const store = await (source as () => Promise<BlockStore>)();
    owned.add(store);
    return store;
  };
  const release = (store: BlockStore | undefined): void => {
    if (store === undefined) return;
    owned.delete(store);
    store.close();
  };
  const databases = new Set<MinnowDatabase>();
  const reports = new WeakMap<MinnowDatabase, { count: number }>();
  const assertDiagnostics = (database: MinnowDatabase): void => {
    const status = database.maintenanceStatus();
    if (
      status.backgroundFailureCount !== reports.get(database)?.count ||
      status.backgroundErrors.length !== Math.min(32, status.backgroundFailureCount) ||
      status.backgroundErrors.some(
        (entry, index) =>
          entry.sequence !==
          status.backgroundFailureCount - status.backgroundErrors.length + index + 1,
      )
    ) {
      throw new Error("Background diagnostic history and observer evidence diverged");
    }
  };
  const open = async (): Promise<{ database: MinnowDatabase; store: BlockStore | undefined }> => {
    const store = await acquire();
    const evidence = { count: 0 };
    const observer = options.databaseOptions?.onBackgroundError;
    const database = new MinnowDatabase(shared === undefined ? wrap(store) : store, {
      ...options.databaseOptions,
      onBackgroundError: (error, context) => {
        evidence.count += 1;
        if (observer !== undefined) observer(error, context);
        else console.error(`[minnowdb] background failure (${context}):`, error);
      },
    });
    reports.set(database, evidence);
    databases.add(database);
    return { database, store: shared === undefined ? store : undefined };
  };
  return {
    faults: controller,
    open: async (): Promise<SimulatedConnection> => {
      let { database, store } = await open();
      return {
        execute: async (sql, params) => {
          const result = await database
            .execute(sql, params)
            .finally(() => assertDiagnostics(database));
          return "rowCount" in result
            ? { kind: result.kind, rowCount: result.rowCount }
            : { kind: result.kind };
        },
        query: async (sql, params) => {
          const result = await database
            .query(
              sql,
              params === undefined ? { memoize: false } : { memoize: false, params: [...params] },
            )
            .finally(() => assertDiagnostics(database));
          return { columns: result.columns, rows: result.rows };
        },
        reopen: async () => {
          await database.close();
          assertDiagnostics(database);
          databases.delete(database);
          release(store);
          ({ database, store } = await open());
        },
        maintain: async (table) => {
          await database.compactTable(table);
          await database.collectGarbage();
        },
        explain: (sql) => database.explain(sql),
        visibleSegments: async (table) => {
          let count = 0;
          let page = await database.listVisibleSegmentPage(table);
          count += page.records.length;
          while (page.nextCursor !== null) {
            page = await database.listVisibleSegmentPage(table, { cursor: page.nextCursor });
            count += page.records.length;
          }
          return count;
        },
      };
    },
    close: async () => {
      await Promise.allSettled([...databases].map((database) => database.close()));
      for (const store of owned) store.close();
      owned.clear();
    },
  };
}

class InjectedFaultController implements SimulatedFaults {
  #point: FaultPoint | undefined;
  #occurrence = 0;
  #seen = 0;
  #fired = false;

  arm(point: Exclude<FaultPointName, "crash">, occurrence: number): void {
    this.#point = point;
    this.#occurrence = occurrence;
    this.#seen = 0;
    this.#fired = false;
  }

  disarm(): void {
    this.#point = undefined;
  }

  fired(): boolean {
    return this.#fired;
  }

  inject(point: FaultPoint): void {
    if (point !== this.#point || this.#fired) return;
    this.#seen++;
    if (this.#seen !== this.#occurrence) return;
    this.#fired = true;
    throw new Error(`injected ${point} #${String(this.#occurrence)}`);
  }
}
