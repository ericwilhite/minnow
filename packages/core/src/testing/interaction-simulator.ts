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
 *   - fault-atomicity: a mutation interrupted by a storage fault or a crash lands entirely or
 *     not at all, and a reported success is durable
 *   - agreement: at every checkpoint every connection reads the same database as the model
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
}

export interface PlanTable {
  readonly name: string;
  /** Every table has `id INTEGER PRIMARY KEY`; these are the other columns. */
  readonly columns: readonly PlanColumn[];
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
  | { readonly kind: "and"; readonly left: Predicate; readonly right: Predicate }
  | { readonly kind: "or"; readonly left: Predicate; readonly right: Predicate }
  | { readonly kind: "not"; readonly inner: Predicate }
  | { readonly kind: "literal"; readonly value: boolean };

export type Assignment =
  | { readonly kind: "set"; readonly column: string; readonly value: PlanValue }
  | { readonly kind: "increment"; readonly column: string; readonly by: number };

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
  | { readonly kind: "delete"; readonly predicate: Predicate };

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
    }
  | {
      readonly kind: "dropTable";
      readonly connection: number;
      readonly table: string;
      readonly expectMissing: boolean;
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
  | { readonly kind: "reopen"; readonly connection: number }
  | { readonly kind: "maintenance"; readonly connection: number; readonly table: string }
  | { readonly kind: "checkpoint" };

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
  readonly interactions: number;
  readonly statements: number;
  readonly queries: number;
  readonly acceptedWrites: number;
  readonly rejectedConflicts: number;
  readonly expectedFailures: number;
  readonly faultsInjected: number;
  readonly faultsSkipped: number;
  readonly reopens: number;
  readonly checkpoints: number;
  readonly tablesAtEnd: number;
  readonly rowsAtEnd: number;
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
  const interactions: Interaction[] = [];
  // The generator keeps its own shadow of the schema so predicates name real columns and keyed
  // rounds target keys that can commute. Values are not tracked: the runner's model does that.
  const live = new Map<string, PlanTable>();
  const indexCount = new Map<string, number>();
  let tableSerial = 0;
  const connection = (): number => random.int(connections);
  const anyTable = (): PlanTable | undefined => {
    const tables = [...live.values()];
    return tables.length === 0 ? undefined : random.pick(tables);
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
    const table: PlanTable = { name: `t${String(tableSerial++)}`, columns };
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
  };

  createTable();
  for (let step = 0; step < length; step++) {
    const table = anyTable();
    if (table === undefined || (live.size < tableLimit && random.chance(0.04))) {
      createTable();
      continue;
    }
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
    } else if (roll < 0.72 && live.size > 1) {
      live.delete(table.name);
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
    if (step % 25 === 24) interactions.push({ kind: "checkpoint" });
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
      if (!Array.isArray(value.columns) || value.columns.length === 0 || value.columns.length > 4) {
        throw new TypeError(`${where} index columns are invalid`);
      }
      return;
    }
    case "dropTable": {
      checkConnection("connection");
      checkTableName();
      if (typeof value.expectMissing !== "boolean")
        throw new TypeError(`${where} expectMissing must be boolean`);
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
  for (const column of value.columns as unknown[]) {
    if (
      !isRecord(column) ||
      typeof column.name !== "string" ||
      !/^[a-z][a-z0-9_]*$/u.test(column.name) ||
      column.name === "id" ||
      !["integer", "real", "text", "boolean"].includes(column.type as string) ||
      typeof column.nullable !== "boolean"
    ) {
      throw new TypeError(`${where} table column is invalid`);
    }
  }
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

interface ModelTable {
  readonly definition: PlanTable;
  readonly rows: Map<number, PlanRow>;
}

class ShadowModel {
  readonly tables = new Map<string, ModelTable>();

  clone(): ShadowModel {
    const copy = new ShadowModel();
    for (const [name, table] of this.tables) {
      copy.tables.set(name, { definition: table.definition, rows: new Map(table.rows) });
    }
    return copy;
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
    return this.sorted(name).filter((row) => evaluate(predicate, row) === true);
  }

  sorted(name: string): PlanRow[] {
    return [...this.table(name).rows.values()].sort((left, right) => idOf(left) - idOf(right));
  }

  insert(name: string, rows: readonly PlanRow[]): boolean {
    const table = this.table(name);
    const staged = new Set<number>();
    for (const row of rows) {
      const id = idOf(row);
      if (table.rows.has(id) || staged.has(id)) return false;
      staged.add(id);
    }
    for (const row of rows) table.rows.set(idOf(row), completeRow(table.definition, row));
    return true;
  }

  update(name: string, predicate: Predicate, assignments: readonly Assignment[]): number {
    const table = this.table(name);
    let count = 0;
    for (const row of this.matching(name, predicate)) {
      table.rows.set(idOf(row), assign(row, assignments));
      count++;
    }
    return count;
  }

  delete(name: string, predicate: Predicate): number {
    const table = this.table(name);
    let count = 0;
    for (const row of this.matching(name, predicate)) {
      table.rows.delete(idOf(row));
      count++;
    }
    return count;
  }

  /** Applies a keyed mutation; `false` means the engine must reject it (duplicate insert). */
  applyKeyed(name: string, mutation: KeyedMutation): boolean {
    const table = this.table(name);
    switch (mutation.kind) {
      case "insert":
        return this.insert(name, [mutation.row]);
      case "upsert":
        table.rows.set(idOf(mutation.row), completeRow(table.definition, mutation.row));
        return true;
      case "updateKey": {
        const row = table.rows.get(mutation.id);
        if (row !== undefined) table.rows.set(mutation.id, assign(row, mutation.assignments));
        return true;
      }
      case "deleteKey":
        table.rows.delete(mutation.id);
        return true;
    }
  }
}

function idOf(row: PlanRow): number {
  const id = row.id;
  if (typeof id !== "number") throw new Error("Row without a numeric id");
  return id;
}

function completeRow(definition: PlanTable, row: PlanRow): PlanRow {
  const complete: Record<string, PlanValue> = { id: idOf(row) };
  for (const column of definition.columns) complete[column.name] = row[column.name] ?? null;
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

function renderCreateTable(table: PlanTable): string {
  const columns = table.columns.map(
    (column) =>
      `${quote(column.name)} ${sqlType(column.type)}${column.nullable ? "" : " NOT NULL"}`,
  );
  return `CREATE TABLE ${quote(table.name)} (id INTEGER PRIMARY KEY, ${columns.join(", ")})`;
}

function renderInsert(
  table: string,
  columns: readonly string[],
  rows: readonly PlanRow[],
  viaParameters: boolean,
): { sql: string; params: PlanValue[] } {
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

  constructor(plan: InteractionPlan, driver: SimulationDriver, traceLength: number) {
    this.#plan = plan;
    this.#driver = driver;
    this.#traceLength = traceLength;
  }

  async run(): Promise<InteractionRunResult> {
    for (let index = 0; index < this.#plan.connections; index++) {
      this.#connections.push(await this.#driver.open(index));
    }
    for (const [index, interaction] of this.#plan.interactions.entries()) {
      this.#index = index;
      await this.#step(interaction);
    }
    let rows = 0;
    for (const table of this.#model.tables.values()) rows += table.rows.size;
    return {
      seed: this.#plan.seed,
      interactions: this.#plan.interactions.length,
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
    return this.#connection(connection).query(sql);
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
    }
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
      this.#expectedFailures++;
      return;
    }
    if (outcome.error !== undefined)
      this.#fail(`CREATE TABLE failed: ${describeError(outcome.error)}`, outcome.error);
    this.#model.tables.set(interaction.table.name, {
      definition: interaction.table,
      rows: new Map(),
    });
  }

  async #insert(interaction: Extract<Interaction, { kind: "insert" }>): Promise<void> {
    const columns = this.#model.columns(interaction.table);
    const rendered = renderInsert(
      interaction.table,
      columns,
      interaction.rows,
      interaction.viaParameters,
    );
    const before = this.#model.clone();
    const accepted = this.#model.insert(interaction.table, interaction.rows);
    const outcome = await this.#attempt(interaction.connection, rendered.sql, rendered.params);
    if (!accepted) {
      this.#model = before;
      if (outcome.error === undefined)
        this.#fail("insert-select: a duplicate primary key was accepted");
      if (!isUniqueViolation(outcome.error)) {
        this.#fail(
          `duplicate insert failed with the wrong error: ${describeError(outcome.error)}`,
          outcome.error,
        );
      }
      this.#expectedFailures++;
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
    const expected = this.#model.update(
      interaction.table,
      interaction.predicate,
      interaction.assignments,
    );
    const sql = `UPDATE ${quote(interaction.table)} SET ${renderAssignments(interaction.assignments)} WHERE ${renderPredicate(interaction.predicate)}`;
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
    const expected = this.#model.delete(interaction.table, interaction.predicate);
    const where = renderPredicate(interaction.predicate);
    const result = await this.#execute(
      interaction.connection,
      `DELETE FROM ${quote(interaction.table)} WHERE ${where}`,
    ).catch((error: unknown) => this.#fail(`DELETE failed: ${describeError(error)}`, error));
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
      (row) => evaluate(interaction.predicate, row) === true,
    ).length;
    const modelUnknown = rows.filter((row) => evaluate(interaction.predicate, row) === null).length;
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
    const sql = `CREATE INDEX ${quote(interaction.name)} ON ${quote(interaction.table)} (${interaction.columns.map(quote).join(", ")})`;
    await this.#execute(interaction.connection, sql).catch((error: unknown) =>
      this.#fail(`CREATE INDEX failed: ${describeError(error)}`, error),
    );
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
      this.#expectedFailures++;
      return;
    }
    if (outcome.error !== undefined)
      this.#fail(`DROP TABLE failed: ${describeError(outcome.error)}`, outcome.error);
    this.#model.tables.delete(interaction.table);
    const select = await this.#attemptQuery(
      interaction.connection,
      `SELECT * FROM ${quote(interaction.table)}`,
    );
    if (select.error === undefined)
      this.#fail("drop-select: a dropped table still answers queries");
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
      if (statement.kind === "insert") {
        const accepted = this.#model.insert(interaction.table, statement.rows);
        const rendered = renderInsert(interaction.table, columns, statement.rows, true);
        const outcome = await this.#attempt(interaction.connection, rendered.sql, rendered.params);
        if (accepted && outcome.error !== undefined)
          this.#refuseTransaction("INSERT inside a transaction", outcome.error);
        if (!accepted) {
          if (outcome.error === undefined)
            this.#fail("a duplicate primary key was accepted inside a transaction");
          this.#expectedFailures++;
          // A failed statement inside a SQL transaction leaves the transaction unusable; the
          // engine must refuse further statements and COMMIT must roll back, like PostgreSQL.
          poisoned = true;
          break;
        }
      } else if (statement.kind === "update") {
        this.#model.update(interaction.table, statement.predicate, statement.assignments);
        const sql = `UPDATE ${quote(interaction.table)} SET ${renderAssignments(statement.assignments)} WHERE ${renderPredicate(statement.predicate)}`;
        await this.#execute(interaction.connection, sql).catch((error: unknown) =>
          this.#refuseTransaction("UPDATE inside a transaction", error),
        );
      } else {
        this.#model.delete(interaction.table, statement.predicate);
        await this.#execute(
          interaction.connection,
          `DELETE FROM ${quote(interaction.table)} WHERE ${renderPredicate(statement.predicate)}`,
        ).catch((error: unknown) => this.#refuseTransaction("DELETE inside a transaction", error));
      }
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
    const writes = interaction.operations.map(async (operation) => {
      const sql = renderKeyed(interaction.table, columns, operation.mutation);
      const outcome = await this.#attempt(operation.connection, sql);
      return { operation, outcome };
    });
    const reads = interaction.readers.map(async (reader) => ({
      reader,
      outcome: await this.#attemptQuery(
        reader,
        `SELECT * FROM ${quote(interaction.table)} ORDER BY id`,
      ),
    }));
    const [writeOutcomes, readOutcomes] = await Promise.all([
      Promise.all(writes),
      Promise.all(reads),
    ]);
    const touched = new Set<number>();
    for (const { operation, outcome } of writeOutcomes) {
      const id = keyOf(operation.mutation);
      touched.add(id);
      const wouldAccept = before.clone().applyKeyed(interaction.table, operation.mutation);
      if (outcome.error === undefined) {
        if (!wouldAccept)
          this.#fail(`concurrent insert of an existing key ${String(id)} was accepted`);
        this.#model.applyKeyed(interaction.table, operation.mutation);
        this.#acceptedWrites++;
      } else if (isUniqueViolation(outcome.error)) {
        if (wouldAccept)
          this.#fail(
            `concurrent write on key ${String(id)} was refused as a duplicate: ${describeError(outcome.error)}`,
          );
        this.#expectedFailures++;
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

  async #fault(interaction: Extract<Interaction, { kind: "fault" }>): Promise<void> {
    const connection = this.#connection(interaction.connection);
    const columns = this.#model.columns(interaction.table);
    const sql = renderKeyed(interaction.table, columns, interaction.mutation);
    const before = this.#model.clone();
    const after = this.#model.clone();
    const accepted = after.applyKeyed(interaction.table, interaction.mutation);
    if (!accepted) {
      // A duplicate insert is refused before any storage write; nothing to interrupt.
      const outcome = await this.#attempt(interaction.connection, sql);
      if (outcome.error === undefined)
        this.#fail("a duplicate insert was accepted during a fault step");
      this.#expectedFailures++;
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
    if (
      outcome.error !== undefined &&
      !isUnknownOutcome(outcome.error) &&
      !isInjectedFault(outcome.error) &&
      !isConflict(outcome.error)
    ) {
      this.#fail(
        `fault ${interaction.point} surfaced an unexpected error: ${describeError(outcome.error)}`,
      );
    }
    // Reopen before judging: the model is settled only once the durable state has been read.
    await this.#reopenConnection(interaction.connection);
    const actual = await this.#read(interaction.connection, interaction.table);
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
    const missing = columns.filter((column) => !result.columns.includes(column));
    if (missing.length > 0) this.#fail(`${property}: result lacks columns ${missing.join(", ")}`);
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

function errorText(error: unknown): string {
  return describeError(error);
}

function isUniqueViolation(error: unknown): boolean {
  return /UniqueConstraint|duplicate value|unique/iu.test(errorText(error));
}

function isConflict(error: unknown): boolean {
  return /WriteConflict|Manifest changed|conflict/iu.test(errorText(error));
}

function isExpiredTransaction(error: unknown): boolean {
  return /TransactionExpired|transaction expired/iu.test(errorText(error));
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

function isUnknownOutcome(error: unknown): boolean {
  return /OutcomeUnknown|UnknownOutcome|ConnectionLost|Worker.*(terminated|failed|closed)|is closed/iu.test(
    errorText(error),
  );
}

function isInjectedFault(error: unknown): boolean {
  return /injected/iu.test(errorText(error));
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
  const open = async (): Promise<{ database: MinnowDatabase; store: BlockStore | undefined }> => {
    const store = await acquire();
    const database = new MinnowDatabase(shared === undefined ? wrap(store) : store, {
      ...options.databaseOptions,
    });
    databases.add(database);
    return { database, store: shared === undefined ? store : undefined };
  };
  return {
    faults: controller,
    open: async (): Promise<SimulatedConnection> => {
      let { database, store } = await open();
      return {
        execute: async (sql, params) => {
          const result = await database.execute(sql, params);
          return "rowCount" in result
            ? { kind: result.kind, rowCount: result.rowCount }
            : { kind: result.kind };
        },
        query: async (sql, params) => {
          const result = await database.query(
            sql,
            params === undefined ? { memoize: false } : { memoize: false, params: [...params] },
          );
          return { columns: result.columns, rows: result.rows };
        },
        reopen: async () => {
          await database.close();
          databases.delete(database);
          release(store);
          ({ database, store } = await open());
        },
        maintain: async (table) => {
          await database.compactTable(table);
          await database.collectGarbage();
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
