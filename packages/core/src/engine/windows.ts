import type {
  QueryResult,
  QueryRow,
  QueryValue,
  WindowFrameBound,
  WindowSpec,
} from "../plan/model.js";
import { QueryMemoryContext } from "./memory.js";
import { buildSortKeyColumn, sortKeyIndexes } from "./sort-keys.js";
import { compareSqlValues } from "./sql-semantics.js";
import { exactNumericBinary, exactNumericValue, isExactNumeric } from "./sql-domains.js";
import { throwIfAborted } from "./cancellation.js";

interface WindowOptions {
  copyRows?: boolean;
  memoryContext?: QueryMemoryContext;
  signal?: AbortSignal;
}

interface Partition {
  indexes: Uint32Array;
  peerStart: Int32Array;
  peerEnd: Int32Array;
  groupOrdinal: Int32Array;
  groupStarts: number[];
  orderValues: QueryValue[];
}

/** A range tree combines only members inside the frame; it never subtracts rounded prefixes. */
class FrameAggregate {
  readonly #width: number;
  readonly #values: QueryValue[];
  readonly #counts: Uint32Array;
  readonly #window: WindowSpec;
  readonly #exact: boolean;
  #prefixEnd = 0;
  #prefixTotal: QueryValue = null;
  #prefixCount = 0;

  constructor(
    values: QueryValue[],
    window: WindowSpec,
    memory: QueryMemoryContext,
    prefix: boolean,
  ) {
    this.#window = window;
    this.#exact = values.some(isExactNumeric);
    if (prefix) {
      this.#width = 0;
      this.#values = values;
      this.#counts = new Uint32Array(0);
      return;
    }
    const width = 2 ** Math.ceil(Math.log2(Math.max(1, values.length)));
    this.#width = width;
    memory.tally(width * 2 * 20, "Window aggregate tree");
    this.#values = new Array<QueryValue>(width * 2).fill(null);
    this.#counts = new Uint32Array(width * 2);
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index] ?? null;
      this.#values[width + index] = value;
      this.#counts[width + index] = value === null ? 0 : 1;
    }
    for (let index = width - 1; index > 0; index -= 1) {
      this.#counts[index] = (this.#counts[index * 2] ?? 0) + (this.#counts[index * 2 + 1] ?? 0);
      this.#values[index] = this.#combine(
        this.#values[index * 2] ?? null,
        this.#values[index * 2 + 1] ?? null,
      );
    }
  }

  #combine(left: QueryValue, right: QueryValue): QueryValue {
    if (left === null) return right;
    if (right === null) return left;
    if (this.#window.name === "COUNT") return null;
    if (this.#window.name === "MIN") return compareSqlValues(left, right) <= 0 ? left : right;
    if (this.#window.name === "MAX") return compareSqlValues(left, right) >= 0 ? left : right;
    if (this.#exact) {
      const total = exactNumericBinary("+", left, right);
      if (total === undefined) throw new TypeError("Window SUM requires numeric values");
      return total;
    }
    if (typeof left !== "number" || typeof right !== "number")
      throw new TypeError("Window SUM requires numeric values");
    return left + right;
  }

  /** Prefixes grow without subtraction, so each input is combined once without a range tree. */
  prefixValue(end: number, scale?: number): QueryValue {
    while (this.#prefixEnd < end) {
      const value = this.#values[this.#prefixEnd] ?? null;
      this.#prefixEnd += 1;
      if (value === null) continue;
      this.#prefixCount += 1;
      this.#prefixTotal = this.#combine(this.#prefixTotal, value);
    }
    return this.#finish(this.#prefixTotal, this.#prefixCount, scale);
  }

  value(ranges: ReadonlyArray<readonly [number, number]>, scale?: number): QueryValue {
    let total: QueryValue = null;
    let count = 0;
    for (const [from, to] of ranges) {
      let low = from + this.#width;
      let high = to + this.#width;
      let left: QueryValue = null;
      let right: QueryValue = null;
      while (low < high) {
        if (low % 2 === 1) {
          count += this.#counts[low] ?? 0;
          left = this.#combine(left, this.#values[low] ?? null);
          low += 1;
        }
        if (high % 2 === 1) {
          high -= 1;
          count += this.#counts[high] ?? 0;
          right = this.#combine(this.#values[high] ?? null, right);
        }
        low = Math.floor(low / 2);
        high = Math.floor(high / 2);
      }
      total = this.#combine(total, this.#combine(left, right));
    }
    return this.#finish(total, count, scale);
  }

  #finish(total: QueryValue, count: number, scale?: number): QueryValue {
    if (this.#window.name === "COUNT") return count;
    if (count === 0) return null;
    if (this.#window.name !== "AVG") return total;
    if (this.#exact)
      return exactNumericBinary("/", total ?? exactNumericValue(0), count, scale) ?? null;
    if (typeof total !== "number") throw new TypeError("Window AVG requires numeric values");
    return total / count;
  }
}

function frameRanges(
  partition: Partition,
  window: WindowSpec,
  position: number,
): Array<readonly [number, number]> {
  const size = partition.indexes.length;
  const frame = window.frame ?? {
    unit: "range",
    start: { kind: "unbounded-preceding" },
    end: { kind: window.orderAliases.length === 0 ? "unbounded-following" : "current-row" },
  };
  const groupEdge = (group: number): number =>
    group < 0 ? 0 : (partition.groupStarts[group] ?? size);
  const bound = (edge: WindowFrameBound, start: boolean): number => {
    switch (edge.kind) {
      case "unbounded-preceding":
        return 0;
      case "unbounded-following":
        return size;
      case "current-row":
        return frame.unit === "rows"
          ? position + (start ? 0 : 1)
          : start
            ? (partition.peerStart[position] ?? 0)
            : (partition.peerEnd[position] ?? size);
      case "preceding":
      case "following": {
        const delta = (edge.offset ?? 0) * (edge.kind === "preceding" ? -1 : 1);
        if (frame.unit === "range") {
          const current = partition.orderValues[position] ?? null;
          if (current === null)
            return start
              ? (partition.peerStart[position] ?? 0)
              : (partition.peerEnd[position] ?? size);
          if (typeof current !== "number" && !isExactNumeric(current))
            throw new TypeError("Offset RANGE frames require numeric ORDER BY values");
          const order = window.orderAliases[0];
          if (window.orderAliases.length !== 1 || order === undefined)
            throw new TypeError("Offset RANGE frames require exactly one ORDER BY expression");
          const descending = order.direction === "desc";
          const distance = descending ? -delta : delta;
          const target = isExactNumeric(current)
            ? (exactNumericBinary("+", current, distance) ?? null)
            : current + distance;
          let low = 0;
          let high = size;
          while (low < high) {
            const middle = Math.floor((low + high) / 2);
            const value = partition.orderValues[middle] ?? null;
            const comparison =
              value === null
                ? (order.nulls ?? (descending ? "first" : "last")) === "first"
                  ? -1
                  : 1
                : compareSqlValues(value, target) * (descending ? -1 : 1);
            if (comparison < 0 || (!start && comparison === 0)) low = middle + 1;
            else high = middle;
          }
          return low;
        }
        return frame.unit === "groups"
          ? groupEdge((partition.groupOrdinal[position] ?? 0) + delta + (start ? 0 : 1))
          : position + delta + (start ? 0 : 1);
      }
    }
  };
  const low = Math.max(0, Math.min(size, bound(frame.start, true)));
  const high = Math.max(0, Math.min(size, bound(frame.end, false)));
  if (high <= low) return [];
  if (frame.exclude === undefined || frame.exclude === "no-others") return [[low, high]];
  const from =
    frame.exclude === "current-row" ? position : (partition.peerStart[position] ?? position);
  const to =
    frame.exclude === "current-row" ? position + 1 : (partition.peerEnd[position] ?? position + 1);
  const ranges: Array<readonly [number, number]> = [];
  if (low < Math.min(high, from)) ranges.push([low, Math.min(high, from)]);
  if (frame.exclude === "ties" && position >= low && position < high)
    ranges.push([position, position + 1]);
  if (Math.max(low, to) < high) ranges.push([Math.max(low, to), high]);
  return ranges;
}

function* applyPartition(
  rows: QueryRow[],
  partition: Partition,
  window: WindowSpec,
  result: QueryResult,
  memory: QueryMemoryContext,
): Generator<void> {
  const { indexes, peerStart, peerEnd, groupOrdinal } = partition;
  const size = indexes.length;
  memory.tally(size * 8, "Window argument values");
  const values = Array.from(indexes, (index) =>
    window.argumentAlias === undefined ? 1 : (rows[index]?.[window.argumentAlias] ?? null),
  );
  const aggregates = new Set(["SUM", "AVG", "COUNT", "MIN", "MAX"]);
  const frame = window.frame;
  const prefix =
    frame === undefined ||
    ((frame.exclude === undefined || frame.exclude === "no-others") &&
      frame.start.kind === "unbounded-preceding" &&
      (frame.end.kind === "current-row" || frame.end.kind === "unbounded-following"));
  const whole =
    frame?.end.kind === "unbounded-following" ||
    (frame === undefined && window.orderAliases.length === 0);
  const aggregate = aggregates.has(window.name)
    ? new FrameAggregate(values, window, memory, prefix)
    : undefined;
  const domain = result.columnDomains[result.columns.indexOf(window.argumentAlias ?? "")];
  for (let position = 0; position < size; position += 1) {
    if (position % 2048 === 0) yield;
    let value: QueryValue;
    switch (window.name) {
      case "ROW_NUMBER":
        value = position + 1;
        break;
      case "RANK":
        value = (peerStart[position] ?? 0) + 1;
        break;
      case "DENSE_RANK":
        value = (groupOrdinal[position] ?? 0) + 1;
        break;
      case "PERCENT_RANK":
        value = size === 1 ? 0 : (peerStart[position] ?? 0) / (size - 1);
        break;
      case "CUME_DIST":
        value = (peerEnd[position] ?? size) / size;
        break;
      case "NTILE": {
        const buckets = window.offset ?? 1;
        const width = Math.floor(size / buckets);
        const extra = size % buckets;
        const larger = (width + 1) * extra;
        value =
          position < larger
            ? Math.floor(position / (width + 1)) + 1
            : extra + Math.floor((position - larger) / width) + 1;
        break;
      }
      case "LAG":
      case "LEAD": {
        const target = position + (window.offset ?? 1) * (window.name === "LAG" ? -1 : 1);
        value = target < 0 || target >= size ? (window.fallback ?? null) : (values[target] ?? null);
        break;
      }
      default: {
        if (aggregate !== undefined && prefix) {
          const end = whole
            ? size
            : frame?.unit === "rows"
              ? position + 1
              : (peerEnd[position] ?? size);
          value = aggregate.prefixValue(end, domain?.kind === "numeric" ? domain.scale : undefined);
          break;
        }
        const ranges = frameRanges(partition, window, position);
        if (aggregate !== undefined)
          value = aggregate.value(ranges, domain?.kind === "numeric" ? domain.scale : undefined);
        else {
          let remaining = window.name === "NTH_VALUE" ? (window.offset ?? 1) : 1;
          value = null;
          if (window.name === "LAST_VALUE") {
            const last = ranges.at(-1);
            value = last === undefined ? null : (values[last[1] - 1] ?? null);
          } else {
            for (const [from, to] of ranges) {
              if (remaining <= to - from) {
                value = values[from + remaining - 1] ?? null;
                break;
              }
              remaining -= to - from;
            }
          }
        }
      }
    }
    const row = rows[indexes[position] ?? -1];
    if (row !== undefined)
      Object.defineProperty(row, window.alias, {
        value,
        enumerable: true,
        writable: true,
        configurable: true,
      });
  }
}

function* windowSteps(
  result: QueryResult,
  windows: readonly WindowSpec[],
  options: WindowOptions,
): Generator<void, QueryResult> {
  const memory = options.memoryContext ?? new QueryMemoryContext();
  const owned = options.memoryContext === undefined;
  const rows: QueryRow[] = [];
  try {
    memory.tally(
      result.rows.length *
        (24 +
          windows.length * 16 +
          (options.copyRows === false ? 0 : 48 + result.columns.length * 16)),
      "Window result rows",
    );
    for (let index = 0; index < result.rows.length; index += 1) {
      if (index % 2048 === 0) yield;
      const row = result.rows[index] ?? {};
      rows.push(options.copyRows === false ? row : { ...row });
    }
    const groups = new Map<string, WindowSpec[]>();
    for (const window of windows) {
      const key = JSON.stringify([window.partitionAliases, window.orderAliases]);
      const group = groups.get(key);
      if (group === undefined) groups.set(key, [window]);
      else group.push(window);
    }
    for (const group of groups.values()) {
      const window = group[0];
      if (window === undefined) continue;
      const work = memory.createChild();
      try {
        const aliases = [
          ...window.partitionAliases,
          ...window.orderAliases.map(({ alias }) => alias),
        ];
        work.tally(rows.length * (48 + aliases.length * 40), "Window sort and partition buffers");
        const columns = aliases.map((alias) =>
          buildSortKeyColumn(rows.length, (index) => rows[index]?.[alias] ?? null),
        );
        yield;
        const indexes = sortKeyIndexes(
          rows.length,
          columns.map((column, index) => {
            const order = window.orderAliases[index - window.partitionAliases.length];
            return { column, descending: order?.direction === "desc", nulls: order?.nulls };
          }),
        );
        const partitions = columns.slice(0, window.partitionAliases.length);
        const orders = columns.slice(window.partitionAliases.length);
        let start = 0;
        while (start < indexes.length) {
          let end = start + 1;
          while (
            end < indexes.length &&
            partitions.every(
              (column) => column.compare(indexes[start] ?? 0, indexes[end] ?? 0) === 0,
            )
          ) {
            if (end % 2048 === 0) yield;
            end += 1;
          }
          const size = end - start;
          const partition: Partition = {
            indexes: indexes.subarray(start, end),
            peerStart: new Int32Array(size),
            peerEnd: new Int32Array(size),
            groupOrdinal: new Int32Array(size),
            groupStarts: [],
            orderValues: Array.from(
              indexes.subarray(start, end),
              (index) => rows[index]?.[window.orderAliases[0]?.alias ?? ""] ?? null,
            ),
          };
          let begin = 0;
          for (let position = 1; position <= size; position += 1) {
            if (position % 2048 === 0) yield;
            if (
              position === size ||
              orders.some(
                (column) =>
                  column.compare(indexes[start + begin] ?? 0, indexes[start + position] ?? 0) !== 0,
              )
            ) {
              const ordinal = partition.groupStarts.length;
              partition.groupStarts.push(begin);
              partition.peerStart.fill(begin, begin, position);
              partition.peerEnd.fill(position, begin, position);
              partition.groupOrdinal.fill(ordinal, begin, position);
              begin = position;
            }
          }
          for (const member of group) {
            const frameMemory = work.createChild();
            try {
              yield* applyPartition(rows, partition, member, result, frameMemory);
            } finally {
              frameMemory.close();
            }
          }
          start = end;
        }
      } finally {
        work.close();
      }
    }
    return {
      columns: [...result.columns, ...windows.map(({ alias }) => alias)],
      columnDomains: [...result.columnDomains, ...windows.map(() => null)],
      rows,
    };
  } finally {
    if (owned) memory.close();
  }
}

/** Synchronous executor used by standalone queries; shares every kernel with asynchronous reads. */
export function applyWindowFunctions(
  result: QueryResult,
  windows: readonly WindowSpec[],
  options: WindowOptions = {},
): QueryResult {
  const steps = windowSteps(result, windows, options);
  try {
    for (;;) {
      throwIfAborted(options.signal);
      const step = steps.next();
      if (step.done) return step.value;
    }
  } finally {
    steps.return({ columns: [], columnDomains: [], rows: [] });
  }
}

/** Cooperatively runs window passes so cancellation can arrive while a large partition is active. */
export async function applyWindowFunctionsAsync(
  result: QueryResult,
  windows: readonly WindowSpec[],
  options: WindowOptions = {},
): Promise<QueryResult> {
  const steps = windowSteps(result, windows, options);
  let yieldedAt = performance.now();
  try {
    for (;;) {
      throwIfAborted(options.signal);
      const step = steps.next();
      if (step.done) return step.value;
      if (performance.now() - yieldedAt >= 8) {
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        yieldedAt = performance.now();
      }
    }
  } finally {
    steps.return({ columns: [], columnDomains: [], rows: [] });
  }
}
