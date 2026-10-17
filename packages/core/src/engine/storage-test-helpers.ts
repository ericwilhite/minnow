import type { BlockStore, SegmentRecord, TransactionRecord } from "../storage/index.js";
import type { MinnowDatabase, VisibleSegment, VisibleSegmentPageCursor } from "./database.js";

/**
 * The test timeout for a file too heavy for vitest.config.ts's 20-second local default, applied
 * with `vi.setConfig({ testTimeout })` rather than as a third `it()` argument — prettier keeps
 * a test call's layout only when that argument is a numeric literal. Locally it is exactly the
 * number given, so a hang still fails fast; on CI it is never below the 300 seconds the config
 * grants every other test there. Hosted CPU contention and coverage instrumentation make the
 * same work substantially slower than a developer run. The maintenance stress suite runs in a
 * separate execution group so unrelated files cannot consume its deadline.
 */
export function heavyTestTimeout(localMs: number): number {
  return process.env.CI === undefined ? localMs : Math.max(localMs, 300_000);
}

/** Exhausts the bounded public segment cursor for finite test fixtures. */
export async function allVisibleSegments(
  database: MinnowDatabase,
  tableName: string,
  version?: number,
): Promise<VisibleSegment[]> {
  const records: VisibleSegment[] = [];
  let cursor: VisibleSegmentPageCursor | null = null;
  do {
    const page = await database.listVisibleSegmentPage(
      tableName,
      cursor === null ? { ...(version === undefined ? {} : { version }) } : { cursor },
    );
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records
    .sort((left, right) => {
      const leftPage = left as VisibleSegment & {
        logicalOrder: number;
        committedVersion: number;
        commitOrdinal: number;
      };
      const rightPage = right as typeof leftPage;
      return (
        leftPage.logicalOrder - rightPage.logicalOrder ||
        leftPage.committedVersion - rightPage.committedVersion ||
        leftPage.commitOrdinal - rightPage.commitOrdinal ||
        left.id.localeCompare(right.id)
      );
    })
    .map(({ id, rowCount, columnBlockIds }) => ({ id, rowCount, columnBlockIds }));
}

/** Exhausts a bounded segment cursor for tests that need the complete finite fixture. */
export async function allSegmentRecords(
  store: BlockStore,
  tableId?: string,
): Promise<SegmentRecord[]> {
  const records: SegmentRecord[] = [];
  let cursor: string | null = null;
  do {
    const page: { records: SegmentRecord[]; nextCursor: string | null } =
      tableId === undefined
        ? await store.listSegmentPage(cursor, 1_024)
        : await store.listTableSegmentPage(tableId, cursor, 1_024);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

/** Exhausts a bounded transaction cursor for tests that need the complete finite fixture. */
export async function allTransactionRecords(store: BlockStore): Promise<TransactionRecord[]> {
  const records: TransactionRecord[] = [];
  let cursor: string | null = null;
  do {
    const page = await store.listTransactionPage(cursor, 1_024);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

/** What `instrumentStoreCalls` records, and the hook it runs before each call while armed. */
export interface StoreCallProbe {
  armed: boolean;
  /** Store calls made while armed, by method name, in order. */
  readonly calls: string[];
  onCall: (call: number) => void;
}

/**
 * Runs the probe's `onCall` before every store method call while it is armed. Wrapping every
 * method, rather than a chosen few, is what lets a sweep reach every step an operation takes.
 * Defined on the instance, not assigned: the OPFS store's methods are read-only on its
 * prototype.
 */
export function instrumentStoreCalls(store: BlockStore): StoreCallProbe {
  const probe: StoreCallProbe = { armed: false, calls: [], onCall: () => undefined };
  const seen = new Set<string>();
  for (
    let prototype = Object.getPrototypeOf(store) as object | null;
    prototype !== null && prototype !== Object.prototype;
    prototype = Object.getPrototypeOf(prototype) as object | null
  ) {
    for (const name of Object.getOwnPropertyNames(prototype)) {
      const descriptor = Object.getOwnPropertyDescriptor(prototype, name);
      if (name === "constructor" || seen.has(name) || typeof descriptor?.value !== "function") {
        continue;
      }
      seen.add(name);
      const original = descriptor.value as (...args: unknown[]) => unknown;
      Object.defineProperty(store, name, {
        configurable: true,
        writable: true,
        value: function (this: unknown, ...args: unknown[]): unknown {
          if (probe.armed) {
            probe.calls.push(name);
            probe.onCall(probe.calls.length);
          }
          return original.apply(this, args);
        },
      });
    }
  }
  return probe;
}
