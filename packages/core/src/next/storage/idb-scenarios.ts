/**
 * Transaction scenarios that pin the IndexedDB semantics the backend relies on (idb-like.ts
 * "Rules"). Run against the simulated IndexedDB in Node (sim-idb.test.ts) and against both the
 * real and the simulated one in browsers (browser-tests/next-storage.spec.ts), where the two
 * transcripts must be identical: the simulator is only evidence while it matches the platform.
 * Test support; nothing in the engine imports it.
 */
import type { IdbDatabaseLike, IdbStoreLike } from "./idb-like.js";

export interface ScenarioOutcome {
  /** What the scenario observed, in order. */
  readonly seen: unknown[];
  /** The name of the error the transaction (or a late call) failed with, or null. */
  readonly error: string | null;
}

const errorName = (error: unknown) =>
  typeof error === "object" && error !== null && "name" in error
    ? String(error.name)
    : String(error);

const bytesOf = (value: unknown) =>
  value instanceof ArrayBuffer ? [...new Uint8Array(value)] : value;

async function scenario(
  db: IdbDatabaseLike,
  run: (db: IdbDatabaseLike, seen: unknown[]) => Promise<void>,
): Promise<ScenarioOutcome> {
  const seen: unknown[] = [];
  try {
    await run(db, seen);
    return { seen, error: null };
  } catch (error) {
    return { seen, error: errorName(error) };
  }
}

/** Every scenario, by name, on fresh keys of `db` (`meta` and `data` stores). */
export async function runIdbScenarios(
  db: IdbDatabaseLike,
): Promise<Record<string, ScenarioOutcome>> {
  const out: Record<string, ScenarioOutcome> = {};
  // Requests execute in issuance order: a get between two puts sees the first.
  out["issuance order"] = await scenario(db, async (d, seen) => {
    await d.transaction(["meta"], "readwrite", "relaxed", (tx) => {
      const meta = tx.store("meta");
      meta.put("order", 1);
      meta.get("order", (value) => seen.push(value));
      meta.put("order", 2);
      meta.get("order", (value) => seen.push(value));
    });
  });
  // A callback's requests run after every request issued before it.
  out["callback order"] = await scenario(db, async (d, seen) => {
    await d.transaction(["meta"], "readwrite", "relaxed", (tx) => {
      const meta = tx.store("meta");
      meta.put("a", "a");
      meta.put("b", "b");
      meta.put("c", "c");
      meta.get("a", (value) => {
        seen.push(value);
        meta.get("c", (inner) => seen.push(inner));
      });
      meta.get("b", (value) => seen.push(value));
    });
  });
  // put() clones its value when called; every read returns its own copy.
  out["structured clone"] = await scenario(db, async (d, seen) => {
    const source = new Uint8Array([1, 2]);
    await d.transaction(["meta"], "readwrite", "relaxed", (tx) => {
      tx.store("meta").put("clone", source.buffer);
      source[0] = 9;
    });
    let first: unknown;
    await d.transaction(["meta"], "readonly", "relaxed", (tx) => {
      const meta = tx.store("meta");
      meta.get("clone", (value) => {
        first = value;
        if (value instanceof ArrayBuffer) new Uint8Array(value)[1] = 7;
      });
      meta.get("clone", (value) => {
        seen.push(bytesOf(value), value === first);
      });
    });
  });
  // A store used after its transaction completed refuses requests.
  out["inactive after completion"] = await scenario(db, async (d, seen) => {
    let kept: IdbStoreLike | null = null;
    await d.transaction(["meta"], "readwrite", "relaxed", (tx) => {
      kept = tx.store("meta");
    });
    try {
      (kept as IdbStoreLike | null)?.put("late", 1);
      seen.push("accepted");
    } catch (error) {
      seen.push(errorName(error));
    }
  });
  // Abort undoes everything the transaction did.
  out.abort = await scenario(db, async (d, seen) => {
    try {
      await d.transaction(["meta"], "readwrite", "relaxed", (tx) => {
        const meta = tx.store("meta");
        meta.put("aborted", 1);
        meta.get("aborted", () => {
          tx.abort(Object.assign(new Error("stop"), { name: "StopError" }));
        });
      });
    } catch (error) {
      seen.push(errorName(error));
    }
    await d.transaction(["meta"], "readonly", "relaxed", (tx) => {
      tx.store("meta").get("aborted", (value) => seen.push(value ?? "absent"));
    });
  });
  // An empty key range is refused (DataError) and fails the transaction.
  out["empty range"] = await scenario(db, async (d) => {
    await d.transaction(["data"], "readonly", "relaxed", (tx) => {
      tx.store("data").getAll({ lower: 5, upper: 5, upperOpen: true }, () => undefined);
    });
  });
  // A read-only transaction refuses writes (ReadOnlyError).
  out["read-only"] = await scenario(db, async (d) => {
    await d.transaction(["meta"], "readonly", "relaxed", (tx) => {
      tx.store("meta").put("ro", 1);
    });
  });
  // Numeric keys in order; ranges and `last`; deleteRange seen by later requests.
  out.ranges = await scenario(db, async (d, seen) => {
    await d.transaction(["data"], "readwrite", "relaxed", (tx) => {
      const data = tx.store("data");
      for (const key of [30, 2, 10, 40]) data.put(key, key);
      data.getAll({ lower: 2, upper: 30, upperOpen: true }, (values) => seen.push(values));
      data.last({ upper: 20 }, (value) => seen.push(value));
      data.last(null, (value) => seen.push(value));
      data.deleteRange({ lower: 10 });
      data.getAll({}, (values) => seen.push(values));
      data.last({ upper: 1 }, (value) => seen.push(value ?? "none"));
    });
  });
  return out;
}

/** The outcomes IndexedDB gives (the browser spec checks real IndexedDB against them too). */
export const EXPECTED_IDB_OUTCOMES: Record<string, ScenarioOutcome> = {
  "issuance order": { seen: [1, 2], error: null },
  "callback order": { seen: ["a", "b", "c"], error: null },
  "structured clone": { seen: [[1, 2], false], error: null },
  "inactive after completion": { seen: ["TransactionInactiveError"], error: null },
  abort: { seen: ["StopError", "absent"], error: null },
  "empty range": { seen: [], error: "DataError" },
  "read-only": { seen: [], error: "ReadOnlyError" },
  ranges: { seen: [[2, 10], 10, 40, [2], "none"], error: null },
};
