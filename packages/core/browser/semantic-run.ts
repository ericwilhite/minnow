import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { deleteOpfsDatabase } from "@minnowdb/core/storage/opfs";
import {
  semanticCorpus,
  semanticOutcome,
  semanticFingerprint,
} from "../src/testing/semantic-corpus.js";

export async function runSemanticEquivalence(kind: "indexeddb" | "opfs") {
  const worker = new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  const name = `semantics-${crypto.randomUUID()}`;
  const client = new MinnowDatabaseClient(worker, {
    store: { kind, name },
    databaseOptions: { autoCompact: false, autoCollect: false },
  });
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    autoCompact: false,
    autoCollect: false,
  });
  let compared = 0;
  let result: { compared: number; failures: number } | undefined;
  const failures: unknown[] = [];
  try {
    for (const db of [database, client]) {
      await db.execute("CREATE TABLE semantic_rows (id INTEGER)");
      await db.execute("INSERT INTO semantic_rows VALUES (1),(2)");
    }
    for (const testCase of semanticCorpus()) {
      const options = testCase.params === undefined ? {} : { params: testCase.params };
      const expected = await semanticOutcome(() =>
        database.query(testCase.sql, { ...options, memoize: false }),
      );
      for (const memoize of [false, true]) {
        const actual = await semanticOutcome(() =>
          client.query(testCase.sql, { ...options, memoize }),
        );
        if (semanticFingerprint(actual) !== semanticFingerprint(expected))
          throw new Error(`Semantic worker drift: ${testCase.sql}`);
        compared += 1;
      }
    }
    result = { compared, failures: (await client.maintenanceStatus()).backgroundFailureCount };
  } catch (error) {
    failures.push(error);
  } finally {
    const closes = await Promise.allSettled([client.close(), database.close()]);
    worker.terminate();
    for (const result of closes) if (result.status === "rejected") failures.push(result.reason);
    try {
      if (kind === "opfs") await deleteOpfsDatabase({ name });
      else
        await new Promise<void>((resolve, reject) => {
          const request = indexedDB.deleteDatabase(name);
          request.onsuccess = () => resolve();
          request.onerror = () =>
            reject(request.error ?? new Error("Semantic database deletion failed"));
          request.onblocked = () => reject(new Error("Semantic database deletion blocked"));
        });
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length > 0)
    throw new AggregateError(failures, "Semantic comparison or cleanup failed");
  if (result === undefined) throw new Error("Semantic comparison result is missing");
  return result;
}
