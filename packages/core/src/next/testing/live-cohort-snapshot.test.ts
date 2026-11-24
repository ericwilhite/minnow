/**
 * A live sweep's cohort reads one committed state, and every plan matches its snapshot's catalog,
 * even when a commit or DDL lands while the sweep runs.
 *
 * Neon's review of #59 (f361faf1): a read-only transaction now takes its snapshot at its first
 * statement, so `begin(true)` no longer pinned one. `LiveRuntime.#sweep` relied on it ("No
 * await between these pins: every session captures the same committed state"): it planned every
 * entry against the unpinned catalog, and each session pinned lazily when its worker reached
 * `run`, after awaits. With more than eight queries (the worker cap), a commit between the first
 * eight runs and the rest split the cohort (LSN 2 and 3 delivered together), and a DROP TABLE
 * failed the late entries ("The plan was lowered at catalog version 1, the snapshot is at 4").
 * The sweep now pins each session (`EngineSession.pinSnapshot`) before it plans.
 *
 * Law: live/runtime.ts's contract: "All affected queries publish in one synchronous turn" over
 * "the same committed state"; a plan executes at the catalog it was lowered at.
 */
import { expect, it } from "vitest";
import type { EngineSession } from "../owner/engine.js";
import { setup, until } from "./live-runtime.test-support.js";

const COHORT = 10;

/** The fixture with the first `held` live runs held at a gate the test opens. */
async function gated(held: number) {
  let waiting = 0;
  let open!: () => void;
  const gate = new Promise<void>((resolve) => {
    open = resolve;
  });
  const active = await setup(undefined, (engine) => {
    const openSession = engine.openSession.bind(engine);
    engine.openSession = (clientId, generation, sessionId) => {
      const session: EngineSession = openSession(clientId, generation, sessionId);
      if (clientId !== "live") return session;
      const run = session.run.bind(session);
      session.run = (statement, params, context) => {
        // Runs at once (a lazily pinned session pins here), then holds its first frame, as a
        // read's I/O would: the worker stays busy until the gate opens.
        const started = run(statement, params, context);
        if (waiting >= held) return started;
        waiting++;
        const results = started.results;
        let first = true;
        return {
          ...started,
          results: {
            ...results,
            next: () => {
              if (!first) return results.next();
              first = false;
              return gate.then(() => results.next());
            },
          },
        };
      };
      return session;
    };
  });
  return { active, waiting: () => waiting, open: () => open() };
}

it("delivers one committed state to a cohort larger than the worker cap, across a commit", async () => {
  const { active, waiting, open } = await gated(8);
  const { client } = active;
  try {
    await client.query("CREATE TABLE t (id integer PRIMARY KEY, v integer)");
    await client.query("INSERT INTO t VALUES (1, 10)");
    const first: Array<number | null> = Array<number | null>(COHORT).fill(null);
    const errors: Error[] = [];
    const handles = first.map((_, k) =>
      client.subscribeLive("SELECT count(*) AS n FROM t", [], {
        onResult(result) {
          first[k] ??= Number(result.rows()[0]?.[0]);
        },
        onError(error) {
          errors.push(error);
        },
      }),
    );
    // The first eight runs have started and are held: the cohort's last two have not run yet.
    await until(() => waiting() >= 8);
    await client.query("INSERT INTO t VALUES (2, 20)");
    open();
    await until(() => first.every((n) => n !== null) || errors.length > 0);
    expect(errors).toEqual([]);
    expect(first, "every query of the cohort saw the same committed state").toEqual(
      Array<number>(COHORT).fill(1),
    );
    for (const handle of handles) handle.close();
  } finally {
    open();
    await active.close();
  }
}, 60_000);

it("runs each cohort plan at its own catalog when DDL commits mid-sweep", async () => {
  const { active, waiting, open } = await gated(8);
  const { client } = active;
  try {
    await client.query("CREATE TABLE t (id integer PRIMARY KEY, v integer)");
    await client.query("CREATE TABLE other (id integer PRIMARY KEY)");
    await client.query("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    const first: Array<number | null> = Array<number | null>(COHORT).fill(null);
    const errors: Error[] = [];
    const handles = first.map((_, k) =>
      client.subscribeLive("SELECT count(*) AS n FROM t", [], {
        onResult(result) {
          first[k] ??= Number(result.rows()[0]?.[0]);
        },
        onError(error) {
          errors.push(error);
        },
      }),
    );
    await until(() => waiting() >= 8);
    await client.query("DROP TABLE other");
    open();
    await until(() => first.every((n) => n !== null) || errors.length > 0);
    expect(errors.map((error) => error.message)).toEqual([]);
    expect(first).toEqual(Array<number>(COHORT).fill(3));
    for (const handle of handles) handle.close();
  } finally {
    open();
    await active.close();
  }
}, 60_000);
