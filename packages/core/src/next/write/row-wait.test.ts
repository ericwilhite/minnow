/**
 * A write step that relief cannot fund waits in the governor beside its statement's holdings
 * (`#waitForRows`), and is served once another holder releases: never refused part way.
 *
 * Before (#56 at 9ddbec1be) the memtable charge refused here ("write path memtable needs a
 * minimum above the cap … 0 available"), though the step fits once the other holder releases.
 * Through SQL the shape needs a statement admitted beside another connection's read whose
 * memory then runs out under its rows (multi-row-beside-read.test.ts keeps that law); here the
 * pressure is placed directly: the writer's idle credit is allocated, and another holder takes
 * all free memory after the statement starts.
 *
 * Law (GOVERNOR-WAIT-POLICY point 5, next/memory/README.md): a statement that fits once a finite
 * holder releases waits and is served; its waits end with its cancellation; PostgreSQL statement
 * atomicity for a cancelled statement.
 */
import { expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  rules,
  tableInput,
  TEXT,
  texts,
} from "./test-support.js";
import type { WritePathImpl } from "./engine.js";

const MiB = 1 << 20;
const PENDING = Symbol("pending");
const within = <T>(work: Promise<T>, ms: number) =>
  Promise.race([
    work,
    new Promise<typeof PENDING>((resolve) => {
      setTimeout(() => {
        resolve(PENDING);
      }, ms);
    }),
  ]);

for (const cancelled of [false, true])
  it(`a write step waits for rows beside another holder, then is ${cancelled ? "cancelled" : "written"}`, async () => {
    const h = new Harness("strict", "memory", 2);
    const governor = new Governor(32 * MiB);
    Object.defineProperty(h, "governor", { value: governor });
    const path: WritePathImpl = await h.open();
    const undo: Array<() => void> = [];
    let other: Grant | null = null;
    try {
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
            })
          ).id ?? 0;
      });
      const tx = await path.begin();
      const cancel = new AbortController();
      const statement = tx.statement({ rules, signal: cancel.signal });
      try {
        // What the statement keeps while it waits: the writer's (an executing query registers
        // its own grants too).
        statement.waitHoldings = () => ({ grants: [], lent: path.writerHoldings() });
        // No credit of the writer's own to fund rows from (its row, commit and merge accounts;
        // its metadata step stays prepaid), and no room for them: another holder (finite: it
        // releases below) takes the free memory.
        const [rows, , commits, merge] = path.writerHoldings().grants;
        for (const grant of [rows, commits, merge]) {
          if (grant === undefined) continue;
          const idle = grant.held - grant.allocated - grant.loaned;
          if (idle > 0 && grant.allocate(idle)) undo.push(() => grant.free(idle));
        }
        // It leaves 64 KiB: enough for the step's metadata (its rollback pin), not for its rows.
        const free = governor.capBytes - governor.reserveBytes - governor.usedBytes - 65536;
        other = governor.tryAdmit({
          label: "another holder",
          minimumBytes: free,
          spillable: false,
        });
        if (other === null) throw new Error("the other holder");
        const waitsBefore = path.maintenance.rowWaits;
        const ids = [1, 2, 3, 4];
        const write = Promise.resolve(
          statement.table(t).insert(batch(ints(ids), texts(ids.map(() => "w".repeat(MiB)))), null),
        ).then(
          () => "written" as const,
          (error: unknown) => error,
        );
        expect(await within(write, 500), "it waits beside the other holder").toBe(PENDING);
        expect(path.maintenance.rowWaits - waitsBefore, "in the governor, for its rows").toBe(1);
        if (cancelled) {
          cancel.abort(new Error("cancelled by the test"));
          const outcome = await within(write, 1_000);
          expect(outcome, "cancellation ends the wait").not.toBe(PENDING);
          expect(outcome).not.toBe("written");
          statement.rollback();
          tx.rollback();
        } else {
          other.close();
          other = null;
          for (const step of undo.splice(0)) step();
          expect(await within(write, 5_000), "served once the other holder releases").toBe(
            "written",
          );
          await statement.end();
          await tx.commit(null);
        }
      } finally {
        other?.close();
        for (const step of undo.splice(0)) step();
        statement.rollback();
        tx.rollback();
      }
      const view = path.pin();
      try {
        expect((await rowsOf(view, t)).map((row) => row[0])).toEqual(cancelled ? [] : [1, 2, 3, 4]);
      } finally {
        view.release();
      }
      expect(path.failure).toBeNull();
    } finally {
      other?.close();
      for (const step of undo) step();
      await h.close();
    }
  }, 60_000);
