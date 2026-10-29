/**
 * Calibration for the live publication checks: each law rejects a delivery that breaks it, and a
 * well-formed stream passes. A checker that cannot reject a wrong answer proves nothing when the
 * live oracles report it clean.
 */
import { describe, expect, it } from "vitest";
import type { LiveQueryDelivery } from "../engine/live.js";
import type { QueryResult, QueryRow } from "../plan/model.js";
import { LivePatchReplica, LivePublicationLedger } from "./live-publication.js";
import type { LivePublicationState } from "./live-publication.js";
import { createReach } from "./reach.js";

const result = (rows: QueryRow[]): QueryResult => ({
  columns: ["id", "x"],
  columnDomains: [null, null],
  rows,
});
const delivery = (
  initial: boolean,
  manifestVersion: number | null,
  retained?: number[],
): LiveQueryDelivery => ({
  initial,
  manifestVersion,
  catalogEpoch: 0,
  ...(retained === undefined ? {} : { retained: Int32Array.from(retained) }),
});

const a = { id: 1, x: 10 };
const b = { id: 2, x: 20 };
const c = { id: 3, x: 30 };

describe("live publication ledger", () => {
  it("accepts a well-formed stream and reports what each delivery did", () => {
    const seen = new Set<LivePublicationState>();
    const ledger = new LivePublicationLedger("t", (state) => seen.add(state));
    ledger.change(result([a, b]), delivery(true, null));
    ledger.change(result([b, c]), delivery(false, 3, [1, -1]));
    ledger.change(result([c]), delivery(false, 4, [1]));
    ledger.change(result([c]), delivery(false, 4));
    expect(ledger.violations).toEqual([]);
    expect(ledger.latest()?.rows).toEqual([c]);
    expect([...seen].sort()).toEqual(["moved", "patched", "replaced", "rerun", "shrunk"]);
  });

  it.each([
    ["a first delivery that is not initial", [[[a], delivery(false, 1)]], "initial is false"],
    [
      "a later delivery marked initial",
      [
        [[a], delivery(true, 1)],
        [[a], delivery(true, 2)],
      ],
      "initial is true",
    ],
    ["retained on the first delivery", [[[a], delivery(true, 1, [0])]], "first delivery"],
    [
      "a manifest version going back",
      [
        [[a], delivery(true, 5)],
        [[a], delivery(false, 4)],
      ],
      "went back",
    ],
    [
      "retained of the wrong length",
      [
        [[a, b], delivery(true, 1)],
        [[a, b], delivery(false, 2, [0])],
      ],
      "1 entries for 2 rows",
    ],
    [
      "a retained row whose values changed",
      [
        [[a, b], delivery(true, 1)],
        [[a, { id: 2, x: 21 }], delivery(false, 2, [0, 1])],
      ],
      "but changed",
    ],
    [
      "a retained row that gained a column",
      [
        [[a], delivery(true, 1)],
        [[{ ...a, y: null }], delivery(false, 2, [0])],
      ],
      "but changed",
    ],
    [
      "a previous row retained twice",
      [
        [[a, b], delivery(true, 1)],
        [[a, a], delivery(false, 2, [0, 0])],
      ],
      "retained twice",
    ],
    [
      "a retained index past the previous rows",
      [
        [[a], delivery(true, 1)],
        [[a, b], delivery(false, 2, [0, 1])],
      ],
      "past the previous",
    ],
    [
      "a negative position other than -1",
      [
        [[a], delivery(true, 1)],
        [[a], delivery(false, 2, [-100])],
      ],
      "retained as -100",
    ],
  ] as const)("rejects %s", (_name, stream, message) => {
    const ledger = new LivePublicationLedger("t");
    for (const [rows, sent] of stream) ledger.change(result([...rows]), sent);
    expect(ledger.violations.join("\n")).toContain(message);
  });
});

describe("live patch replica", () => {
  const reset = (rows: QueryRow[]) => ({ type: "reset" as const, result: result(rows) });
  const patch = (retained: number[], changedRows: Array<{ index: number; row: QueryRow }>) => ({
    type: "patch" as const,
    retained: Int32Array.from(retained),
    changedRows,
  });

  it("rebuilds rows from a reset and its patches", () => {
    const replica = new LivePatchReplica("t");
    replica.apply(reset([a, b]));
    replica.apply(patch([1, -1], [{ index: 1, row: c }]));
    replica.apply(patch([], []));
    replica.apply(patch([-1], [{ index: 0, row: a }]));
    expect(replica.violations).toEqual([]);
    expect(replica.rows()).toEqual([a]);
  });

  it.each([
    ["a patch before any reset", [patch([], [])], "before any reset"],
    ["a retained position past the rows", [reset([a]), patch([1], [])], "retains 1 of 1"],
    ["a position retained twice", [reset([a]), patch([0, 0], [])], "a second time"],
    ["a -1 position without its row", [reset([a]), patch([0, -1], [])], "no changed row"],
    [
      "a changed row at a retained position",
      [reset([a]), patch([0], [{ index: 0, row: b }])],
      "not a -1 position",
    ],
    [
      "changed rows out of order",
      [
        reset([]),
        patch(
          [-1, -1],
          [
            { index: 1, row: b },
            { index: 0, row: a },
          ],
        ),
      ],
      "out of order",
    ],
  ] as const)("rejects %s", (_name, stream, message) => {
    const replica = new LivePatchReplica("t");
    for (const item of stream) replica.apply(item);
    expect(replica.violations.join("\n")).toContain(message);
  });
});

describe("declared reach", () => {
  it("fails for a state too few histories entered, counting each history once", () => {
    const reach = createReach("calibration", ["often", "once", "never"], { minimum: 2 });
    for (let history = 0; history < 3; history += 1) {
      reach.beginHistory();
      reach.reach("often");
      reach.reach("often");
      if (history === 0) reach.reach("once");
    }
    expect(reach.counts().get("often")).toBe(3);
    expect(() => reach.assertReached()).toThrow(
      "of 3 generated histories, fewer than 2 reached these states, so their laws were not enforced there: once (1), never (0)",
    );
  });

  it("refuses an undeclared state and stands aside when no history ran", () => {
    const reach = createReach("calibration", ["declared"]);
    expect(() => reach.assertReached()).not.toThrow();
    reach.beginHistory();
    expect(() => reach.reach("other" as "declared")).toThrow("undeclared reach state other");
  });
});
