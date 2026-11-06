/**
 * The R21 registry's matchers (review B3-p2a-2 #4): a registered divergence matches only when its
 * cause explains the discrepancy, never merely because a relevant value appears in the step.
 */
import { describe, expect, it } from "vitest";
import {
  intendedDivergence,
  type DivergenceObservation,
  type StatementFacts,
} from "./divergences.js";

const DAY = 24 * 60 * 60 * 1000;
const facts = (rekeyedAt = 99) => ({
  nowMs: 100,
  primaryKeys: new Map([
    ["t", "id"],
    ["c", "id"],
  ]),
  rekeyedAway: new Map([["t", new Map([[51, rekeyedAt]])]]),
  movedTo: new Map([["t", new Map([[51, 99]])]]),
});

const statement = (
  facts_: Partial<StatementFacts> & Pick<StatementFacts, "kind" | "table">,
): StatementFacts => ({
  sql: "",
  newKeys: [],
  matchedKeys: [],
  references: [],
  ...facts_,
});

const observation = (part: Partial<DivergenceObservation>): DivergenceObservation => ({
  statements: [],
  oracle: { outcome: "ok", message: null },
  authority: { outcome: "ok", message: null },
  rowsDiffer: false,
  facts: facts(),
  ...part,
});

const id = (o: DivergenceObservation) => intendedDivergence(o)?.id ?? null;

describe("key-forwarding matcher", () => {
  it("does not explain an unrelated duplicate while a parent key forwards (the reviewer's probe)", () => {
    expect(
      id(
        observation({
          statements: [
            statement({
              kind: "insert",
              table: "c",
              newKeys: [1],
              references: [{ table: "t", value: 51 }],
            }),
          ],
          oracle: { outcome: "ok", message: null },
          authority: {
            outcome: "unique",
            message: "duplicate key value violates unique constraint c_pkey",
          },
        }),
      ),
    ).toBeNull();
  });

  it("explains a refused INSERT or rekey onto the forwarding key, and nothing else of that shape", () => {
    const refused = { outcome: "unique" as const, message: "Duplicate value for t.id: 51" };
    expect(
      id(
        observation({
          statements: [statement({ kind: "insert", table: "t", newKeys: [51] })],
          oracle: refused,
        }),
      ),
    ).toBe("key-forwarding");
    expect(
      id(
        observation({
          statements: [statement({ kind: "rekey", table: "t", newKeys: [51], matchedKeys: [3] })],
          oracle: refused,
        }),
      ),
    ).toBe("key-forwarding");
    // The duplicate is another key, another column, or not written by the step.
    expect(
      id(
        observation({
          statements: [statement({ kind: "insert", table: "t", newKeys: [52] })],
          oracle: { outcome: "unique", message: "Duplicate value for t.id: 52" },
        }),
      ),
    ).toBeNull();
    expect(
      id(
        observation({
          statements: [statement({ kind: "insert", table: "t", newKeys: [51] })],
          oracle: { outcome: "unique", message: "Duplicate value for t.u: 51" },
        }),
      ),
    ).toBeNull();
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51] })],
          oracle: refused,
        }),
      ),
    ).toBeNull();
    // Out of the window: no forward, nothing to explain it.
    expect(
      id(
        observation({
          statements: [statement({ kind: "insert", table: "t", newKeys: [51] })],
          oracle: refused,
          facts: facts(100 - 11 * DAY),
        }),
      ),
    ).toBeNull();
  });

  it("explains a child write the oracle stored through a forward that PostgreSQL refused", () => {
    const child = (value: number, key = 1) =>
      statement({
        kind: "insert",
        table: "c",
        newKeys: [key],
        references: [{ table: "t", value }],
      });
    const missing = (value: number, at: number) => ({
      outcome: "foreignKey" as const,
      message:
        'insert or update on table "c" violates foreign key constraint "c_pid_fkey": ' +
        `Key (pid)=(${String(value)}) is not present in table "t".`,
      statement: at,
    });
    expect(id(observation({ statements: [child(51)], authority: missing(51, 0) }))).toBe(
      "key-forwarding",
    );
    expect(id(observation({ statements: [child(52)], authority: missing(52, 0) }))).toBeNull();
    // The refused key must be the forwarded one, and the refused statement the one writing it.
    expect(id(observation({ statements: [child(51)], authority: missing(52, 0) }))).toBeNull();
    expect(
      id(
        observation({
          statements: [child(7, 1), child(51, 2)],
          authority: { ...missing(51, 0) },
        }),
      ),
    ).toBeNull();
    // No refused statement or no DETAIL: no evidence, no match.
    expect(
      id(
        observation({ statements: [child(51)], authority: { ...missing(51, 0), statement: null } }),
      ),
    ).toBeNull();
    expect(
      id(
        observation({
          statements: [child(51)],
          authority: {
            outcome: "foreignKey",
            message: "violates foreign key constraint",
            statement: 0,
          },
        }),
      ),
    ).toBeNull();
  });

  it("does not explain an unrelated FK failure beside a forwarded reference (review B3-p2a-3 #4)", () => {
    // The step inserts child (1, 999), then child (2, 51); 51 forwards, 999 never existed.
    // PostgreSQL refuses the first statement for missing parent 999.
    expect(
      id(
        observation({
          statements: [
            statement({
              sql: "INSERT INTO c VALUES (1, 999)",
              kind: "insert",
              table: "c",
              newKeys: [1],
              references: [{ table: "t", value: 999 }],
            }),
            statement({
              sql: "INSERT INTO c VALUES (2, 51)",
              kind: "insert",
              table: "c",
              newKeys: [2],
              references: [{ table: "t", value: 51 }],
            }),
          ],
          authority: {
            outcome: "foreignKey",
            message:
              'insert or update on table "c" violates foreign key constraint "c_parent_fkey": ' +
              'Key (parent)=(999) is not present in table "t".',
            statement: 0,
          },
        }),
      ),
    ).toBeNull();
  });

  it("explains a RESTRICT refusal or changed rows only through a statement that matched the forward", () => {
    const restrict = {
      outcome: "foreignKey" as const,
      message: "FOREIGN KEY c_pid_fkey still has 1 c row(s)",
    };
    expect(
      id(
        observation({
          statements: [statement({ kind: "delete", table: "t", matchedKeys: [51] })],
          oracle: restrict,
        }),
      ),
    ).toBe("key-forwarding");
    expect(
      id(
        observation({
          statements: [statement({ kind: "insert", table: "t", newKeys: [51] })],
          oracle: restrict,
        }),
      ),
    ).toBeNull();
    const differ = (...rows: Array<[string, number, number?]>) => ({
      rowsDiffer: true,
      differingRows: rows.map(([table, key, parent]) => ({
        table,
        key,
        references: parent === undefined ? [] : [{ table: "t", value: parent }],
      })),
    });
    // The UPDATE through 51 reached row 99 (where 51 moved): exactly that row differs.
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51] })],
          ...differ(["t", 99]),
        }),
      ),
    ).toBe("key-forwarding");
    // A child of the reached row may differ too (a cascade through it).
    expect(
      id(
        observation({
          statements: [statement({ kind: "delete", table: "t", matchedKeys: [51] })],
          ...differ(["t", 99], ["c", 4, 99]),
        }),
      ),
    ).toBe("key-forwarding");
    // Any other differing row is not explained, even beside the forward.
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51] })],
          ...differ(["t", 99], ["t", 7]),
        }),
      ),
    ).toBeNull();
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51] })],
          rowsDiffer: true,
        }),
      ),
    ).toBeNull();
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [52] })],
          ...differ(["t", 99]),
        }),
      ),
    ).toBeNull();
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51] })],
          rowsDiffer: false,
        }),
      ),
    ).toBeNull();
  });
});
