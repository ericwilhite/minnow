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
    const child = statement({
      kind: "insert",
      table: "c",
      newKeys: [1],
      references: [{ table: "t", value: 51 }],
    });
    const fk = { outcome: "foreignKey" as const, message: "violates foreign key constraint" };
    expect(id(observation({ statements: [child], authority: fk }))).toBe("key-forwarding");
    expect(
      id(
        observation({
          statements: [
            statement({
              kind: "insert",
              table: "c",
              newKeys: [1],
              references: [{ table: "t", value: 52 }],
            }),
          ],
          authority: fk,
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
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51] })],
          rowsDiffer: true,
        }),
      ),
    ).toBe("key-forwarding");
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [52] })],
          rowsDiffer: true,
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
