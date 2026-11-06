/**
 * The R21 registry's matchers (review B3-p2a-2 #4): a registered divergence matches only when its
 * cause explains the discrepancy, never merely because a relevant value appears in the step.
 */
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import {
  intendedDivergence,
  type DifferingRow,
  type DivergenceObservation,
  type RowSide,
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

/** A child row (id, ref → t, note) as one side holds it. */
function child(key: number, ref: number | null, note: unknown = "n"): RowSide {
  return { values: [key, ref, note], references: [{ column: 1, table: "t", value: ref }] };
}

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
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51], assigned: [1] })],
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
      statement: 0,
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
    /** A parent t(id, v) updated through the forward (v 2 -> 1), or deleted through it. */
    const parentRow = (key: number): DifferingRow => ({
      table: "t",
      key,
      oracle: { values: [key, 1], references: [] },
      authority: { values: [key, 2], references: [] },
    });
    const deletedParent = (key: number): DifferingRow => ({
      table: "t",
      key,
      oracle: null,
      authority: { values: [key, 2], references: [] },
    });
    /** A child whose PostgreSQL version references `before` and the current engine's `after`. */
    const childRow = (
      key: number,
      before: number,
      after: number | null | "absent",
    ): DifferingRow => ({
      table: "c",
      key,
      authority: child(key, before),
      oracle: after === "absent" ? null : child(key, after),
    });
    const differ = (...rows: DifferingRow[]) => ({ rowsDiffer: true, differingRows: rows });
    // The UPDATE through 51 reached row 99 (where 51 moved): exactly that row differs.
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51], assigned: [1] })],
          ...differ(parentRow(99)),
        }),
      ),
    ).toBe("key-forwarding");
    // A child of the reached row may differ too (a cascade through it).
    expect(
      id(
        observation({
          statements: [statement({ kind: "delete", table: "t", matchedKeys: [51] })],
          ...differ(deletedParent(99), childRow(4, 99, "absent")),
        }),
      ),
    ).toBe("key-forwarding");
    // Any other differing row is not explained, even beside the forward.
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51], assigned: [1] })],
          ...differ(parentRow(99), parentRow(7)),
        }),
      ),
    ).toBeNull();
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51], assigned: [1] })],
          rowsDiffer: true,
        }),
      ),
    ).toBeNull();
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [52], assigned: [1] })],
          ...differ(parentRow(99)),
        }),
      ),
    ).toBeNull();
    expect(
      id(
        observation({
          statements: [statement({ kind: "update", table: "t", matchedKeys: [51], assigned: [1] })],
          rowsDiffer: false,
        }),
      ),
    ).toBeNull();
    // A DELETE through the forward may set the child's reference to NULL (SET NULL); a rekey
    // moves it to the new key, and only there.
    const del = statement({ kind: "delete", table: "t", matchedKeys: [51] });
    const rekey = statement({ kind: "rekey", table: "t", matchedKeys: [51], newKeys: [60] });
    expect(id(observation({ statements: [del], ...differ(childRow(4, 99, null)) }))).toBe(
      "key-forwarding",
    );
    const moved = (key: number): DifferingRow => ({
      table: "t",
      key,
      oracle: { values: [key, 2], references: [] },
      authority: null,
    });
    expect(
      id(
        observation({
          statements: [rekey],
          ...differ(deletedParent(99), moved(60), childRow(4, 99, 60)),
        }),
      ),
    ).toBe("key-forwarding");
    expect(id(observation({ statements: [rekey], ...differ(childRow(4, 99, 61)) }))).toBeNull();
    expect(id(observation({ statements: [del], ...differ(childRow(4, 98, "absent")) }))).toBeNull();
  });

  it("does not excuse a child's disappearance after a non-key UPDATE of a forwarded parent (review B3-p2a-4 #2)", () => {
    // 51 forwards to 99; UPDATE t SET v = 7 WHERE id = 51 cannot delete child 123 or change it.
    const update = statement({
      sql: "UPDATE t SET v=7 WHERE id=51",
      kind: "update",
      table: "t",
      matchedKeys: [51],
    });
    for (const after of ["absent", null, 7] as const)
      expect(
        id(
          observation({
            statements: [update],
            rowsDiffer: true,
            differingRows: [
              {
                table: "c",
                key: 123,
                authority: child(123, 99),
                oracle: after === "absent" ? null : child(123, after),
              },
            ],
          }),
        ),
      ).toBeNull();
  });

  it("does not excuse a RESTRICT refusal of another statement (review B3-p2a-4 #1)", () => {
    // Statement 0 deletes 999 (never forwarded, no children); statement 1 deletes forwarding 51.
    // PostgreSQL accepts both; a defect refusing statement 0 is not forwarding.
    const statements = [
      statement({
        sql: "DELETE FROM t WHERE id=999",
        kind: "delete",
        table: "t",
        matchedKeys: [999],
      }),
      statement({
        sql: "DELETE FROM t WHERE id=51",
        kind: "delete",
        table: "t",
        matchedKeys: [51],
      }),
    ];
    const refused = (at: number | null) => ({
      outcome: "foreignKey" as const,
      message: "FOREIGN KEY restrict",
      statement: at,
    });
    expect(id(observation({ statements, oracle: refused(0) }))).toBeNull();
    expect(id(observation({ statements, oracle: refused(null) }))).toBeNull();
    expect(id(observation({ statements, oracle: refused(1) }))).toBe("key-forwarding");
  });

  it("one explained reference does not excuse another changed reference (review B3-p2b-2 A1)", () => {
    // DELETE through 51 (forwarding to 99): child 123's reference to t may become NULL; its
    // reference to `other` (7 → 999) is not the forward's doing.
    const side = (t: number | null, other: number): RowSide => ({
      values: [123, t, other],
      references: [
        { column: 1, table: "t", value: t },
        { column: 2, table: "other", value: other },
      ],
    });
    const del = statement({ kind: "delete", table: "t", matchedKeys: [51] });
    const row = (after: RowSide): DifferingRow => ({
      table: "c",
      key: 123,
      authority: side(99, 7),
      oracle: after,
    });
    expect(
      id(
        observation({ statements: [del], rowsDiffer: true, differingRows: [row(side(null, 999))] }),
      ),
    ).toBeNull();
    expect(
      id(observation({ statements: [del], rowsDiffer: true, differingRows: [row(side(null, 7))] })),
    ).toBe("key-forwarding");
  });

  const SEEDS = seedsFor("next-testing-forwarding-matcher", [801, 802, 803, 804]);
  for (const seed of SEEDS)
    it(`a child difference matches only when every changed field is the forward's (seed ${String(seed)})`, () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      let positives = 0;
      let negatives = 0;
      for (let k = 0; k < 300; k++) {
        const kind = random() < 0.5 ? "delete" : "rekey";
        const target = 99;
        const before = child(int(1000), target, `v${String(int(5))}`);
        // The forward's own effect on the child (SET NULL, or follow to the rekey's new key).
        let after: RowSide = child(
          Number(before.values[0]),
          kind === "delete" ? null : 60,
          before.values[2],
        );
        // Sometimes also an unrelated change: the note, or the reference to something else.
        const unrelated = random() < 0.5;
        if (unrelated) {
          after =
            random() < 0.5
              ? child(Number(after.values[0]), after.references[0]?.value ?? null, "changed")
              : child(Number(after.values[0]), kind === "delete" ? 7 : 61, after.values[2]);
        }
        const matched = id(
          observation({
            statements: [
              statement({
                kind,
                table: "t",
                matchedKeys: [51],
                newKeys: kind === "rekey" ? [60] : [],
              }),
            ],
            rowsDiffer: true,
            differingRows: [
              { table: "c", key: Number(before.values[0]), authority: before, oracle: after },
            ],
          }),
        );
        expect(matched, `seed ${String(seed)} case ${String(k)}`).toBe(
          unrelated ? null : "key-forwarding",
        );
        if (unrelated) negatives += 1;
        else positives += 1;
      }
      expect(positives).toBeGreaterThan(50);
      expect(negatives).toBeGreaterThan(50);
    });

  it("a forwarded DELETE does not explain a retained parent with a changed value (review B3-p2b-3 A2)", () => {
    expect(
      id(
        observation({
          statements: [
            statement({
              sql: "DELETE FROM t WHERE id=51",
              kind: "delete",
              table: "t",
              matchedKeys: [51],
            }),
          ],
          rowsDiffer: true,
          differingRows: [
            {
              table: "t",
              key: 99,
              authority: { values: [99, "correct"], references: [] },
              oracle: { values: [99, "corrupted"], references: [] },
            },
          ],
        }),
      ),
    ).toBeNull();
  });

  const PARENT_SEEDS = seedsFor("next-testing-forwarding-parent", [811, 812, 813, 814]);
  for (const seed of PARENT_SEEDS)
    it(`a parent difference matches only the effect its operation has (seed ${String(seed)})`, () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      let positives = 0;
      let negatives = 0;
      for (let k = 0; k < 300; k++) {
        // t(id, a, b): 51 forwards to 99; the statement goes through 51.
        const original: unknown[] = [99, int(10), `b${String(int(5))}`];
        const kind = (["delete", "update", "rekey"] as const)[int(3)] ?? "delete";
        const assignedColumn = 1 + int(2); // an UPDATE assigns a or b
        const unassigned = assignedColumn === 1 ? 2 : 1;
        const kept = (after: unknown[]): DifferingRow => ({
          table: "t",
          key: 99,
          oracle: { values: after, references: [] },
          authority: { values: original, references: [] },
        });
        const gone: DifferingRow = {
          table: "t",
          key: 99,
          oracle: null,
          authority: { values: original, references: [] },
        };
        const arrived = (after: unknown[]): DifferingRow => ({
          table: "t",
          key: 60,
          oracle: { values: after, references: [] },
          authority: null,
        });
        const wrong = random() < 0.5;
        let rows: DifferingRow[];
        if (kind === "delete")
          // Wrong: the row kept, a value changed.
          rows = wrong ? [kept([99, "x", original[2]])] : [gone];
        else if (kind === "update") {
          const after = [...original];
          after[assignedColumn] = "set";
          // Wrong: a column the UPDATE does not assign changed too.
          if (wrong) after[unassigned] = "unassigned";
          rows = [kept(after)];
        } else
          // Wrong: the moved row lost a value on the way.
          rows = [gone, arrived(wrong ? [60, "lost", original[2]] : [60, ...original.slice(1)])];
        const matched = id(
          observation({
            statements: [
              statement({
                kind,
                table: "t",
                matchedKeys: [51],
                newKeys: kind === "rekey" ? [60] : [],
                ...(kind === "update" ? { assigned: [assignedColumn] } : {}),
              }),
            ],
            rowsDiffer: true,
            differingRows: rows,
          }),
        );
        expect(
          matched,
          `seed ${String(seed)} case ${String(k)} (${kind}, wrong ${String(wrong)})`,
        ).toBe(wrong ? null : "key-forwarding");
        if (wrong) negatives += 1;
        else positives += 1;
      }
      expect(positives).toBeGreaterThan(50);
      expect(negatives).toBeGreaterThan(50);
    });
});
