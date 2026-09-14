import { describe, expect, it } from "vitest";
import rawProfile from "../postgres-feature-profile.json";
import rawMatrix from "../sql-feature-matrix.json";
import { featureBehaviorProbes } from "./sql-feature-probes.js";

const expectedFeatureIds = [
  "aggregate.any-value",
  "mutation.insert-default-values",
  "mutation.insert-runtime-values",
  "mutation.truncate",
  "mutation.upsert-replace",
  "mutation.upsert-expression",
  "trigger.create-after",
  "trigger.create-before",
  "trigger.body-update-delete",
  "trigger.drop",
  "ddl.create-table-default",
  "ddl.generated-column",
  "ddl.unique-secondary-index",
  "ddl.check-constraint",
  "ddl.foreign-key",
  "ddl.create-view",
  "ddl.sequence",
  "ddl.serial",
  "ddl.identity",
  "ddl.alter-table-add-column-default",
  "transaction.begin",
  "transaction.end",
  "transaction.abort",
  "transaction.commit",
  "transaction.rollback",
  "transaction.savepoint",
  "transaction.session-settings",
  "transaction.show-setting",
  "expression.arithmetic",
  "expression.round",
  "parameter.positional",
  "predicate.match",
  "predicate.match-star",
  "predicate.match-parameter",
  "function.bm25",
  "subquery.correlated-json-aggregate",
  "expression.modulo",
  "function.numeric-core",
  "function.string-extended",
  "function.trim-multi-character",
  "literal.scientific",
  "json.query",
  "json.object",
  "json.array",
  "json.arrow",
  "json.arrow-untyped",
  "type.exact-numeric",
  "type.json-jsonb",
  "type.interval",
  "aggregate.json",
  "aggregate.array-agg",
  "type.array",
  "type.date",
  "function.to-date-timestamp",
  "function.make-date",
  "function.age",
  "ddl.create-table",
  "ddl.type-spellings",
  "ddl.temporary-table",
  "ddl.named-column-constraint",
  "ddl.create-table-if-not-exists",
  "ddl.create-table-key-clause",
  "ddl.alter-table-add-column",
  "ddl.create-table-as-select",
  "ddl.enum",
  "ddl.secondary-index",
  "ddl.drop-secondary-index",
  "ddl.composite-secondary-index",
  "ddl.multiple-unique-constraints",
  "ddl.composite-primary-key",
  "ddl.composite-foreign-key",
  "ddl.alter-table-drop-column",
  "ddl.drop-table",
  "ddl.drop-view",
] as const;

describe("browser SQL feature behavior probes", () => {
  it("keeps the independently authored feature and step populations fixed", () => {
    expect(featureBehaviorProbes.map(({ featureId }) => featureId)).toEqual(expectedFeatureIds);
    expect(new Set(expectedFeatureIds).size).toBe(expectedFeatureIds.length);
    expect(featureBehaviorProbes.reduce((total, probe) => total + probe.steps.length, 0)).toBe(137);
    expect(
      featureBehaviorProbes
        .flatMap(({ steps }) => steps)
        .filter((step) => step.kind === "query" && step.compareWithPglite === true),
    ).toHaveLength(2);
    expect(
      featureBehaviorProbes.flatMap(({ featureId, steps }) =>
        steps.flatMap((step) =>
          step.kind === "query" && step.throughExecute === true
            ? [[featureId, step.sql] as const]
            : [],
        ),
      ),
    ).toEqual([
      ["transaction.session-settings", "SHOW search_path"],
      ["transaction.show-setting", "SHOW server_version"],
    ]);
  });

  it("names supported matrix entries and covers every trigger and transaction example", () => {
    const supportedIds = new Set(
      rawMatrix.features.filter(({ status }) => status === "supported").map(({ id }) => id),
    );
    expect(featureBehaviorProbes.filter(({ featureId }) => !supportedIds.has(featureId))).toEqual(
      [],
    );

    for (const prefix of ["trigger.", "transaction."]) {
      expect(
        featureBehaviorProbes
          .map(({ featureId }) => featureId)
          .filter((featureId) => featureId.startsWith(prefix))
          .sort(),
      ).toEqual(
        rawMatrix.features
          .filter(({ id, status }) => status === "supported" && id.startsWith(prefix))
          .map(({ id }) => id)
          .sort(),
      );
    }

    expect(
      featureBehaviorProbes
        .map(({ featureId }) => featureId)
        .filter((featureId) => featureId.startsWith("ddl."))
        .sort(),
    ).toEqual(
      rawMatrix.features
        .filter(({ id, status }) => status === "supported" && id.startsWith("ddl."))
        .map(({ id }) => id)
        .sort(),
    );

    const classifications = new Map(
      rawProfile.overrides.map(({ classification, id }) => [id, classification]),
    );
    expect(
      featureBehaviorProbes
        .map(({ featureId }) => featureId)
        .filter((featureId) => !featureId.startsWith("ddl."))
        .filter((featureId) =>
          rawMatrix.features.some(
            ({ id, status }) =>
              id === featureId &&
              status === "supported" &&
              !id.startsWith("mutation.") &&
              !id.startsWith("trigger.") &&
              !id.startsWith("transaction.") &&
              (classifications.get(id) ?? rawProfile.defaults.supported) !== "compatible",
          ),
        )
        .sort(),
    ).toEqual(
      rawMatrix.features
        .filter(
          ({ id, status }) =>
            status === "supported" &&
            !id.startsWith("mutation.") &&
            !id.startsWith("ddl.") &&
            !id.startsWith("trigger.") &&
            !id.startsWith("transaction.") &&
            (classifications.get(id) ?? rawProfile.defaults.supported) !== "compatible",
        )
        .map(({ id }) => id)
        .sort(),
    );

    expect(
      rawProfile.overrides
        .filter(({ verification }) => verification === "acceptance")
        .map(({ id }) => id)
        .filter((id) => !featureBehaviorProbes.some(({ featureId }) => featureId === id)),
    ).toEqual([]);
  });

  it("leaves no supported acceptance category without an independent semantic oracle", () => {
    const probes = new Set(featureBehaviorProbes.map(({ featureId }) => featureId));
    const overrides = new Map(rawProfile.overrides.map((entry) => [entry.id, entry]));
    const missing = rawMatrix.features
      .filter(({ status }) => status === "supported")
      .filter(({ id }) => {
        const override = overrides.get(id);
        const featureClass = override?.classification ?? rawProfile.defaults.supported;
        if (id.startsWith("mutation.")) return featureClass !== "compatible" && !probes.has(id);
        const write =
          id.startsWith("ddl.") || id.startsWith("trigger.") || id.startsWith("transaction.");
        if (write) return !probes.has(id);
        const externallyCompared =
          featureClass === "compatible" && override?.verification !== "acceptance";
        return !externallyCompared && !probes.has(id);
      })
      .map(({ id }) => id);
    expect(missing).toEqual([]);
  });

  it("uses the real target tables for every formerly count-only mutation", () => {
    const mutationQueries = new Map(
      featureBehaviorProbes
        .filter(({ featureId }) => featureId.startsWith("mutation."))
        .map(({ featureId, steps }) => [
          featureId,
          steps
            .filter(({ kind }) => kind === "query" || kind === "query-pattern")
            .map(({ sql }) => sql),
        ]),
    );
    expect(mutationQueries).toEqual(
      new Map([
        ["mutation.insert-default-values", ["SELECT name, score FROM defaulted_insert"]],
        [
          "mutation.insert-runtime-values",
          [
            "SELECT id, noted_at, sample, token FROM runtime_values",
            "SELECT id, noted_at IS NOT NULL AS has_time, sample >= 0 AND sample < 1 AS sample_in_range, token IS NOT NULL AS has_token FROM runtime_values",
          ],
        ],
        ["mutation.truncate", ["SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST"]],
        [
          "mutation.upsert-replace",
          ["SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST"],
        ],
        [
          "mutation.upsert-expression",
          ["SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST"],
        ],
      ]),
    );
    expect(
      featureBehaviorProbes
        .filter(({ replacesCompatibleMutationState }) => replacesCompatibleMutationState === true)
        .map(({ featureId }) => featureId),
    ).toEqual(["mutation.insert-default-values", "mutation.insert-runtime-values"]);
  });

  it("makes every result shape and expected error explicit", () => {
    for (const probe of featureBehaviorProbes) {
      expect(probe.steps.length, probe.featureId).toBeGreaterThan(0);
      for (const step of probe.steps) {
        expect(step.sql.trim(), probe.featureId).not.toBe("");
        if (
          step.kind === "query" ||
          step.kind === "query-pattern" ||
          (step.kind === "mutation" && step.expected !== undefined)
        ) {
          const expected = step.expected;
          if (expected !== undefined) {
            for (const row of expected.rows) {
              expect(row, `${probe.featureId}: ${step.sql}`).toHaveLength(expected.columns.length);
            }
          }
        }
        if (step.kind === "mutation") expect(step.affectedRows).toBeGreaterThanOrEqual(0);
        if (step.kind === "error") {
          expect(step.errorName.trim()).not.toBe("");
          expect(step.includes.trim()).not.toBe("");
        }
      }
    }
  });

  it("requires the source error class and target-specific message for negative probes", () => {
    expect(
      featureBehaviorProbes.flatMap(({ featureId, steps }) =>
        steps.flatMap((step) =>
          step.kind === "error" ? [[featureId, step.errorName, step.includes] as const] : [],
        ),
      ),
    ).toEqual([
      ["ddl.unique-secondary-index", "UniqueConstraintError", "Duplicate value for keyed.bonus: 7"],
      ["ddl.check-constraint", "TypeError", "CHECK checked_a_check failed for row 0 of checked"],
      [
        "ddl.foreign-key",
        "TypeError",
        "FOREIGN KEY children_parent_fkey has no parents row with id 9",
      ],
      [
        "ddl.named-column-constraint",
        "TypeError",
        "CHECK guarded_n_positive failed for row 0 of guarded",
      ],
      [
        "ddl.create-table-key-clause",
        "UniqueConstraintError",
        "Duplicate value for keyed_clause.a: 1",
      ],
      ["ddl.enum", "TypeError", "unknown is not a value of enum mood"],
      ["ddl.secondary-index", "TypeError", "Index already exists: keyed_score_idx"],
      ["ddl.drop-secondary-index", "TypeError", "Index already exists: keyed_score_idx"],
      ["ddl.composite-secondary-index", "TypeError", "Index already exists: keyed_score_bonus_idx"],
      [
        "ddl.multiple-unique-constraints",
        "UniqueConstraintError",
        "Duplicate value for multi_unique.email: one@example.test",
      ],
      [
        "ddl.composite-primary-key",
        "UniqueConstraintError",
        "Duplicate value for composite_key.(shop, receipt)",
      ],
      [
        "ddl.composite-foreign-key",
        "TypeError",
        "FOREIGN KEY composite_child_shop_receipt_fkey has no composite_key row with (shop, receipt) (1, 2)",
      ],
    ]);
  });
});
