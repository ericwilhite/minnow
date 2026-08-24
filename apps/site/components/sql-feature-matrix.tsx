import postgresProfile from "@minnowdb/core/postgres-feature-profile.json";
import matrix from "@minnowdb/core/sql-feature-matrix.json";

/**
 * The SQL surface, rendered from the checked-in feature matrix.
 *
 * The matrix is not documentation that describes the engine — it is a fixture the engine is
 * tested against. `feature-matrix.test.ts` runs every supported example through both executors
 * and asserts every unsupported one still fails with the recorded error, so a page built from it
 * cannot drift from what the engine actually does.
 */
interface SqlFeature {
  id: string;
  status: "supported" | "unsupported";
  example: string;
  params?: unknown[];
  error?: string;
  notes?: string;
}

const features = (matrix as { features: SqlFeature[] }).features;

type PostgresClassification =
  "compatible" | "different" | "extension" | "unsupported" | "inapplicable";

const profile = postgresProfile as {
  defaults: {
    supported: PostgresClassification;
    unsupported: PostgresClassification;
  };
  overrides: Array<{
    id: string;
    classification: PostgresClassification;
    reason: string;
  }>;
};
const overrides = new Map(profile.overrides.map((entry) => [entry.id, entry]));

function postgresClassification(feature: SqlFeature): PostgresClassification {
  const override = overrides.get(feature.id);
  if (override !== undefined) return override.classification;
  if (feature.status === "unsupported") return profile.defaults.unsupported;
  return profile.defaults.supported;
}

const postgresLabels: Record<PostgresClassification, string> = {
  compatible: "PostgreSQL compatible",
  different: "PostgreSQL difference",
  extension: "Minnow extension",
  unsupported: "Unsupported",
  inapplicable: "Not applicable",
};
function Feature({ feature }: { feature: SqlFeature }) {
  const pg = postgresClassification(feature);
  const pgReason = overrides.get(feature.id)?.reason;
  const hasDetails = feature.notes !== undefined || pgReason !== undefined;
  return (
    <div className="rounded-lg border border-fd-border p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <code className="text-xs text-fd-muted-foreground">{feature.id}</code>
        <span className="rounded bg-fd-muted px-1.5 py-0.5 text-[11px] text-fd-muted-foreground">
          {postgresLabels[pg]}
        </span>
      </div>
      <pre className="overflow-x-auto rounded-md bg-fd-muted px-3 py-2 text-[13px] leading-relaxed">
        <code>
          {feature.example}
          {feature.params === undefined ? "" : `\n-- bound: ${JSON.stringify(feature.params)}`}
        </code>
      </pre>
      {feature.error === undefined ? null : (
        <p className="mt-2 text-xs text-fd-muted-foreground">
          Error: <code>{feature.error}</code>
        </p>
      )}
      {hasDetails ? (
        <details className="mt-2 text-sm text-fd-muted-foreground">
          <summary className="cursor-pointer select-none text-xs">Details</summary>
          {feature.notes === undefined ? null : <p className="mt-2">{feature.notes}</p>}
          {pgReason === undefined ? null : <p className="mt-2">PostgreSQL: {pgReason}</p>}
        </details>
      ) : null}
    </div>
  );
}

export function SqlFeatureMatrix({ status }: { status: "supported" | "unsupported" }) {
  const selected = features.filter((feature) => feature.status === status);
  if (status === "unsupported") {
    selected.sort((left, right) => left.id.localeCompare(right.id));
  }
  const counts = Object.fromEntries(
    Object.keys(postgresLabels).map((classification) => [
      classification,
      selected.filter((feature) => postgresClassification(feature) === classification).length,
    ]),
  ) as Record<PostgresClassification, number>;
  return (
    <div className="not-prose my-6 flex flex-col gap-2">
      <p className="text-sm text-fd-muted-foreground">
        {status === "supported"
          ? `${String(selected.length)} checked forms · ${String(counts.compatible)} PostgreSQL compatible · ${String(counts.different)} different · ${String(counts.extension)} extensions`
          : `${String(selected.length)} deliberate embedded-database exclusions`}
      </p>
      {selected.map((feature) => (
        <Feature key={feature.id} feature={feature} />
      ))}
    </div>
  );
}
