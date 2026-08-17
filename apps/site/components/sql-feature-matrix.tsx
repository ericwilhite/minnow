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

function Feature({ feature }: { feature: SqlFeature }) {
  return (
    <div className="rounded-lg border border-fd-border p-4">
      <div className="mb-2 flex items-center gap-2">
        <code className="text-xs text-fd-muted-foreground">{feature.id}</code>
      </div>
      <pre className="overflow-x-auto rounded-md bg-fd-muted p-3 text-[13px] leading-relaxed">
        <code>
          {feature.example}
          {feature.params === undefined ? "" : `\n-- bound: ${JSON.stringify(feature.params)}`}
        </code>
      </pre>
      {feature.error === undefined ? null : (
        <p className="mt-2 text-sm text-fd-muted-foreground">
          Rejected with: <code>{feature.error}</code>
        </p>
      )}
      {feature.notes === undefined ? null : (
        <p className="mt-2 text-sm text-fd-muted-foreground">{feature.notes}</p>
      )}
    </div>
  );
}

export function SqlFeatureMatrix({ status }: { status: "supported" | "unsupported" }) {
  const selected = features.filter((feature) => feature.status === status);
  return (
    <div className="not-prose my-6 flex flex-col gap-3">
      <p className="text-sm text-fd-muted-foreground">
        {status === "supported"
          ? `${String(selected.length)} forms, each executed through both executors on every test run.`
          : `${String(selected.length)} forms, each checked on every test run to still fail with the error below.`}
      </p>
      {selected.map((feature) => (
        <Feature key={feature.id} feature={feature} />
      ))}
    </div>
  );
}
