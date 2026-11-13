/**
 * `@electric-sql/pglite` as Vitest runs it (vitest.config.ts aliases the package here).
 *
 * A fresh PGlite runs initdb before its first query: about three seconds on a CI runner, and more
 * than two hundred test files start one or more. The run's global setup (./pglite-snapshot.ts)
 * starts one fresh instance with this same package and dumps its data directory. Here a PGlite
 * constructed with default options — `new PGlite()` or `PGlite.create()`, no arguments — loads that
 * dump instead of running initdb again, so it starts from the identical just-initialised cluster
 * about six times faster. A call with any argument (a data directory, parsers, extensions) is
 * passed through untouched, and so is every call when no snapshot is set
 * (`MINNOW_PGLITE_SNAPSHOT=off` turns it off for a run). Everything else the package exports is the
 * package's own. pglite-snapshot.test.ts checks that a snapshot instance and a fresh one are
 * indistinguishable.
 */
import { readFileSync } from "node:fs";
import { PGlite as FreshPGlite } from "../node_modules/@electric-sql/pglite/dist/index.js";

export * from "../node_modules/@electric-sql/pglite/dist/index.js";

let snapshot: Blob | undefined;

/** The run's snapshot, or undefined to construct exactly as asked. */
function snapshotBlob(): Blob | undefined {
  const path = (process.env.MINNOW_PGLITE_SNAPSHOT ?? "").trim();
  if (path === "" || path === "off") return undefined;
  snapshot ??= new Blob([readFileSync(path)]);
  return snapshot;
}

type Arguments = ConstructorParameters<typeof FreshPGlite>;

/** The arguments as given, or the snapshot's when there are none and the run has one. */
function withSnapshot(args: readonly unknown[]): Arguments {
  if (args.length !== 0) return args as Arguments;
  const loadDataDir = snapshotBlob();
  return (loadDataDir === undefined ? [] : [{ loadDataDir }]) as Arguments;
}

export class PGlite extends FreshPGlite {
  constructor(...args: unknown[]) {
    super(...withSnapshot(args));
  }
}

// The package's `create` constructs its own class, so it is replaced with one that constructs this
// one, keeping its contract: construct with the same arguments, then wait until ready.
Object.defineProperty(PGlite, "create", {
  value: async (...args: unknown[]): Promise<PGlite> => {
    const db = new PGlite(...args);
    await db.waitReady;
    return db;
  },
});
