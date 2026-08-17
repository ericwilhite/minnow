/**
 * Loading the comparison engines without bundling them.
 *
 * SQLite Wasm and PGlite both locate their own assets — `.wasm` modules, the OPFS async proxy
 * worker, PGlite's packed data directory — with `new URL(…, import.meta.url)` and dynamic
 * imports a bundler cannot follow. Bundled, those URLs point at nothing.
 *
 * So they are not bundled. `scripts/vendor-engines.mjs` copies each package's shipped browser
 * build into `public/vendor/`, and these imports name a URL, which the browser resolves natively
 * and the bundler is told to leave alone. Every asset beside the entry then resolves exactly as
 * the package expects.
 *
 * The imports stay dynamic, so a benchmark run that selects only Minnow downloads neither.
 */
import type { Sqlite3Static } from "@sqlite.org/sqlite-wasm";
import type { PGlite as PGliteClass, types as pgTypes } from "@electric-sql/pglite";

const SQLITE_ENTRY = "/vendor/sqlite/index.mjs";
const PGLITE_ENTRY = "/vendor/pglite/index.js";

interface SqliteModule {
  default: (options?: { print?: (message: string) => void }) => Promise<Sqlite3Static>;
}

interface PgliteModule {
  PGlite: typeof PGliteClass;
  types: typeof pgTypes;
}

export async function loadSqlite(): Promise<SqliteModule["default"]> {
  const module = (await import(
    /* webpackIgnore: true */ /* turbopackIgnore: true */
    SQLITE_ENTRY
  )) as SqliteModule;
  return module.default;
}

export async function loadPglite(): Promise<PgliteModule> {
  return (await import(
    /* webpackIgnore: true */ /* turbopackIgnore: true */
    PGLITE_ENTRY
  )) as PgliteModule;
}
