import type { AnyTable, MinnowSqlDriver, SchemaDefinition } from "@minnowdb/core";
import {
  DialectAdapterBase,
  type DatabaseIntrospector,
  type Dialect,
  type DialectAdapter,
  type Driver,
  type QueryCompiler,
} from "kysely";
import { MinnowQueryCompiler } from "./compiler.js";
import { MinnowKyselyDriver } from "./driver.js";
import { MinnowKyselyIntrospector } from "./introspector.js";

export interface MinnowDialectConfig {
  /** An in-thread `MinnowDatabase` or worker-backed `MinnowDatabaseClient`. */
  readonly driver: MinnowSqlDriver;
  /** Enables type inference and multi-row empty-object INSERT normalization. */
  readonly schema?: SchemaDefinition<readonly AnyTable[]>;
}

class MinnowKyselyAdapter extends DialectAdapterBase {
  override get supportsMultipleConnections(): boolean {
    return false;
  }

  override get supportsReturning(): boolean {
    return true;
  }

  acquireMigrationLock(): Promise<void> {
    // Kysely reserves this dialect's only connection for the complete migration. As with PGlite,
    // there is no second connection inside one dialect instance to race it.
    return Promise.resolve();
  }

  releaseMigrationLock(): Promise<void> {
    return Promise.resolve();
  }
}

/** A Kysely dialect that compiles PostgreSQL SQL and executes it through Minnow. */
export class MinnowDialect implements Dialect {
  readonly #driver: MinnowSqlDriver;
  readonly #schema: SchemaDefinition<readonly AnyTable[]> | undefined;

  constructor(config: MinnowDialectConfig) {
    this.#driver = config.driver;
    this.#schema = config.schema;
  }

  createAdapter(): DialectAdapter {
    return new MinnowKyselyAdapter();
  }

  createDriver(): Driver {
    return new MinnowKyselyDriver(this.#driver);
  }

  createIntrospector(): DatabaseIntrospector {
    return new MinnowKyselyIntrospector(this.#driver);
  }

  createQueryCompiler(): QueryCompiler {
    return new MinnowQueryCompiler(this.#schema);
  }
}
