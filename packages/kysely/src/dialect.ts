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
import { MinnowKyselyDriver, type MinnowResultDecoding } from "./driver.js";
import { MinnowKyselyIntrospector } from "./introspector.js";

export interface MinnowDialectConfig {
  /** An in-thread `MinnowDatabase` or worker-backed `MinnowDatabaseClient`. */
  readonly driver: MinnowSqlDriver;
  /** Enables type inference and multi-row empty-object INSERT normalization. */
  readonly schema?: SchemaDefinition<readonly AnyTable[]>;
  /** Optional logical-domain decoding. Omitted values retain Minnow's lossless text boundary. */
  readonly resultDecoding?: MinnowResultDecoding;
}

/**
 * The dialect adapter, which also carries the dialect's result decoding: a Kysely instance
 * exposes its adapter through `getExecutor().adapter`, and that is how the live-query wrapper
 * decodes results the engine delivers to it the same way the driver decodes executed ones.
 */
export class MinnowKyselyAdapter extends DialectAdapterBase {
  readonly resultDecoding: MinnowResultDecoding;

  constructor(resultDecoding: MinnowResultDecoding) {
    super();
    this.resultDecoding = resultDecoding;
  }

  // Minnow has one logical connection, but the driver serializes it through its own FIFO mutex.
  // Claiming a single connection here would add Kysely's RuntimeDriver mutex in front of ours,
  // and that lock only releases through releaseConnection — a `startTransaction()` whose BEGIN
  // is refused never calls it, wedging the instance forever. The driver's mutex releases the
  // hold when BEGIN throws, so it must be the only serializer.
  override get supportsMultipleConnections(): boolean {
    return true;
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
  readonly #resultDecoding: MinnowResultDecoding;

  constructor(config: MinnowDialectConfig) {
    this.#driver = config.driver;
    this.#schema = config.schema;
    this.#resultDecoding = config.resultDecoding ?? {};
  }

  createAdapter(): DialectAdapter {
    return new MinnowKyselyAdapter(this.#resultDecoding);
  }

  createDriver(): Driver {
    return new MinnowKyselyDriver(this.#driver, this.#resultDecoding);
  }

  createIntrospector(): DatabaseIntrospector {
    return new MinnowKyselyIntrospector(this.#driver);
  }

  createQueryCompiler(): QueryCompiler {
    return new MinnowQueryCompiler(this.#schema);
  }
}
