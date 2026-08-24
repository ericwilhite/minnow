import type { AnyTable, MinnowSqlDriver, SchemaDefinition } from "@minnowdb/core";
import { Kysely } from "kysely";
import { MinnowDialect } from "./dialect.js";
import type { InferKyselyDatabase } from "./schema.js";

export interface CreateKyselyConfig<TSchema extends SchemaDefinition<readonly AnyTable[]>> {
  /** An in-thread database or worker-backed client. */
  readonly driver: MinnowSqlDriver;
  /** Drives exact types and empty-object INSERT normalization; migrate it at application startup. */
  readonly schema: TSchema;
}

/** Creates a Kysely instance whose `DB` type is inferred directly from the Minnow schema. */
export function createKysely<const TSchema extends SchemaDefinition<readonly AnyTable[]>>(
  config: CreateKyselyConfig<TSchema>,
): Kysely<InferKyselyDatabase<TSchema>> {
  return new Kysely<InferKyselyDatabase<TSchema>>({
    dialect: new MinnowDialect({ driver: config.driver, schema: config.schema }),
  });
}
