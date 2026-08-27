import type { AnyTable, MinnowSqlDriver, SchemaDefinition } from "@minnowdb/core";
import { Kysely } from "kysely";
import { MinnowDialect } from "./dialect.js";
import type { MinnowResultDecoding } from "./driver.js";
import type { InferKyselyDatabase } from "./schema.js";

export interface CreateKyselyConfig<
  TSchema extends SchemaDefinition<readonly AnyTable[]>,
  TDecoding extends MinnowResultDecoding | undefined = undefined,
> {
  /** An in-thread database or worker-backed client. */
  readonly driver: MinnowSqlDriver;
  /** Drives exact types and empty-object INSERT normalization; migrate it at application startup. */
  readonly schema: TSchema;
  /** Optional logical-domain decoding. Exact NUMERIC conversion is deliberately opt-in. */
  readonly resultDecoding?: TDecoding;
}

/** Creates a Kysely instance whose `DB` type is inferred directly from the Minnow schema. */
export function createKysely<
  const TSchema extends SchemaDefinition<readonly AnyTable[]>,
  const TDecoding extends MinnowResultDecoding | undefined = undefined,
>(config: CreateKyselyConfig<TSchema, TDecoding>): Kysely<InferKyselyDatabase<TSchema, TDecoding>> {
  return new Kysely<InferKyselyDatabase<TSchema, TDecoding>>({
    dialect: new MinnowDialect({
      driver: config.driver,
      schema: config.schema,
      ...(config.resultDecoding === undefined ? {} : { resultDecoding: config.resultDecoding }),
    }),
  });
}
