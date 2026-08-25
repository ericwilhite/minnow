import type { QueryValue } from "@minnowdb/core";

/** Validates and snapshots Kysely's unknown parameter array at the adapter boundary. */
export function kyselyQueryValues(parameters: readonly unknown[]): QueryValue[] {
  return parameters.map((value, index) => {
    if (
      value === null ||
      typeof value === "boolean" ||
      typeof value === "number" ||
      typeof value === "string" ||
      value instanceof Date
    ) {
      return value instanceof Date ? new Date(value.getTime()) : value;
    }
    throw new TypeError(
      `Kysely parameter ${String(index + 1)} has unsupported type ${typeof value}; ` +
        "Minnow accepts boolean, number, string, Date, or null",
    );
  });
}
