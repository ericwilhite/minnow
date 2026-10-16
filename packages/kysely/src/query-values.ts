import type { QueryParameter, QueryValue } from "@minnowdb/core";
import { copyDate } from "./date-value.js";

function scalarQueryValue(value: unknown): QueryValue | undefined {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return value;
  }
  return value instanceof Date ? copyDate(value) : undefined;
}

/**
 * Validates and snapshots Kysely's unknown parameter array at the adapter boundary. A
 * one-dimensional array of scalars is kept as an array: Minnow binds it as the operand of
 * `= ANY($n)` (Kysely's `eb.fn.any(eb.val([...]))`) and refuses it anywhere else.
 */
export function kyselyQueryValues(parameters: readonly unknown[]): QueryParameter[] {
  const values: QueryParameter[] = [];
  for (let index = 0; index < parameters.length; index += 1) {
    const value = parameters[index];
    const scalar = scalarQueryValue(value);
    if (scalar !== undefined) {
      values.push(scalar);
      continue;
    }
    if (Array.isArray(value)) {
      const members: QueryValue[] = [];
      for (const member of value as unknown[]) {
        const copied = scalarQueryValue(member);
        if (copied === undefined) {
          throw new TypeError(
            `Kysely parameter ${String(index + 1)} is an array with an unsupported member; ` +
              "Minnow accepts arrays of boolean, number, string, Date, or null",
          );
        }
        members.push(copied);
      }
      values.push(members);
      continue;
    }
    throw new TypeError(
      `Kysely parameter ${String(index + 1)} has unsupported type ${typeof value}; ` +
        "Minnow accepts boolean, number, string, Date, null, or an array of those",
    );
  }
  return values;
}
