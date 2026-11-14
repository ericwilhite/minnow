/** Retain source object references before typed coercion/constant folding erases them. */
import type { CatalogView } from "../access/catalog.js";
import { PlanningError } from "./errors.js";
export function expressionObjects(
  value: unknown,
  catalog: CatalogView,
): {
  readonly typeIds: readonly number[];
  readonly sequenceIds: readonly number[];
} {
  const types = new Set<number>(),
    sequences = new Set<number>(),
    seen = new Set<object>();
  const visit = (value: unknown): void => {
    if (typeof value !== "object" || value === null || seen.has(value)) return;
    seen.add(value);
    const node = value as Record<string, unknown>;
    // Literal JSON/record contents are data, even when they resemble an expression node.
    if (node.kind === "literal") return;
    if (node.kind === "call" && typeof node.name === "string" && Array.isArray(node.arguments)) {
      const name = node.name.toUpperCase();
      const operand = node.arguments[name === "CAST" ? 1 : 0] as
        Record<string, unknown> | undefined;
      if (operand?.kind === "literal" && typeof operand.value === "string") {
        if (name === "CAST" && operand.value.startsWith("enum:")) {
          const name = operand.value.slice(5),
            type = catalog.typeByName(name);
          if (!type) throw new PlanningError(`type "${name}" does not exist`, "42704");
          types.add(type.id);
        } else if (["NEXTVAL", "CURRVAL", "SETVAL"].includes(name)) {
          const sequence = catalog.sequences.find((sequence) => sequence.name === operand.value);
          if (!sequence)
            throw new PlanningError(`relation "${operand.value}" does not exist`, "42P01");
          sequences.add(sequence.id);
        }
      }
    }
    for (const child of Object.values(node)) visit(child);
  };
  visit(value);
  return {
    typeIds: [...types].sort((a, b) => a - b),
    sequenceIds: [...sequences].sort((a, b) => a - b),
  };
}
