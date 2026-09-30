/** The regression gate preserves its historical schema. Comparisons use equal primary keys. */
export function performanceMode(arguments_: readonly string[]): "regression" | "comparison" {
  if (arguments_.includes("--compare")) {
    if (arguments_.includes("--update"))
      throw new Error("A matched-index comparison cannot update historical regression thresholds");
    return "comparison";
  }
  return "regression";
}
export function performanceIdDeclaration(mode: "regression" | "comparison"): string {
  return mode === "comparison" ? '"id" INTEGER PRIMARY KEY' : '"id" INTEGER';
}
