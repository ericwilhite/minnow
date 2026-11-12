/** One shared CPU span covers complete query lifetimes; never normalize unexecuted work. */
import type { BoundaryValue } from "./independent-cpu.mts";
export async function queryLifetimes(
  repeats: number,
  rowsPerLifetime: number,
  run: () => Promise<BoundaryValue>,
): Promise<BoundaryValue> {
  if (
    !Number.isSafeInteger(repeats) ||
    repeats < 1 ||
    !Number.isSafeInteger(rowsPerLifetime) ||
    rowsPerLifetime < 0 ||
    !Number.isSafeInteger(repeats * rowsPerLifetime)
  )
    throw new RangeError("Invalid complete query volume");
  let rows = 0,
    completed = 0;
  for (; completed < repeats; completed++) {
    const next = await run();
    if (next.rows !== rowsPerLifetime) throw new Error("Complete query row count drift");
    rows += next.rows;
  }
  if (completed !== repeats || rows !== repeats * rowsPerLifetime)
    throw new Error("Incomplete query observation");
  return { rows, detail: { completedLifetimes: completed, rowsPerLifetime } };
}
