/** Integer results must stay inside the number representation promised by INTEGER. */
export function checkedInteger(value: number): number {
  if (!Number.isSafeInteger(value))
    throw new RangeError("Integer out of range (JavaScript safe integer required)");
  return value;
}

/** Also protects untyped numeric accumulators when their contributions are exact integers. */
export function checkedSum(left: number, right: number): number {
  const result = left + right;
  // In-range integer sums are exact; floating sums retain their existing number semantics.
  if (Math.abs(result) <= Number.MAX_SAFE_INTEGER) return result;
  return Number.isSafeInteger(left) && Number.isSafeInteger(right)
    ? checkedInteger(result)
    : result;
}
