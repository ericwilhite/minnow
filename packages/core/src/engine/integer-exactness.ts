/** Integer results must stay inside the number representation promised by INTEGER. */
export function checkedInteger(value: number): number {
  if (!Number.isSafeInteger(value))
    throw new RangeError("Integer out of range (JavaScript safe integer required)");
  return value;
}

/** Only the planner's integer domain requires exact integer accumulation. */
export function checkedSum(left: number, right: number, integer: boolean): number {
  const result = left + right;
  return integer ? checkedInteger(result) : result;
}
