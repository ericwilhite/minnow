import { dateMilliseconds } from "../date-value.js";

/**
 * Structural equality over live result values: dates by instant, arrays by element, objects by
 * own keys in insertion order. Shared by the typed live store's exact suppression and the keyed
 * live window's diffing; internal on purpose — live-api re-exports its modules wholesale, and
 * this helper is not part of the live API.
 */
export function sameLiveValue(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (left instanceof Date || right instanceof Date) {
    return (
      left instanceof Date &&
      right instanceof Date &&
      Object.is(dateMilliseconds(left), dateMilliseconds(right))
    );
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
    for (let index = 0; index < left.length; index += 1) {
      if (!sameLiveValue(left[index], right[index])) return false;
    }
    return true;
  }
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null) {
    return false;
  }
  const leftRecord = left as Record<string, unknown>;
  const rightRecord = right as Record<string, unknown>;
  const leftKeys = Object.keys(leftRecord);
  const rightKeys = Object.keys(rightRecord);
  if (leftKeys.length !== rightKeys.length) return false;
  for (let index = 0; index < leftKeys.length; index += 1) {
    const key = leftKeys[index];
    if (key === undefined || key !== rightKeys[index]) return false;
    if (!sameLiveValue(leftRecord[key], rightRecord[key])) return false;
  }
  return true;
}
