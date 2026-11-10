/** Native interval provenance, outside the published logical-plan fields. */
export interface IntervalArithmeticSource {
  readonly text: string;
  readonly subtract: boolean;
}
const sources = new WeakMap<object, IntervalArithmeticSource>();
let present = false;
export function intervalArithmeticSource(value: object): IntervalArithmeticSource | undefined {
  return present ? sources.get(value) : undefined;
}
export function rememberIntervalArithmeticSource(
  value: object,
  source: IntervalArithmeticSource,
): void {
  present = true;
  sources.set(value, source);
}
export function copyIntervalArithmeticSource(source: object, target: object): void {
  const value = intervalArithmeticSource(source);
  if (value !== undefined) rememberIntervalArithmeticSource(target, value);
}
