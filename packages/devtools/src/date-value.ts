// Results and callback inputs are caller-owned. Read Date internal slots directly so instance
// method overrides cannot alter what the devtools validates or displays.
// eslint-disable-next-line @typescript-eslint/unbound-method
const intrinsicDateGetTime = Date.prototype.getTime;
// eslint-disable-next-line @typescript-eslint/unbound-method -- See the getTime capture above.
const intrinsicDateToISOString = Date.prototype.toISOString;

export function dateMilliseconds(value: Date): number {
  return intrinsicDateGetTime.call(value);
}

export function dateIsoString(value: Date): string {
  return intrinsicDateToISOString.call(value);
}
