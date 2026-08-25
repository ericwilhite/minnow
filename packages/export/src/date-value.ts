// Query cursors are structural and may be implemented outside Minnow. Read Date internal slots
// directly so an exported value cannot replace the methods used to validate or serialize it.
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
