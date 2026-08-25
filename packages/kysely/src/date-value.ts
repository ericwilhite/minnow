// Read Date internal slots directly. Adapter inputs are caller-owned objects and may shadow their
// instance methods; later Date.prototype mutations must not change the value sent to Minnow.
// eslint-disable-next-line @typescript-eslint/unbound-method
const intrinsicDateGetTime = Date.prototype.getTime;

/** Copies a Date without invoking caller-controlled instance methods. */
export function copyDate(value: Date): Date {
  return new Date(intrinsicDateGetTime.call(value));
}
