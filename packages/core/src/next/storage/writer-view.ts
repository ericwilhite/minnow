/** A borrowed array-shaped ownership view carries a bounded async source for native consumers.
 * Length is a scalar. Synchronous element materialization is available only in the resident
 * window; no proxy trap performs I/O or allocates a history-sized vector.
 */
import type { ExtentRef, ExtentWriter } from "./extents.js";
const NATIVE_VIEW = Symbol("native sealed extent view");
export interface WriterViewSource {
  readonly writer: ExtentWriter;
  resident(): readonly ExtentRef[];
}
export function writerViewSource(value: readonly ExtentRef[]): WriterViewSource | undefined {
  return (value as readonly ExtentRef[] & { readonly [NATIVE_VIEW]?: WriterViewSource })[
    NATIVE_VIEW
  ];
}
export function createWriterExtentView(
  writer: ExtentWriter,
  resident: () => readonly ExtentRef[],
  count: () => number,
): readonly ExtentRef[] {
  const source: WriterViewSource = { writer, resident };
  const view = new Proxy<ExtentRef[]>([], {
    get(_target, key) {
      if (key === NATIVE_VIEW) return source;
      if (key === "length") return count();
      if (key === "constructor") return Array;
      if (key === Symbol.toStringTag) return "Array";
      if (key === "then" || key === "toJSON") return undefined;
      const values = resident();
      const value: unknown = Reflect.get(values, key, values);
      if (typeof value === "function") {
        if (
          typeof key === "string" &&
          [
            "push",
            "pop",
            "shift",
            "unshift",
            "splice",
            "sort",
            "reverse",
            "fill",
            "copyWithin",
          ].includes(key)
        )
          throw new TypeError("Writer ownership views are read-only");
        return (...args: unknown[]) => Reflect.apply(value, view, args) as unknown;
      }
      return value;
    },
    has: (_target, key) => Reflect.has(resident(), key),
    ownKeys: () => Reflect.ownKeys(resident()),
    getOwnPropertyDescriptor(_target, key) {
      if (key === "length")
        return { value: count(), writable: true, configurable: false, enumerable: false };
      const descriptor = Reflect.getOwnPropertyDescriptor(resident(), key);
      return descriptor === undefined
        ? undefined
        : { ...descriptor, configurable: true, writable: false };
    },
    set: () => false,
    defineProperty: () => false,
    deleteProperty: () => false,
    setPrototypeOf: () => false,
    preventExtensions: () => false,
  });
  return view;
}
