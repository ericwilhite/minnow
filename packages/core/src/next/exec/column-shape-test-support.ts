/** Test-only non-memoized shape oracle. Byte counts come from immutable source values,
 * never ChunkReader.varlenBytes/gatherBytes. Header queries use the legacy verified methods.
 * No production imports this module.
 */
import type { ChunkReader } from "../page/reader.js";
import { ValueTag } from "../page/format.js";
import type { TestValue } from "../page/test-support.js";
import type { ChunkPages } from "./scan.js";

export function sourceSizedPages(reader: ChunkReader, source: readonly TestValue[]): ChunkPages {
  const size = (row: number) => {
    const value = source[row];
    return value instanceof Uint8Array ? value.length : 0;
  };
  return new Proxy(reader, {
    get(target, key) {
      if (key === "decodeFacts") return undefined;
      if (key === "varlenBytes")
        return (from: number, to: number) => {
          let bytes = 0;
          for (let row = from; row < to; row++) bytes += size(row);
          return bytes;
        };
      if (key === "gatherBytes")
        return (rows: Uint16Array, start = 0, end = rows.length) => {
          let bytes = 0;
          for (let i = start; i < end; i++) bytes += size(rows[i] ?? 0);
          return bytes;
        };
      if (key === "needsInt128") {
        if (target.directory.tag !== ValueTag.dec64) return false;
        const { min, max } = target.chunk.statistics;
        return (
          typeof min !== "bigint" ||
          typeof max !== "bigint" ||
          min < -(2n ** 63n) ||
          max > 2n ** 63n - 1n
        );
      }
      const value: unknown = Reflect.get(target, key, target);
      if (typeof value !== "function") return value;
      const bound: unknown = value.bind(target);
      return bound;
    },
  });
}
