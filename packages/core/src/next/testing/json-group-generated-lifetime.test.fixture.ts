/** Observe native cache ownership; no mirrored peak formula or production diagnostic hook. */
import { expect, vi } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { Charge } from "../exec/spill-pool.js";
import * as keys from "../exec/sort-keys.js";
import { JsonGroupKeys } from "../exec/agg/json-group-keys.js";
import type { JsonHistory } from "./json-construct-history.test.fixture.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied to the original receiver.
const tryAdmit = Governor.prototype.tryAdmit;
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied to the original receiver.
const admitGroup = Governor.prototype.admitGroup;
// eslint-disable-next-line @typescript-eslint/unbound-method -- applied to the original receiver.
const equal = JsonGroupKeys.prototype.equal;
const decode = keys.decodeOperands;
const drop = keys.dropOperands;
export interface GeneratedNestedCase {
  readonly widths: readonly number[];
  readonly depth: number;
  readonly nullGroups: number;
  readonly unicode: boolean;
  readonly descending: boolean;
}
export function generatedNestedSql(data: GeneratedNestedCase): string {
  const total = data.widths.reduce((sum, width) => sum + width, 0);
  const widths = data.widths.map((width) =>
    Math.max(32, Math.floor(width * Math.min(1, 49152 / total))),
  );
  const group = widths.map((width, index) => {
    let value: unknown = data.unicode ? "汉".repeat(Math.floor(width / 3)) : "x".repeat(width);
    for (let level = 0; level < data.depth; level++)
      value = (level + index) % 2 ? [value, null, index] : { item: value, tag: true };
    const literal = "'" + JSON.stringify(value).replaceAll("'", "''") + "'::jsonb";
    return `CASE WHEN n>${String(4 - data.nullGroups)} AND (n-${String(5 - data.nullGroups)})%${String(widths.length)}=${String(index)} THEN NULL ELSE ${literal} END`;
  });
  return `SELECT json_agg(n ORDER BY n ${data.descending ? "DESC" : "ASC"})::text j FROM(VALUES(1),(2),(3),(4))q(n) GROUP BY ${group.join(",")}`;
}
export function observeNestedLifetime(history: JsonHistory) {
  const native = new Map<string, Grant>();
  const cached = new Map<keys.Operands, Grant>();
  const pendingGrowth = new Set<Grant>();
  const watched = new Set<Grant>();
  const states = { cached: false, retired: false, growth: false, boundary: false };
  const capture = (grants: readonly Grant[]) => {
    try {
      history.guard();
    } catch (error) {
      for (const grant of grants) grant.close();
      throw error;
    }
    for (const grant of grants) native.set(grant.label, grant);
    return grants;
  };
  history.observe(
    vi.spyOn(history.governor, "tryAdmit").mockImplementation((options) => {
      history.guard();
      const grant = tryAdmit.call(history.governor, options);
      if (grant) capture([grant]);
      return grant;
    }),
  );
  history.observe(
    vi.spyOn(history.governor, "admitGroup").mockImplementation((options) => {
      history.guard();
      return admitGroup.call(history.governor, options).then(capture);
    }),
  );
  history.observe(
    vi.spyOn(keys, "decodeOperands").mockImplementation((...args) => {
      history.guard();
      const result = decode(...args);
      const charge = args[5],
        into = args[6];
      if (result === 0 && into.bytes > 0 && charge instanceof Charge) {
        const grant = native.get(charge.grant.label);
        if (!grant) throw new Error("Decoded sort cache has no captured native account");
        cached.set(into, grant);
        if (into.rows >= 2) states.cached = true;
        if (!watched.has(grant)) {
          watched.add(grant);
          const release = grant.release.bind(grant);
          history.observe(
            vi.spyOn(grant, "release").mockImplementation((bytes) => {
              history.guard();
              release(bytes);
              if (pendingGrowth.has(grant) && grant.held === grant.allocated + grant.loaned) {
                pendingGrowth.delete(grant);
                states.growth = true;
              }
            }),
          );
        }
      }
      return result;
    }),
  );
  history.observe(
    vi.spyOn(keys, "dropOperands").mockImplementation((operands, charge) => {
      history.guard();
      const grant = cached.get(operands),
        bytes = operands.bytes;
      drop(operands, charge);
      if (grant && bytes > 0) {
        states.retired = true;
        if (grant.held > grant.allocated + grant.loaned) pendingGrowth.add(grant);
      }
    }),
  );
  history.observe(
    vi.spyOn(JsonGroupKeys.prototype, "equal").mockImplementation(function (
      this: JsonGroupKeys,
      ...args
    ) {
      history.guard();
      states.boundary = true;
      expect(
        [...cached.keys()].reduce((sum, operands) => sum + operands.bytes, 0),
        "completed resident sort retained decoded operands at group handoff",
      ).toBe(0);
      expect(
        pendingGrowth.size,
        "completed resident sort retained unused cache growth at group handoff",
      ).toBe(0);
      return equal.apply(this, args);
    }),
  );
  return states;
}
