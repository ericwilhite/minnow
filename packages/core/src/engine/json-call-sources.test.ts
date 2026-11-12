/** Native spelling is private metadata; shipped aliases keep their existing DTO and identity. */
import { expect, it } from "vitest";
import {
  compileQuery,
  compileWithDeclaredTypes,
  clonePlanTree,
  mapChildExpressions,
} from "./query.js";
import { jsonCallSource } from "./json-call-sources.js";
import { encodeQueryIdentity } from "./query-identity.js";
it("keeps native JSON spelling through cloning, annotations and identity", () => {
  const native = (name: string) =>
    compileWithDeclaredTypes(() => compileQuery(`SELECT ${name}(TIMESTAMP '2000-01-01',NULL)`))
      .select[0]?.expression;
  const build = native("json_build_array"),
    standard = native("json_array");
  expect(build?.kind).toBe("call");
  expect(standard?.kind).toBe("call");
  if (!build || !standard) throw new Error("Expected expressions");
  expect(jsonCallSource(build)).toBe("JSON_BUILD_ARRAY");
  expect(jsonCallSource(clonePlanTree(build))).toBe("JSON_BUILD_ARRAY");
  expect(jsonCallSource(mapChildExpressions(build, (x) => x))).toBe("JSON_BUILD_ARRAY");
  expect(JSON.stringify(build)).toBe(JSON.stringify(standard));
  expect(encodeQueryIdentity(build)).not.toBe(encodeQueryIdentity(standard));
  const shipped = (name: string) => compileQuery(`SELECT ${name}(1,NULL)`).select[0]?.expression;
  const a = shipped("json_build_array"),
    b = shipped("json_array");
  expect(a).toBeDefined();
  if (!a || !b) throw new Error("Expected shipped expressions");
  expect(jsonCallSource(a)).toBeUndefined();
  expect(encodeQueryIdentity(a)).toBe(encodeQueryIdentity(b));
});
