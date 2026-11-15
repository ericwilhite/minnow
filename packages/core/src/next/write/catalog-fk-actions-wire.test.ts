/** Independent family10/field9 ABI: do not derive field IDs or flags from the registry. */
import { expect, it } from "vitest";
import { ByteWriter } from "./bytes.js";
import { encodeCatalogRecord, decodeCatalogRecord } from "./catalog-records.js";
const integer = (n: number) => {
  const w = new ByteWriter();
  w.varuint(n);
  return w.finish();
};
const string = (s: string) => {
  const w = new ByteWriter();
  w.string(s);
  return w.finish();
};
const wire = (fields: ReadonlyArray<readonly [number, Uint8Array]>) => {
  const body = new ByteWriter();
  for (const [id, bytes] of fields) {
    body.varuint(id);
    body.u8(1);
    body.blob(bytes);
  }
  const w = new ByteWriter();
  w.varuint(10);
  w.varuint(1);
  w.blob(body.finish());
  return w.finish();
};
it("keeps every effective action in required foreign-key field9", () => {
  const columns = new ByteWriter();
  columns.varuint(1);
  columns.varuint(2);
  let cases = 0;
  for (const onDelete of ["restrict", "noAction", "cascade", "setNull", "setDefault"])
    for (const onUpdate of ["follow", "restrict", "noAction", "cascade", "setNull", "setDefault"]) {
      const fields: Array<readonly [number, Uint8Array]> = [
        [1, integer(1)],
        [2, string("Case.汉")],
        [3, columns.finish()],
        [4, integer(3)],
        [5, integer(4)],
        [6, Uint8Array.of(1, 5)],
        [7, string(onDelete)],
        [8, Uint8Array.of(1)],
        [9, string(onUpdate)],
      ];
      const bytes = wire(fields),
        decoded = decodeCatalogRecord("foreignKey", bytes);
      expect(decoded).toMatchObject({ onDelete, onUpdate });
      const encoded = encodeCatalogRecord("foreignKey", decoded);
      try {
        expect(encoded.bytes).toEqual(bytes);
      } finally {
        encoded.close();
      }
      expect(() => decodeCatalogRecord("foreignKey", wire(fields.slice(0, 8)))).toThrow(
        expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
      );
      expect(() =>
        decodeCatalogRecord("foreignKey", wire([...fields, [12, Uint8Array.of(0)]])),
      ).toThrow(expect.objectContaining({ code: "MINNOW_CATALOG_UNSUPPORTED" }));
      cases++;
    }
  expect(cases).toBe(30);
});
