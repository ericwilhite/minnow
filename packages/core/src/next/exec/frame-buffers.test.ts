/** Whole owned views, including dictionary deltas and validity, participate exactly once. */
import { expect, it } from "vitest";
import { frameTransferList, FrameBufferError } from "./frame-buffers.js";
import type { UnnumberedPayload } from "./result.js";
it("collects deltas, constants, codes and validity, deduplicating full-buffer aliases", () => {
  const delta = new Float64Array([1]);
  const shared = new ArrayBuffer(4);
  const bytes = new Uint8Array([120]);
  const offsets = new Int32Array([0, 1]);
  const validity = new Uint32Array([1]);
  const payload: UnnumberedPayload = {
    rowCount: 1,
    dictionaries: [
      {
        id: 1,
        from: 0,
        values: { form: "flat", length: 1, validity: null, data: { layout: "f64", values: delta } },
      },
    ],
    columns: [
      {
        form: "constant",
        length: 1,
        value: {
          form: "flat",
          length: 1,
          validity: new Uint32Array(shared),
          data: { layout: "i32", values: new Int32Array(shared) },
        },
      },
      { form: "flat", length: 1, validity: null, data: { layout: "varlen", bytes, offsets } },
      { form: "dictionary", length: 1, dictionary: 1, codes: new Int32Array(shared), validity },
    ],
  };
  expect(new Set(frameTransferList(payload))).toEqual(
    new Set([delta.buffer, shared, bytes.buffer, offsets.buffer, validity.buffer]),
  );
  const invalid = {
    ...payload,
    columns: [
      {
        form: "flat" as const,
        length: 1,
        validity: null,
        data: { layout: "i32" as const, values: new Int32Array(new ArrayBuffer(8), 4, 1) },
      },
    ],
  };
  expect(() => frameTransferList(invalid)).toThrow(FrameBufferError);
});

it("validates every view even when a previous full view already contributed its buffer", () => {
  const b = new ArrayBuffer(8);
  expect(() =>
    frameTransferList({
      rowCount: 1,
      dictionaries: [],
      columns: [
        {
          form: "flat",
          length: 2,
          validity: null,
          data: { layout: "i32", values: new Int32Array(b) },
        },
        {
          form: "constant",
          length: 1,
          value: {
            form: "flat",
            length: 1,
            validity: null,
            data: { layout: "i32", values: new Int32Array(b, 4, 1) },
          },
        },
      ],
    }),
  ).toThrow(FrameBufferError);
});
