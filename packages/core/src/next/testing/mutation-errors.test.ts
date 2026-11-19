/** Native row errors keep code/state/identity and bounded diagnostic causes across both ports. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MUTATION_ERRORS_REACH } from "../../testing/reach.js";
import { MAX_SERIALIZED_CAUSE_DEPTH } from "../../worker-protocol/index.js";
import { CancelledError } from "../owner/errors.js";
import { DEFAULT_RESULT_WINDOW_BYTES } from "../owner/protocol.js";
import { mutationOwner } from "./mutation-owner.test-support.js";

class PublicCheckError extends Error {}
class PublicNullError extends Error {}
class WrongPublicError extends Error {}

for (const seed of seedsFor("next-mutation-errors-native", [0xb70d, 0xb70e]))
  it(`rehydrates native mutation identity and error context seed ${String(seed)}`, async () => {
    const reach = createReach("native mutation wire errors", MUTATION_ERRORS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("direct" as const, "channel" as const),
        fc.constantFrom("check", "null", "cancel"),
        fc.integer({ min: 0, max: 10 }),
        fc.boolean(),
        fc.constantFrom("context", "雪", 'quoted"name'),
        async (transport, kind, depth, dom, tag) => {
          reach.beginHistory();
          const registry = new Map<string, new (...args: never[]) => Error>([
              ["CheckViolationError", PublicCheckError],
              ["NotNullViolationError", PublicNullError],
              ["CancelledError", WrongPublicError],
            ]),
            f = await mutationOwner(transport, 2, { errorRegistry: registry });
          // Changing caller configuration later cannot change an existing client's prototypes.
          registry.set("CheckViolationError", WrongPublicError);
          registry.set("NotNullViolationError", WrongPublicError);
          let cause: Error = dom ? new DOMException(tag, "QuotaExceededError") : new Error(tag);
          for (let i = 0; i < depth; i++)
            cause = Object.assign(new Error(`${tag}:${String(i)}`, { cause }), {
              code: `CTX_${String(i)}`,
              sqlState: "XX000",
            });
          const captured: { error: Error | null } = { error: null };
          f.annotateError((error) => {
            if (!(error instanceof Error)) return;
            if (!Object.hasOwn(error, "cause"))
              Object.defineProperty(error, "cause", { value: cause });
            captured.error = error;
          });
          try {
            await f.connect();
            const gate = kind === "cancel" ? f.arm("end") : null,
              request = f.client.send(
                {
                  t: "statement",
                  session: 0,
                  prepared: null,
                  knownColumnsVersion: -1,
                  text: `UPDATE t SET v=${kind === "check" ? "-1" : kind === "null" ? "NULL" : "v+1"}`,
                  params: [],
                  result: {
                    windowBytes: DEFAULT_RESULT_WINDOW_BYTES,
                    frameRows: null,
                    wantRows: true,
                  },
                },
                { mutation: true },
              ),
              result = request.promise.then(
                () => null,
                (error: unknown) => error,
              );
            if (gate) {
              await gate.entered;
              f.client.cancel(request.seq);
              await f.fence();
              gate.release();
            }
            const error = await result;
            if (kind === "cancel") {
              expect(error).toBeInstanceOf(CancelledError);
              expect(error).toMatchObject({ code: "MINNOW_CANCELLED", sqlState: "57014" });
              reach.reach("owner prototype protected");
            } else {
              expect(error).toBeInstanceOf(kind === "check" ? PublicCheckError : PublicNullError);
              expect(error).toMatchObject({
                name: kind === "check" ? "CheckViolationError" : "NotNullViolationError",
                code: kind === "check" ? "MINNOW_CHECK_VIOLATION" : "MINNOW_NOT_NULL_VIOLATION",
                sqlState: kind === "check" ? "23514" : "23502",
                row: 0,
              });
              const original = captured.error;
              expect(original).toBeInstanceOf(Error);
              if (!(original instanceof Error) || !(error instanceof Error))
                throw Error("native error context");
              for (const key of ["tableId", kind === "check" ? "checkId" : "columnId"])
                expect(Reflect.get(error, key)).toEqual(Reflect.get(original, key));
              let expected: unknown = original,
                actual: unknown = error;
              for (let links = 0; links <= MAX_SERIALIZED_CAUSE_DEPTH; links++) {
                if (!(expected instanceof Error) || !(actual instanceof Error))
                  throw Error("missing typed cause");
                expect(actual).toMatchObject({ name: expected.name, message: expected.message });
                for (const key of ["code", "sqlState"])
                  if (Reflect.has(expected, key))
                    expect(Reflect.get(actual, key)).toEqual(Reflect.get(expected, key));
                if (expected instanceof DOMException) {
                  expect(actual).toBeInstanceOf(DOMException);
                  reach.reach("DOMException cause");
                } else reach.reach("Error cause");
                if (links === MAX_SERIALIZED_CAUSE_DEPTH || expected.cause === undefined) {
                  expect(actual.cause).toBeUndefined();
                  break;
                }
                expected = expected.cause;
                actual = actual.cause;
              }
              if (depth + 1 > MAX_SERIALIZED_CAUSE_DEPTH) reach.reach("cause depth bounded");
              if (tag === "雪") reach.reach("unicode cause");
              reach.reach(kind === "check" ? "CHECK" : "NOT NULL");
              reach.reach("registry configuration copied");
            }
            expect(await f.rows()).toEqual([
              [1, 1],
              [2, 1],
            ]);
            expect(f.client.pendingCount).toBe(0);
            reach.reach(transport);
          } finally {
            await f.close();
          }
          reach.reach("zero native charge");
        },
      ),
      {
        seed,
        numRuns: 28,
        examples: [
          ["direct", "check", 0, true, "雪"],
          ["channel", "null", 2, false, "context"],
          ["direct", "null", 10, true, "context"],
          ["channel", "cancel", 0, false, "context"],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
