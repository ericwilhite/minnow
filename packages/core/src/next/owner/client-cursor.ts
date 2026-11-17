/** Ordinary cursor binding. Credit is returned on handout; held Result buffers never recycle. */
import type { Cursor, CursorOptions, Param, Row } from "./api.js";
import { resultColumn } from "./api.js";
import type { ProtocolClient } from "./client.js";
import { cancelledReason } from "./client-call.js";
import { decodeResult } from "./result.js";
import type { StatementSummary } from "./protocol.js";
const chunkSummary: StatementSummary = {
  command: "SELECT",
  rowCount: null,
  counts: null,
  lsn: null,
  replayed: false,
  closedEarly: false,
  transaction: "idle",
  catalogVersion: -1,
};
export function clientCursor<TRow = Row>(
  client: ProtocolClient,
  sql: string,
  params: readonly Param[],
  session: number,
  options: CursorOptions,
  retired: () => void,
  prepared: number | null = null,
): Cursor<TRow> {
  options.signal?.throwIfAborted();
  for (const [label, value] of [
    ["frameRows", options.frameRows],
    ["prefetchBytes", options.prefetchBytes],
  ] as const)
    if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0))
      throw new RangeError(`${label} must be a positive safe integer`);
  const raw = client.cursor(sql, structuredClone(params), {
    session,
    queryOnly: true,
    ...(prepared === null ? {} : { prepared }),
    ...(options.frameRows === undefined ? {} : { frameRows: options.frameRows }),
    ...(options.prefetchBytes === undefined ? {} : { windowBytes: options.prefetchBytes }),
  });
  const abort = () => client.cancel(raw.seq);
  let ended = false,
    closing: Promise<void> | null = null;
  const end = () => {
    if (ended) return;
    ended = true;
    options.signal?.removeEventListener("abort", abort);
    retired();
  };
  options.signal?.addEventListener("abort", abort, { once: true });
  if (options.signal?.aborted) abort();
  const columns = raw.columns
    .then((meta) => meta.map(resultColumn))
    .catch((error: unknown) => {
      throw cancelledReason(error, options.signal);
    });
  columns.catch(() => undefined);
  const close = () =>
    (closing ??= (async () => {
      raw.close();
      try {
        await raw.done;
      } catch {
        /* The iterator carries terminal failure; cleanup cannot mask it. */
      } finally {
        end();
      }
    })());
  return {
    columns,
    close,
    [Symbol.asyncIterator]() {
      return this;
    },
    async next() {
      if (ended) return { done: true, value: undefined };
      try {
        const frame = await raw.frames.next();
        if (frame.done) {
          await raw.done;
          end();
          return { done: true, value: undefined };
        }
        return {
          done: false,
          value: decodeResult<TRow>({
            columns: await raw.columns,
            frames: [frame.value],
            summary: chunkSummary,
            dictionaries: raw.snapshotDictionaries(),
          }),
        };
      } catch (error) {
        end();
        throw cancelledReason(error, options.signal);
      }
    },
    async return() {
      await close();
      return { done: true, value: undefined };
    },
    async throw(error: unknown) {
      await close();
      throw error;
    },
  };
}
