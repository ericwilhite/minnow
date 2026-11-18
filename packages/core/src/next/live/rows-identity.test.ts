/** Compare coalesced native identity transitions with the shipped keyed consumer, observing
 * each accepted effect separately there so a reused value cannot hide an identity boundary. */
import { afterAll, expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, LIVE_IDENTITY_REACH } from "../../testing/reach.js";
import { KeyedLiveQuery } from "../../engine/keyed-live.js";
import { LiveQuery } from "../../engine/typed-live.js";
import type { LiveQueryObserveOptions } from "../../engine/live.js";
import { LiveRows, type KeyChange } from "./rows.js";
interface Row {
  id: number;
  identity: string;
}
const reach = createReach("next-live-row-identity", LIVE_IDENTITY_REACH);
afterAll(() => reach.assertReached());
async function settle(check: () => boolean) {
  for (let i = 0; i < 100; i++) {
    if (check()) return;
    await Promise.resolve();
  }
  throw new Error("Shipped consumer did not settle");
}
for (const seed of seedsFor("next-live-row-identity", [8868, 8869]))
  for (const scenario of [
    "reuse source",
    "reuse destination",
    "retire chain",
    "swap",
    "unrelated",
  ] as const)
    for (const updateNew of [false, true]) {
      it(`matches shipped identity events for ${scenario} new-hop=${String(updateNew)} seed=${String(seed)}`, async () => {
        reach.beginHistory();
        const random = mulberry32(seed),
          a = 1 + Math.floor(random() * 50),
          b = a + 100,
          c = b + 100,
          d = c + 100;
        const original: Row[] =
          scenario === "swap" || scenario === "reuse destination"
            ? [
                { id: a, identity: "A" },
                { id: b, identity: "B" },
              ]
            : [{ id: a, identity: "A" }];
        let current = original,
          options: LiveQueryObserveOptions | undefined,
          version = 1;
        const source = new LiveQuery<Row>(
          {
            observe: async (_query, observer) => {
              options = observer;
              observer.onInvalidate({ manifestVersion: version, catalogEpoch: 1, initial: true });
              return { dependencyTableIds: ["t"], close: () => undefined };
            },
            refresh: async () => undefined,
            close: () => undefined,
          },
          {
            query: "SELECT id,identity FROM t ORDER BY id",
            execute: () => Promise.resolve(current),
          },
        );
        const shipped = new KeyedLiveQuery(source, { key: "id", table: "t" });
        const unsubscribe = shipped.subscribe(() => undefined);
        const nativeNotices: KeyChange[] = [],
          facts = new Map<string, { type: string; identity: string; from?: number; to?: number }>();
        const originals = new Map(original.map((row) => [row.identity, row.id]));
        let bytes = 0;
        const native = new LiveRows<Row>(
          {
            allocate(n) {
              bytes += n;
              return true;
            },
            free(n) {
              bytes -= n;
            },
          },
          "id",
          { table: "t" },
        );
        native.apply(original, 1);
        const publish = async (rows: Row[], notice?: KeyChange) => {
          current = rows;
          version++;
          if (notice) nativeNotices.push(notice);
          options?.onInvalidate({
            manifestVersion: version,
            catalogEpoch: 1,
            initial: false,
            keyChanges:
              notice && notice.kind !== "delete"
                ? [{ table: "t", from: notice.from as number, to: notice.to as number }]
                : [],
          });
          await settle(() => {
            const state = shipped.getSnapshot();
            return state.status === "ready" && state.version === version;
          });
          const state = shipped.getSnapshot();
          if (state.status !== "ready") throw new Error("Missing shipped snapshot");
          for (const change of state.changes) {
            if (change.type === "move" || change.type === "update") continue;
            const identity =
              change.type === "delete" ? change.previous.identity : change.row.identity;
            const from = originals.get(identity);
            if (change.type === "delete") {
              if (from !== undefined) facts.set(identity, { type: "delete", identity, from });
              else facts.delete(identity);
            } else if (from === undefined)
              facts.set(identity, { type: "insert", identity, to: change.row.id });
            else facts.set(identity, { type: "rekey", identity, from, to: change.row.id });
          }
        };
        const update = async (identity: string, to: number, fromRowId: number, toRowId: number) => {
          const old = current.find((row) => row.identity === identity);
          if (!old) throw new Error("Missing old identity");
          await publish(
            current.map((row) => (row === old ? { ...row, id: to } : row)),
            { table: "t", from: old.id, to, fromRowId, toRowId },
          );
        };
        const remove = async (identity: string, rowId: number) => {
          const old = current.find((row) => row.identity === identity);
          if (!old) throw new Error("Missing old identity");
          await publish(
            current.filter((row) => row !== old),
            {
              table: "t",
              kind: "delete",
              from: old.id,
              to: old.id,
              fromRowId: rowId,
              toRowId: rowId,
            },
          );
        };
        try {
          await settle(() => shipped.getSnapshot().status === "ready");
          if (scenario === "reuse source") {
            await update("A", b, 10, 11);
            await publish([...current, { id: a, identity: "N" }]);
          } else if (scenario === "reuse destination") {
            await remove("B", 20);
            await update("A", b, 10, 11);
            if (updateNew) await publish([...current, { id: a, identity: "N" }]);
          } else if (scenario === "retire chain") {
            await update("A", b, 10, 11);
            await remove("A", 11);
            await publish([{ id: b, identity: "N" }]);
          } else if (scenario === "swap") {
            await update("A", c, 10, 11);
            await update("B", a, 20, 21);
            await update("A", b, 11, 12);
          } else {
            await remove("A", 10);
            await publish([{ id: b, identity: "N" }]);
          }
          if (updateNew && current.some((row) => row.identity === "N"))
            await update("N", d, 30, 31);
          // Exercise a different final order than accepted-effect order.
          const descending = random() < 0.5;
          const final = [...current].sort((x, y) => (descending ? y.id - x.id : x.id - y.id));
          const state = native.apply(final, version, { keyChanges: nativeNotices });
          const actual = state.changes
            .filter((change) => change.type !== "move" && change.type !== "update")
            .map((change) => {
              if (change.type === "delete")
                return { type: change.type, identity: change.previous.identity, from: change.key };
              if (change.type === "insert")
                return { type: change.type, identity: change.row.identity, to: change.row.id };
              return {
                type: change.type,
                identity: change.previous.identity,
                from: change.from,
                to: change.key,
              };
            });
          expect(actual.sort((x, y) => x.identity.localeCompare(y.identity))).toEqual(
            [...facts.values()].sort((x, y) => x.identity.localeCompare(y.identity)),
          );
          expect(
            state.changes.filter((change) => "previous" in change).map((change) => change.previous),
          ).toHaveLength(
            new Set(
              state.changes
                .filter((change) => "previous" in change)
                .map((change) => change.previous),
            ).size,
          );
          expect(state.rows).toEqual(final);
          reach.reach(scenario);
          if (current.some((row) => row.identity === "N"))
            reach.reach(updateNew ? "new identity hop" : "new identity insert");
        } finally {
          native.close();
          unsubscribe();
          shipped.close();
        }
        expect(bytes).toBe(0);
        reach.reach("zero charges");
      });
    }
