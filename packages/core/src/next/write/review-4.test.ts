/**
 * Review B3-p2a-4 finding 3: a pending publication keeps the chunks its new directory shares with
 * the installed generation charged while it awaits, even if a drop meanwhile releases every
 * installed generation (the reviewer's probe). Findings 1 and 2 are in
 * testing/divergences.test.ts (write/ may not import testing/).
 */
import { watchGrants } from "./test-support.js";
import { expect, it } from "vitest";
import type { Grant } from "../memory/governor.js";
import { need } from "./bytes.js";
import { GroupDirectory } from "./groups.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  countMetadataPinBytes,
  INT,
  ints,
  tableInput,
} from "./test-support.js";

it("a pending publication retains its shared chunks while a drop lands during the publish", async () => {
  const harness = new Harness("strict", "memory", 1);
  const pinCredit = countMetadataPinBytes(harness);
  let metadata: Grant | null = null;
  watchGrants(harness.governor, (grant) => {
    if (grant.label === "write path metadata") metadata = grant;
  });
  const path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)], { primaryKey: null }),
        })
      ).id ?? 0;
  });
  await autocommit(path, (s) =>
    s.table(t).insert(batch(ints(Array.from({ length: 1101 }, (_, k) => k))), null),
  );
  await path.quiesce();
  await autocommit(path, (s) => s.table(t).insert(batch(ints([1101])), null));
  await path.quiesce();
  const before = need(path.state().tables.get(t)).groups;
  expect(before.count).toBeGreaterThan(1024);
  // The reviewer's probe: capture the directory the pending publication builds.
  // eslint-disable-next-line @typescript-eslint/unbound-method -- restored below, called with apply
  const original = GroupDirectory.prototype.with;
  let pending: GroupDirectory | null = null;
  GroupDirectory.prototype.with = function (this: GroupDirectory, ...args) {
    const made = original.apply(this, args);
    pending = made;
    return made;
  };
  const publish = harness.space.publish.bind(harness.space);
  let entered!: () => void;
  let resume!: () => void;
  const hit = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    resume = resolve;
  });
  harness.space.publish = async (...args) => {
    entered();
    await gate;
    return publish(...args);
  };
  let refs: number | undefined;
  try {
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1102])), null));
    await hit;
    const made = need(pending as GroupDirectory | null);
    expect(made.chunks[0]).toBe(before.chunks[0]);
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
    refs = need(made.chunks[0]).refs;
  } finally {
    GroupDirectory.prototype.with = original;
    resume();
    await path.quiesce();
  }
  // Every installed generation let the shared chunk go; the pending directory still held it.
  expect(refs).toBeGreaterThan(0);
  // After the publication resolved (into a dropped table) everything is returned.
  expect(need(metadata as Grant | null).allocated).toBe(path.residentMetadataBytes + pinCredit());
  await harness.close();
});
