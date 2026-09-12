/**
 * Interleavings the unit tests do not cover: repeated foreground flips between two tabs while a
 * third tab writes continuously, and two hidden tabs that keep idle-releasing while both keep
 * writing. Every write resolves "ok" and the final catalog equals the set of writes issued; no
 * `leader-unavailable`, no uncertain outcome, no duplicates.
 */
import { expect, it, vi } from "vitest";
import { heavyTestTimeout } from "../../engine/storage-test-helpers.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { opener, outcomeMessage, sleep, table, waitFor } from "./coordination-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

it("keeps every third-tab write correct across 20 foreground handovers", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "flip", { yieldCooldownMs: 40, handoverGraceMs: 60 });
  const one = await open();
  const two = await open();
  const writer = await open();
  await one.addTable(table("seed"));
  await two.getCurrentManifestVersion();
  await writer.getCurrentManifestVersion();
  const outcomes: string[] = [];
  let index = 0;
  const state = { running: true };
  const writing = (async () => {
    while (state.running) {
      const name = `w${String(index)}`;
      index += 1;
      outcomes.push(await outcomeMessage(writer.addTable(table(name))));
      await sleep(3);
    }
  })();
  for (let flip = 0; flip < 20; flip += 1) {
    const [visible, hidden] = flip % 2 === 0 ? [two, one] : [one, two];
    hidden.setForeground(false);
    visible.setForeground(true);
    await waitFor(() => visible._isLeaderForTests(), `flip ${String(flip)} to settle`);
    await sleep(30);
  }
  state.running = false;
  await writing;
  const names = (await writer.listTables()).map((record) => record.name).sort();
  const expected = ["seed", ...Array.from({ length: index }, (_, i) => `w${String(i)}`)].sort();
  expect(outcomes.filter((result) => result !== "ok")).toEqual([]);
  expect(names).toEqual(expected);
  one.close();
  two.close();
  writer.close();
});

it("keeps writes correct while two hidden tabs keep idle-releasing leadership", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "idle-chaos", { hiddenIdleReleaseMs: 15, handoverGraceMs: 20 });
  const one = await open();
  const two = await open();
  await one.addTable(table("seed"));
  await two.getCurrentManifestVersion();
  one.setForeground(false);
  two.setForeground(false);
  const outcomes: string[] = [];
  const issued: string[] = [];
  const run = async (store: typeof one, prefix: string) => {
    for (let i = 0; i < 60; i += 1) {
      const name = `${prefix}${String(i)}`;
      issued.push(name);
      outcomes.push(await outcomeMessage(store.addTable(table(name))));
      await sleep(Math.random() * 40);
    }
  };
  await Promise.all([run(one, "a"), run(two, "b")]);
  const names = (await one.listTables()).map((record) => record.name).sort();
  expect(outcomes.filter((result) => result !== "ok")).toEqual([]);
  expect(names).toEqual(["seed", ...issued].sort());
  one.close();
  two.close();
});
