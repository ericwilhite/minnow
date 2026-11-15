/** Ordinary writable open must refuse future MCAT before recovery touches roots or WAL. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_CAPABILITY_OPEN_REACH } from "../../testing/reach.js";
import { Harness, autocommit } from "./test-support.js";
import {
  capabilityCases,
  capabilityChanges,
  futureCatalog,
  refusalCode,
} from "./catalog-capability.test-support.js";
import { decodeManifestRoot, encodeManifestRoot } from "../format/manifest-codec.js";
import { encodeCatalogPage, encodeCatalogRoot } from "../format/catalog-pages.js";
import { reachableExtents } from "../format/reachable.js";
import { ExtentListKeeper } from "../storage/extent-list.js";
import type { ExtentRef } from "../storage/extents.js";
import type { StoreSession } from "../storage/contract.js";
const FREE = { allocate: () => true, free: () => undefined };
async function extents(h: Harness, root: Uint8Array): Promise<Map<number, ExtentRef>> {
  const result = new Map<number, ExtentRef>();
  for await (const entry of reachableExtents()(root, h.space, FREE)) {
    const extent = "extent" in entry ? entry.extent : entry;
    result.set(extent.offset, extent);
  }
  return result;
}
async function install(h: Harness, logical: Uint8Array): Promise<void> {
  const before = h.space.root;
  if (!before) throw new Error("missing fixture root");
  const root = decodeManifestRoot(before, FREE).value,
    writer = h.space.beginWrite("future MCAT fixture");
  const keeper = new ExtentListKeeper([], { reader: h.space, budget: FREE });
  try {
    const page = (await writer.write([encodeCatalogPage({ offset: 0, data: logical })])).blobs[0];
    if (!page) throw new Error("missing future catalog page");
    const catalog = (
      await writer.write([
        encodeCatalogRoot({
          depth: 0,
          bytes: logical.length,
          pages: 1,
          children: [{ ref: page, bytes: logical.length, pages: 1 }],
        }),
      ])
    ).blobs[0];
    if (!catalog) throw new Error("missing future catalog root");
    const next = { ...root, sequence: root.sequence + 1, catalog, extentList: [] };
    const previous = await extents(h, before),
      current = await extents(h, encodeManifestRoot(next));
    const prepared = await keeper.prepare(1, {
      added: [...current.values()],
      released: [],
      writer,
    });
    await h.space.publish(encodeManifestRoot({ ...next, extentList: prepared.list }), {
      writers: [writer],
      retired: [...previous.values()].filter((e) => !current.has(e.offset)),
      list: prepared.list,
    });
    keeper.commit(1, next.sequence);
  } finally {
    if (writer.state === "open") await writer.abort();
    keeper.close();
  }
}
function image(h: Harness, name: string): Uint8Array {
  return h.disk.files.get(name)?.current.slice() ?? new Uint8Array();
}
for (const seed of seedsFor("next-catalog-capability-open", [0xb804, 0xb805]))
  it(`refuses before root/WAL mutation and releases open resources seed ${String(seed)}`, async () => {
    const reach = createReach("catalog writable-open refusal", CATALOG_CAPABILITY_OPEN_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: capabilityCases.length - 1 }),
        fc.integer({ min: 0, max: 2 }),
        fc.integer({ min: 100, max: 2 ** 40 }),
        fc.uint8Array({ maxLength: 100 }),
        fc.boolean(),
        async (mode, level, tag, payload, retry) => {
          reach.beginHistory();
          const which = capabilityCases[mode];
          if (!which) throw new Error("missing open mode");
          const h = new Harness("strict", "sim", 1024, undefined, { merges: { mode: "off" } });
          const open = h.backend.open.bind(h.backend);
          let session: StoreSession | undefined;
          h.backend.open = async (lease) => {
            session = await open(lease);
            return session;
          };
          await h.open();
          try {
            for (const change of capabilityChanges())
              await autocommit(h.path, (s) => s.changeCatalog(change));
            await h.path.recycleLog();
            await h.reopen();
            const bytes = futureCatalog(h.path.catalog, which, level, tag, payload, 128);
            await h.path.close();
            await install(h, bytes);
            if (!session) throw new Error("missing fixture session");
            // A valid reader would truncate this unacknowledged torn suffix. A catalog refusal must not.
            await session.appendLog(Uint8Array.of(0xff, 0, 0x81));
            await session.flushLog();
            await h.close();
            const before = ["root-0", "root-1", "log"].map((name) => image(h, name));
            let writers = 0;
            h.onSpace = (space) => {
              const begin = space.beginWrite.bind(space);
              space.beginWrite = (...args) => {
                writers++;
                return begin(...args);
              };
            };
            for (let attempt = 0; attempt < (retry ? 2 : 1); attempt++) {
              await expect(h.open()).rejects.toMatchObject({ code: refusalCode(which) });
              expect(["root-0", "root-1", "log"].map((name) => image(h, name))).toEqual(before);
              expect(writers).toBe(0);
              expect(h.governor.usedBytes).toBe(0);
            }
            reach.reach(which);
            reach.reach(
              level === 0 ? "metadata" : level === 1 ? "nested counters" : "nested physical",
            );
            for (const state of [
              "both root slots unchanged",
              "WAL header and tail unchanged",
              "no native writer",
              "zero resources",
            ] as const)
              reach.reach(state);
            if (retry) reach.reach("retry unchanged");
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: capabilityCases.map(
          (_, i) =>
            [i, i % 3, 100, Uint8Array.of(255), true] as [
              number,
              number,
              number,
              Uint8Array,
              boolean,
            ],
        ),
      },
    );
    reach.assertReached();
  }, 120000);
