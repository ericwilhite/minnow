import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { describe, expect, it } from "vitest";
import { retailBatches, retailDefinition } from "./retail";
import { isLoaded, loadRetailDataset } from "./load";

describe("playground dataset lifecycle", () => {
  it("does not mistake an interrupted build for a complete dataset", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    const scale = 0.001;

    await database.migrate(retailDefinition);
    let inserted = false;
    for (const batch of retailBatches({ scale })) {
      await database.insertBatch(batch.table, batch.rows);
      inserted = true;
      break;
    }
    expect(inserted).toBe(true);

    expect(await isLoaded(database, scale)).toBeUndefined();
    const rows = await loadRetailDataset(database, { scale });
    expect(await isLoaded(database, scale)).toEqual({ rows });

    store.close();
  });

  it("requires an exact scale receipt and rebuilds without duplicate rows", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    const scale = 0.001;

    const firstRows = await loadRetailDataset(database, { scale });
    expect(firstRows).toBeGreaterThan(0);
    expect(await isLoaded(database, scale * 2)).toBeUndefined();

    const rebuiltRows = await loadRetailDataset(database, { scale });
    expect(rebuiltRows).toBe(firstRows);
    expect(await isLoaded(database, scale)).toEqual({ rows: firstRows });
    const counted = await database.query("SELECT COUNT(*) AS count FROM products");
    expect(counted.rows[0]?.count).toBeGreaterThan(0);

    store.close();
  });
});
