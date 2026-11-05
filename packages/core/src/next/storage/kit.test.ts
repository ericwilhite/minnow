import { describe, expect, it } from "vitest";
import { runStorageKit } from "./kit.js";
import { SimBackend, type SimDisk } from "./sim-disk.js";

/** A backend whose sessions skip one file's flush: the reviewer's two wrong stores. */
function withoutFlush(file: "data" | "log") {
  return (disk: SimDisk) => {
    const backend = new SimBackend(disk);
    const acquire = backend.acquire.bind(backend);
    backend.acquire = async (options) => {
      const lease = await acquire(options);
      lease.files[file].flush = () => undefined;
      return lease;
    };
    return backend;
  };
}

describe("storage kit (conformance + physical durability sweep)", () => {
  it("passes the file store with the handle fence", async () => {
    await runStorageKit((disk) => new SimBackend(disk, "handle"));
  });

  it("passes the file store with the epoch fence", async () => {
    await runStorageKit((disk) => new SimBackend(disk, "epoch"));
  });

  it("rejects a store that skips the data flush before publishing a root (review mutant)", async () => {
    await expect(runStorageKit(withoutFlush("data"))).rejects.toThrow(/published extent/u);
  });

  it("rejects a store whose log flush does nothing (review mutant)", async () => {
    await expect(runStorageKit(withoutFlush("log"))).rejects.toThrow(/acknowledged log bytes/u);
  });
});
