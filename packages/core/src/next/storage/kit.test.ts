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
    await expect(runStorageKit(withoutFlush("data"))).rejects.toThrow(
      /published root's extent (is missing|does not match)/u,
    );
  });

  it("rejects a store whose log flush does nothing (review mutant)", async () => {
    await expect(runStorageKit(withoutFlush("log"))).rejects.toThrow(/acknowledged log bytes/u);
  });

  it("rejects a store that loses the previous root on reopen (review A1-5 mutant)", async () => {
    // The reviewer's probe: readPreviousRoot answers only within the session that published.
    const losesPrevious = (disk: SimDisk) => {
      const backend = new SimBackend(disk);
      const open = backend.open.bind(backend);
      backend.open = async (lease) => {
        const session = await open(lease);
        let published = false;
        const publish = session.publish.bind(session);
        session.publish = async (root) => {
          await publish(root);
          published = true;
        };
        const previous = session.readPreviousRoot.bind(session);
        session.readPreviousRoot = async () => (published ? previous() : null);
        return session;
      };
      return backend;
    };
    await expect(runStorageKit(losesPrevious)).rejects.toThrow(
      /clean reopen: previous root null is not the one before the current/u,
    );
  });

  it("rejects a store whose previous root is the current one", async () => {
    const backend = (disk: SimDisk) => {
      const inner = new SimBackend(disk);
      const open = inner.open.bind(inner);
      inner.open = async (lease) => {
        const session = await open(lease);
        session.readPreviousRoot = () => session.readRoot();
        return session;
      };
      return inner;
    };
    await expect(runStorageKit(backend)).rejects.toThrow(/previous root|previous root survives/u);
  });
});
