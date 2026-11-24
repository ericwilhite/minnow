/**
 * Every wait of a query's preparation carries what the statement keeps (`besides`), so one that
 * could never be served beside it is refused typed instead of waiting forever.
 *
 * Found by the admission-ordering council (2026-11-24, S3): a preparation admission made while the
 * attempt held nothing (source recipe, parameter sizing, read-step sizing, a dictionary probe)
 * queued without the statement's holdings, so point 5 could not bound it. A statement holding the
 * writer turn in an open transaction, asking for more than fits beside its own rows, waited
 * forever and blocked every admission behind it.
 *
 * Law: GOVERNOR-WAIT-POLICY point 5 (next/memory/README.md): a request whose statement keeps
 * holdings carries them and is refused typed (53200) when it could never be served beside them;
 * otherwise it waits for finite holders and is served.
 */
import { expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { preparationGovernor } from "./preparation.js";

const MiB = 1 << 20;

async function setting() {
  const governor = new Governor(16 * MiB);
  // The statement keeps 4 MiB while it waits; a finite holder (another query) holds 8 MiB.
  const statement = await governor.admit({
    label: "statement",
    minimumBytes: 4 * MiB,
    spillable: false,
  });
  const other = await governor.admit({
    label: "other query",
    minimumBytes: 8 * MiB,
    spillable: false,
  });
  const preparation = preparationGovernor(governor, "query#1");
  if (preparation === null) throw new Error("the reference governor has a preparation facade");
  preparation.besides(() => ({ grants: [statement] }));
  return { governor, statement, other, preparation };
}

const outcome = (request: Promise<unknown>) =>
  Promise.race([
    request.then(
      () => "served",
      (error: unknown) =>
        `${(error as Error).name} ${String((error as { sqlState?: string }).sqlState)}`,
    ),
    new Promise<string>((resolve) => setTimeout(() => resolve("waiting"), 300)),
  ]);

it("refuses typed a preparation admission that could never fit beside the statement", async () => {
  const { governor, statement, other, preparation } = await setting();
  try {
    const most = governor.servableNow({ grants: [statement] });
    const asked = preparation.admit({
      label: "source-recipe",
      minimumBytes: most + MiB,
      spillable: false,
    });
    expect(await outcome(asked)).toBe("MemoryAdmissionError 53200");
    const grouped = preparation.admitGroup({
      label: "aggregate-domain-planning",
      grants: [{ label: "domains", minimumBytes: most + MiB, spillable: false }],
    });
    expect(await outcome(grouped)).toBe("MemoryAdmissionError 53200");
  } finally {
    other.close();
    statement.close();
    preparation.close();
  }
});

it("waits for a finite holder when it fits beside the statement, and is served", async () => {
  const { governor, statement, other, preparation } = await setting();
  try {
    const most = governor.servableNow({ grants: [statement] });
    const asked = preparation.admit({
      label: "source-recipe",
      minimumBytes: most,
      spillable: false,
    });
    expect(await outcome(asked), "the other query holds the room").toBe("waiting");
    other.close();
    const grant = await asked;
    expect(grant.held).toBe(most);
    grant.close();
  } finally {
    other.close();
    statement.close();
    preparation.close();
  }
});

/** "served" (its grants closed at once), else the error's message. */
const settle = (request: Promise<Grant | readonly Grant[]>) =>
  request.then(
    (granted) => {
      const grants: readonly Grant[] = "close" in granted ? [granted] : granted;
      for (const grant of grants) grant.close();
      return "served";
    },
    (error: unknown) => (error as Error).message,
  );
const turn = () =>
  new Promise<"pending">((resolve) =>
    setImmediate(() => {
      resolve("pending");
    }),
  );

it("keeps a bounded preparation admission in the holder queue it names (holdsCredit)", async () => {
  // #71 review P2: the bounded path dropped `holdsCredit`, so a holder's follow-up queued as a
  // new admission behind ordinary admissions its own release had to serve.
  const governor = new Governor(16 * MiB);
  const running = await governor.admit({
    label: "running holder",
    minimumBytes: 11 * MiB,
    spillable: false,
  });
  const chain = governor.openHolderChain("writer", "writer");
  const preparation = preparationGovernor(governor, "query#1");
  if (preparation === null) throw new Error("the reference governor has a preparation facade");
  preparation.besides(() => ({ grants: [running] }));
  const stop = new AbortController();
  // A new admission that waits at the head of the admission queue (3 MiB free, it asks 5).
  const first = settle(
    governor.admitGroup({
      label: "new admission",
      grants: [{ label: "new admission", minimumBytes: 5 * MiB, spillable: false }],
      signal: stop.signal,
    }),
  );
  const followUp = settle(
    preparation.admit({
      label: "holder follow-up",
      minimumBytes: 3 * MiB,
      spillable: false,
      holdsCredit: chain,
      signal: stop.signal,
    }),
  );
  expect(await Promise.race([followUp, turn()]), "served ahead of the new admission").toBe(
    "served",
  );
  stop.abort(new Error("done"));
  expect(await first).toBe("done");
  preparation.close();
  running.close();
  chain.close();
  expect(governor.usedBytes).toBe(0);
});

it("returns a preparation admission cancelled between its service and its adoption", async () => {
  // #71 review P3: served, then cancelled before the facade adopted it, the 5 MiB grant still went
  // to the caller that had stopped waiting.
  const governor = new Governor(16 * MiB);
  const blocker = await governor.admit({
    label: "blocker",
    minimumBytes: 13 * MiB,
    spillable: false,
  });
  const preparation = preparationGovernor(governor, "query#1");
  if (preparation === null) throw new Error("the reference governor has a preparation facade");
  preparation.besides(() => ({ grants: [] }));
  const stop = new AbortController();
  const asked = settle(
    preparation.admit({
      label: "preparation",
      minimumBytes: 5 * MiB,
      spillable: false,
      signal: stop.signal,
    }),
  );
  blocker.close();
  // Served in that drain; the cancel lands before the facade's adoption runs.
  queueMicrotask(() => {
    stop.abort(new Error("stopped"));
  });
  expect(await asked).toBe("stopped");
  expect(governor.usedBytes).toBe(0);
  preparation.close();
});
