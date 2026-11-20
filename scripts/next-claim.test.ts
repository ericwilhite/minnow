import { describe, expect, it } from "vitest";
import {
  balancedOrders,
  bracketRepeats,
  claimManifest,
  holm,
  manifestDigest,
  report,
  signTestP,
  targetVerdict,
  validateManifest,
  MIN_EFFECT,
  type CellRecord,
  type ClaimManifest,
  type ClaimResult,
} from "./next-claim.mts";

describe("next/ speed-claim protocol arithmetic", () => {
  it("computes the exact two-sided sign test", () => {
    expect(signTestP(9, 0)).toBeCloseTo(2 / 512, 12);
    expect(signTestP(7, 2)).toBeCloseTo((2 * (36 + 9 + 1)) / 512, 12);
    expect(signTestP(0, 0)).toBe(1);
    // Nine pairs can never survive a 45-cell correction; fifteen can.
    expect(signTestP(9, 0) * 45).toBeGreaterThan(0.05);
    expect(signTestP(15, 0) * 45).toBeLessThan(0.05);
  });

  it("applies Holm's step-down adjustment", () => {
    expect(holm([0.01, 0.04, 0.03])).toEqual([0.03, 0.06, 0.06]);
    expect(holm([0.5])).toEqual([0.5]);
  });

  it("orders pairs as a balanced, seeded shuffle", () => {
    const orders = balancedOrders(15, 47);
    expect(orders).toHaveLength(15);
    expect(orders.filter((o) => o[0] === "control")).toHaveLength(8);
    expect(balancedOrders(15, 47)).toEqual(orders);
    expect(balancedOrders(15, 48)).not.toEqual(orders);
  });

  it("sizes brackets to at least a second or ten queries", () => {
    expect(bracketRepeats(5)).toBe(10);
    expect(bracketRepeats(250)).toBe(4);
    expect(bracketRepeats(4000)).toBe(1);
  });

  it("requires the minimum effect on top of the paired rule", () => {
    const base = {
      cell: "c",
      repeats: 2,
      controlMedianMs: 100,
      acceptance: {
        medianDeltaMs: -2 * 100 * MIN_EFFECT,
        iqrDeltaMs: 1,
        favoursCandidate: 9,
        favoursControl: 0,
        acceptedGain: true,
        significantRegression: false,
      },
    };
    expect(targetVerdict(base)).toBe("gain");
    expect(targetVerdict({ ...base, acceptance: { ...base.acceptance, medianDeltaMs: -1 } })).toBe(
      "none",
    );
  });
});

const A = "a".repeat(40),
  B = "b".repeat(40);
const neutral = (cell: string, pairs = 15): CellRecord => ({
  cell,
  repeats: 1,
  completedPairs: pairs,
  controlMedianMs: 100,
  candidateMedianMs: 100,
  signP: 1,
  acceptance: {
    medianDeltaMs: 0,
    iqrDeltaMs: 5,
    favoursCandidate: 7,
    favoursControl: 8,
    acceptedGain: false,
    significantRegression: false,
  },
});
const gain = (cell: string): CellRecord => ({
  ...neutral(cell),
  candidateMedianMs: 50,
  signP: signTestP(15, 0),
  acceptance: {
    medianDeltaMs: -50,
    iqrDeltaMs: 5,
    favoursCandidate: 15,
    favoursControl: 0,
    acceptedGain: true,
    significantRegression: false,
  },
});
const manifest: ClaimManifest = {
  control: A,
  candidate: B,
  pairs: 15,
  targets: ["t"],
  datasets: [
    { dataset: "W1M", shards: 2, controls: ["aa"], cells: ["t", "s1", "s2"] },
    { dataset: "S1", shards: 1, controls: ["qa"], cells: ["q1"] },
  ],
};
const shard = (
  dataset: "W1M" | "S1",
  name: string,
  aa: CellRecord[],
  ab: CellRecord[],
): ClaimResult => ({
  dataset,
  shard: name,
  started: "",
  finished: "",
  control: { root: "", revision: A },
  candidate: { root: "", revision: B },
  node: "v24",
  pairs: 15,
  seed: 1,
  targets: ["t"],
  repeats: {},
  aa,
  ab,
  void: false,
  manifest: manifestDigest(manifest),
});
// W1M shard 0 holds t and s2, shard 1 holds s1; S1 has one shard.
const complete = (): ClaimResult[] => [
  shard("W1M", "0/2", [neutral("aa")], [gain("t"), neutral("s2")]),
  shard("W1M", "1/2", [neutral("aa")], [neutral("s1")]),
  shard("S1", "0/1", [neutral("qa")], [neutral("q1")]),
];

describe("next/ claim report", () => {
  it("gives verdicts for a complete, neutral run", () => {
    const text = report(complete(), manifest);
    expect(text).not.toContain("INCOMPLETE");
    expect(text).toMatch(/W1M\/t \| target .*\*\*gain\*\*/u);
    expect(text).toContain("across all 3 declared screened cells");
  });

  it("voids every shard's verdicts when any shard's A/A control fails the rule", () => {
    const results = complete();
    results[1] = shard("W1M", "1/2", [gain("aa")], [neutral("s1")]);
    const text = report(results, manifest);
    expect(text).toContain("**VOID:**");
    expect(text).toMatch(/W1M\/t \| target \| 0\/2 .*\| void \|/u);
    expect(text).toContain("**gain** (voids the run)");
    expect(text).not.toMatch(/\| (target|screen) \|[^\n]*\*\*gain\*\*/u);
  });

  it("gives no verdict when a shard, a dataset, a cell or pairs are missing", () => {
    const cases: ClaimResult[][] = [
      complete().slice(1),
      complete().filter((r) => r.dataset !== "S1"),
      [shard("W1M", "0/2", [neutral("aa")], [gain("t")]), ...complete().slice(1)],
      [
        shard("W1M", "0/2", [neutral("aa")], [{ ...gain("t"), completedPairs: 14 }, neutral("s2")]),
        ...complete().slice(1),
      ],
      [...complete(), shard("W1M", "0/2", [neutral("aa")], [gain("t"), neutral("s2")])],
    ];
    for (const results of cases) {
      const text = report(results, manifest);
      expect(text).toContain("**INCOMPLETE:**");
      expect(text).not.toContain("**gain**");
    }
    expect(report(complete().slice(0, 2), manifest)).toContain("S1 shard 0/1 missing");
  });

  it("corrects screened p-values over the declared family, not the reported cells", () => {
    const results = complete();
    const s2 = { ...neutral("s2"), signP: 0.01 };
    results[0] = shard("W1M", "0/2", [neutral("aa")], [gain("t"), s2]);
    // Three declared screened cells (s1, s2, q1): Holm multiplies the smallest p by 3.
    expect(report(results, manifest)).toMatch(/W1M\/s2 \| screen .*\| 0\.0300 \|/u);
    // Still 3 when S1 never reported: a missing cell does not shrink the family.
    expect(report(results.slice(0, 2), manifest)).toMatch(/W1M\/s2 \| screen .*\| 0\.0300 \|/u);
  });

  it("validates dispatch inputs before declaring a run", async () => {
    const base = {
      control: A,
      candidate: B,
      datasets: ["W1M"],
      shards: 6,
      pairs: 15,
      targets: [],
      only: [],
    };
    await expect(claimManifest({ ...base, pairs: 8 })).rejects.toThrow(/pairs/u);
    await expect(claimManifest({ ...base, shards: 0 })).rejects.toThrow(/shards/u);
    await expect(claimManifest({ ...base, only: ["x;rm -rf /"] })).rejects.toThrow(/cell name/u);
    await expect(claimManifest({ ...base, targets: ["no_such_cell"] })).rejects.toThrow(/Target/u);
    await expect(claimManifest({ ...base, control: "main" })).rejects.toThrow(/SHA/u);
    const declared = await claimManifest({ ...base, datasets: ["W1M", "S1"] });
    expect(declared.datasets.map((d) => d.cells.length)).toEqual([29, 16]);
  });

  // Review of #12, second round (P2): manifests were neither validated nor bound to results.
  it("refuses results that ran under a different manifest", () => {
    // The shards ran with no targets; a manifest edited afterwards to target t gives no gain.
    const untargeted: ClaimManifest = { ...manifest, targets: [] };
    const results = complete().map((r) => ({
      ...r,
      targets: [],
      manifest: manifestDigest(untargeted),
    }));
    expect(report(results, untargeted)).not.toContain("INCOMPLETE");
    const text = report(results, manifest);
    expect(text).toContain("ran under a different manifest");
    expect(text).not.toContain("**gain**");
  });

  it("refuses an artefact whose manifest digest differs, even when everything else matches", () => {
    const results = complete();
    results[2] = { ...(results[2] as ClaimResult), manifest: "0".repeat(64) };
    const text = report(results, manifest);
    expect(text).toContain("S1 shard 0/1 ran under a different manifest");
    expect(text).not.toContain("**gain**");
  });

  it("validates a loaded manifest, including its mandatory A/A controls", async () => {
    const good = await claimManifest({
      control: A,
      candidate: B,
      datasets: ["W1M", "S1"],
      shards: 2,
      pairs: 15,
      targets: ["join_fact_fact_int"],
      only: [],
    });
    const raw = JSON.parse(JSON.stringify(good)) as {
      datasets: Array<{ controls: string[]; cells: string[]; shards: number }>;
      targets: string[];
    } & Record<string, unknown>;
    expect(await validateManifest(raw)).toEqual(good);
    const broken = (change: (m: typeof raw) => void) => {
      const copy = JSON.parse(JSON.stringify(raw)) as typeof raw;
      change(copy);
      return validateManifest(copy);
    };
    await expect(
      broken((m) => {
        for (const d of m.datasets) d.controls = [];
      }),
    ).rejects.toThrow(/A\/A controls/u);
    await expect(
      broken((m) => {
        const first = m.datasets[0];
        if (first !== undefined) first.controls = ["scan_proj3_agg"];
      }),
    ).rejects.toThrow(/A\/A controls/u);
    await expect(
      broken((m) => {
        m.targets = ["scan_first_page_typo"];
      }),
    ).rejects.toThrow(/target/u);
    await expect(
      broken((m) => {
        m.extra = true;
      }),
    ).rejects.toThrow(/fields/u);
    await expect(
      broken((m) => {
        const first = m.datasets[0];
        if (first !== undefined) first.shards = 0;
      }),
    ).rejects.toThrow(/shards/u);
  });
});
