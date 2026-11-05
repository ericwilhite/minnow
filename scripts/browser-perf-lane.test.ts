import { describe, expect, it } from "vitest";
import {
  analyticsCells,
  analyticsDataset,
  sqlFor,
  WIDE_QUERIES,
} from "./browser-perf/catalogue.ts";
import { canonicalValue, digestResult } from "./browser-perf/page/canonical.ts";
import { wideBatch, wideSpec, wideValue } from "./browser-perf/page/wide-data.ts";
import {
  browserPerfProfile,
  calibrate,
  confirmedFlags,
  evaluate,
  parseBrowserPerfBaseline,
  targetProgress,
  thresholdFor,
  updatedBaseline,
  type CellObservation,
} from "./lib/browser-perf-baseline.mts";
import { filesystemTypeFor, isMemoryFilesystem, parseMounts } from "./lib/browser-perf-host.mts";
import { classifyFailure, compareResults, summarize } from "./lib/browser-perf-results.mts";

const timing = (
  median: number,
  best = median * 0.9,
  worst = median * 1.1,
): ReturnType<typeof summarize> => ({
  median,
  p95: worst,
  best,
  worst,
  count: 5,
});

describe("browser lane host checks", () => {
  const mounts = parseMounts(
    [
      "/dev/mapper/root / btrfs rw,relatime 0 0",
      "/dev/mapper/root /home btrfs rw,relatime 0 0",
      "tmpfs /tmp tmpfs rw,nosuid 0 0",
      "tmpfs /home/me/ram\\040disk tmpfs rw 0 0",
    ].join("\n"),
  );

  it("finds the mount that holds a path, longest prefix first", () => {
    expect(filesystemTypeFor("/home/me/.cache/minnow-browser-perf", mounts)).toBe("btrfs");
    expect(filesystemTypeFor("/tmp/profile", mounts)).toBe("tmpfs");
    expect(filesystemTypeFor("/tmpfoo", mounts)).toBe("btrfs");
    expect(filesystemTypeFor("/home/me/ram disk/profile", mounts)).toBe("tmpfs");
  });

  it("treats tmpfs and ramfs as memory", () => {
    expect(isMemoryFilesystem("tmpfs")).toBe(true);
    expect(isMemoryFilesystem("ramfs")).toBe(true);
    expect(isMemoryFilesystem("ext4")).toBe(false);
    expect(isMemoryFilesystem(undefined)).toBe(false);
  });
});

describe("browser lane statistics and failures", () => {
  it("reports the median and a nearest-rank p95", () => {
    expect(summarize([5, 1, 4, 2, 3])).toEqual({ median: 3, p95: 5, best: 1, worst: 5, count: 5 });
    expect(summarize([1, 2, 3, 4]).median).toBe(2.5);
    expect(() => summarize([])).toThrow();
  });

  it("classifies the failure modes the baseline records", () => {
    const f = (name: string, message: string, timeout = false) =>
      classifyFailure({ name, message, timeout });
    expect(f("DeadlineError", "no answer", true)).toBe("timeout");
    expect(
      f(
        "QueryResourceLimitError",
        "Columnar table events_wide requested 75,139,744 accounted bytes",
      ),
    ).toBe("budget");
    expect(
      f(
        "StorageResourceLimitError",
        "temp page would reach 16385; the fixed safety limit is 16384",
      ),
    ).toBe("spill-limit");
    expect(f("StorageCorruptionError", "Temp owner record must be created at revision zero")).toBe(
      "storage-corruption",
    );
    expect(f("Error", "Out of Memory Error: could not allocate block")).toBe("out-of-memory");
    expect(f("TypeError", "something else")).toBe("error");
    expect(classifyFailure({ crashed: true })).toBe("tab-crash");
  });
});

describe("browser lane result checks", () => {
  it("accepts equal hashes and float noise, and rejects different values", () => {
    expect(compareResults({ hash: "a" }, { hash: "a" }, true)).toBe("match");
    const reference = { hash: "a", dump: [[1, "x", 0.1 + 0.2]] };
    expect(compareResults(reference, { hash: "b", dump: [[1, "x", 0.3]] }, true)).toBe("tolerance");
    expect(compareResults(reference, { hash: "b", dump: [[1, "x", 0.31]] }, true)).toBe("mismatch");
    expect(compareResults({ hash: "a" }, { hash: "b" }, false)).toBe("mismatch-unverifiable");
  });

  it("separates tie reordering from a wrong order", () => {
    const reference = {
      hash: "a",
      dump: [
        ["p", 1.0000000000001],
        ["q", 1],
      ],
    };
    expect(
      compareResults(
        reference,
        {
          hash: "b",
          dump: [
            ["q", 1],
            ["p", 1],
          ],
        },
        true,
      ),
    ).toBe("ties");
    expect(
      compareResults(
        reference,
        {
          hash: "b",
          dump: [
            ["q", 1],
            ["p", 1],
          ],
        },
        false,
      ),
    ).toBe("tolerance");
  });

  it("canonicalizes each engine's representation of the same value", () => {
    expect(canonicalValue("12.50")).toBe(canonicalValue(12.5));
    expect(canonicalValue("2024-01-01")).toBe(canonicalValue(new Date("2024-01-01T00:00:00Z")));
    expect(canonicalValue(true)).toBe(canonicalValue(1));
    expect(canonicalValue(10n)).toBe(canonicalValue(10));
    const rows = [
      [1, "a"],
      [2, "b"],
    ];
    const view = (data: Array<Array<number | string>>) => ({
      rows: data.length,
      columns: 2,
      get: (r: number, c: number) => data[r]?.[c],
    });
    expect(digestResult(view(rows), false).hash).toBe(
      digestResult(view([...rows].reverse()), false).hash,
    );
    expect(digestResult(view(rows), true).hash).not.toBe(
      digestResult(view([...rows].reverse()), true).hash,
    );
  });
});

describe("browser lane datasets", () => {
  it("generates the same values on every call", () => {
    const spec = wideSpec(200);
    expect(spec).toHaveLength(200);
    expect(wideSpec(500)).toHaveLength(500);
    const a = wideBatch(spec, 1000, 3, 1_000_000);
    const b = wideBatch(spec, 1000, 3, 1_000_000);
    expect(a).toEqual(b);
    const id = spec.find((c) => c.name === "id");
    const fk = spec.find((c) => c.name === "i20");
    expect(id && wideValue(id, 41, 100)).toBe(42);
    expect(fk && Number(wideValue(fk, 7, 100))).toBeLessThanOrEqual(100);
  });

  it("puts the spill shapes only on 1M-row tables, under both caps", () => {
    const w1m = analyticsCells(analyticsDataset("W1M")).map((c) => c.id);
    expect(w1m).toContain("W1M/spill_group_1m@64MiB");
    expect(w1m).toContain("W1M/spill_sort_wide_text@256MiB");
    expect(w1m).not.toContain("W1M/spill_group_1m");
    expect(analyticsCells(analyticsDataset("W500")).some((c) => c.query.family === "spill")).toBe(
      false,
    );
    expect(new Set(w1m).size).toBe(w1m.length);
  });

  it("substitutes row-relative offsets", () => {
    const query = WIDE_QUERIES.find((q) => q.id === "sort_deep_offset");
    expect(query && sqlFor(query, "minnow", 1_000_000)).toContain("OFFSET 900000");
  });
});

describe("browser lane baseline", () => {
  const observation = (
    minnowMs: number,
    referenceMs: number,
    extra: Partial<CellObservation["minnow"]> = {},
  ): CellObservation => ({
    cell: "W1M/q",
    reference: "duckdb",
    minnow: { ok: true, timing: timing(minnowMs), check: "match", ...extra },
    referenceOutcome: { ok: true, timing: timing(referenceMs) },
  });

  it("keys profiles by OS, architecture and Chromium major", () => {
    expect(browserPerfProfile("151.0.7000.1", "linux", "x64")).toBe("linux-x64-chromium151");
    expect(() => browserPerfProfile("nightly")).toThrow();
  });

  it("calibrates a ratio with bounded headroom, and a known failure with its class", () => {
    const cell = calibrate(observation(40, 10));
    expect(cell.status).toBe("completes");
    expect(cell.ratio).toBe(4);
    expect(cell.threshold).toBeGreaterThanOrEqual(6);
    expect(cell.threshold).toBeLessThanOrEqual(12);
    expect(calibrate({ ...observation(1, 1), minnow: { ok: false, failure: "budget" } })).toEqual({
      reference: "duckdb",
      status: "fails",
      failure: "budget",
    });
    expect(thresholdFor(timing(10), timing(10), true)).toBe(3);
  });

  it("flags only a ratio whose whole sampled range is past the threshold", () => {
    const baseline = {
      reference: "duckdb" as const,
      status: "completes" as const,
      ratio: 4,
      threshold: 6,
    };
    expect(evaluate(observation(50, 10), baseline).map((v) => v.kind)).toEqual(["ok"]);
    const slow = evaluate(observation(100, 10), baseline);
    expect(slow.map((v) => v.kind)).toEqual(["ratio-regression"]);
    expect(slow[0]?.confirmable).toBe(true);
  });

  it("treats status changes, reopen failures and wrong results differently", () => {
    const completes = { reference: "duckdb" as const, status: "completes" as const };
    const fails = {
      reference: "duckdb" as const,
      status: "fails" as const,
      failure: "budget" as const,
    };
    const failed = { ...observation(1, 1), minnow: { ok: false, failure: "timeout" as const } };
    expect(evaluate(failed, completes)[0]).toMatchObject({
      kind: "newly-failing",
      confirmable: true,
      fatal: false,
    });
    expect(evaluate(failed, fails)[0]).toMatchObject({
      kind: "failure-changed",
      confirmable: false,
      fatal: false,
    });
    expect(evaluate(observation(1, 1), fails)[0]?.kind).toBe("newly-completing");
    const corrupt = { ...failed, minnow: { ...failed.minnow, reopenFailed: true } };
    expect(evaluate(corrupt, fails)[0]).toMatchObject({ kind: "reopen-failure", fatal: true });
    expect(evaluate(observation(1, 1, { check: "mismatch" }), completes)[0]).toMatchObject({
      kind: "wrong-result",
      fatal: true,
    });
    expect(evaluate(observation(1, 1), undefined)[0]).toMatchObject({
      kind: "uncalibrated",
      fatal: true,
    });
  });

  it("confirms only flags that repeat in the re-run", () => {
    const flag = (cell: string) => ({
      cell,
      kind: "ratio-regression" as const,
      fatal: false,
      confirmable: true,
      message: "",
    });
    expect(confirmedFlags([flag("a"), flag("b")], [flag("b")]).map((v) => v.cell)).toEqual(["b"]);
  });

  it("merges one profile's cells and keeps targets and other profiles", () => {
    const parsed = parseBrowserPerfBaseline({
      schemaVersion: 1,
      targets: { "W1M/q": { gate: "B", ratio: 1 } },
      profiles: {
        other: {
          provisional: false,
          recorded: "x",
          cells: { "W1M/q": { reference: "duckdb", status: "completes", ratio: 2, threshold: 3 } },
        },
      },
    });
    const next = updatedBaseline(
      parsed,
      "mine",
      { "W1M/q": { reference: "duckdb", status: "fails", failure: "budget" } },
      { provisional: true, recorded: "y" },
    );
    expect(next.targets).toEqual(parsed.targets);
    expect(next.profiles.other).toEqual(parsed.profiles.other);
    expect(next.profiles.mine?.cells["W1M/q"]?.status).toBe("fails");
    expect(() =>
      parseBrowserPerfBaseline({
        schemaVersion: 1,
        profiles: { p: { cells: { c: { status: "maybe" } } } },
      }),
    ).toThrow();
  });

  it("reports progress toward a target without judging it", () => {
    const minnow = { ok: true, timing: timing(40) };
    const reference = { ok: true, timing: timing(10) };
    expect(targetProgress({ gate: "B", ratio: 1 }, minnow, reference, undefined)).toBe(
      "B: ≤1×: 4× to go",
    );
    expect(targetProgress({ gate: "point", ms: 50 }, minnow, reference, undefined)).toBe(
      "point: ≤50 ms: met",
    );
    expect(
      targetProgress({ gate: "A", completes: true }, { ok: false }, reference, undefined),
    ).toBe("not met: fails");
  });
});
