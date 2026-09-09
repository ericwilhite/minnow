import { describe, expect, it } from "vitest";
import { ArtifactCache } from "./artifact-cache.js";

describe("artifact cache", () => {
  it("evicts the least-recently-used entry within its byte limit", () => {
    const cache = new ArtifactCache(250);
    cache.put("a", "first", 4);
    cache.put("b", "second", 4);
    expect(cache.get("a")).toBe("first");
    cache.put("c", "third", 4);

    expect(cache.get("b")).toBeUndefined();
    expect(cache.get("a")).toBe("first");
    expect(cache.get("c")).toBe("third");
    expect(cache.stats()).toEqual({
      limitBytes: 250,
      usedBytes: 204,
      entries: 2,
      hits: 3,
      misses: 1,
      evictions: 1,
    });
  });

  it("replaces entries without double-counting and ignores oversized values", () => {
    const cache = new ArtifactCache(106);
    cache.put("a", 1, 6);
    cache.put("a", 2, 3);
    cache.put("oversized", 3, 9);
    expect(cache.get("a")).toBe(2);
    expect(cache.stats().usedBytes).toBe(101);
    expect(cache.stats().entries).toBe(1);
  });

  it("validates limits and entry estimates", () => {
    expect(() => new ArtifactCache(-1)).toThrow(RangeError);
    const cache = new ArtifactCache(106);
    expect(() => cache.put("bad", null, 1.5)).toThrow(RangeError);
  });

  it("releases retained entries on clear without erasing lifetime counters", () => {
    const cache = new ArtifactCache(106);
    cache.put("a", 1, 3);
    expect(cache.get("a")).toBe(1);
    cache.clear();
    expect(cache.stats()).toMatchObject({ usedBytes: 0, entries: 0, hits: 1 });
  });

  it("charges long keys and zero-byte payloads, and validates oversized estimates", () => {
    const cache = new ArtifactCache(1024);
    cache.put("x".repeat(1024), null, 0);
    expect(cache.stats().entries).toBe(0);
    for (let i = 0; i < 100; i += 1) cache.put(String(i), null, 0);
    expect(cache.stats().entries).toBeLessThanOrEqual(10);
    expect(cache.stats().usedBytes).toBeLessThanOrEqual(1024);
    expect(cache.stats().evictions).toBeGreaterThan(0);
    expect(() => cache.put("invalid", null, Infinity)).toThrow(RangeError);
    expect(() => cache.put("invalid", null, Number.MAX_SAFE_INTEGER + 1)).toThrow(RangeError);
  });
});
