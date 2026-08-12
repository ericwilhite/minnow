import { describe, expect, it } from "vitest";
import { optionsFromAttributes, resolveOptions } from "./options.js";

function attributes(values: Record<string, string>): { getAttribute(name: string): string | null } {
  return { getAttribute: (name) => values[name] ?? null };
}

describe("resolveOptions", () => {
  it("defaults to a closed launcher in the bottom-right with writes enabled", () => {
    expect(resolveOptions()).toEqual({
      mode: "launcher",
      corner: "bottom-right",
      hotkey: "mod+shift+d",
      defaultOpen: false,
      zIndex: 2_147_483_000,
      write: true,
      initialQuery: "",
      storageKey: "minnow-devtools",
    });
  });

  it("keeps inline panels open, since nothing else can open them", () => {
    expect(resolveOptions({ mode: "inline", defaultOpen: false }).defaultOpen).toBe(true);
  });

  it("falls back rather than trusting an unknown mode or corner", () => {
    const resolved = resolveOptions({
      mode: "floating" as never,
      corner: "middle" as never,
    });
    expect(resolved.mode).toBe("launcher");
    expect(resolved.corner).toBe("bottom-right");
  });

  it("treats write as opt-out, not opt-in", () => {
    expect(resolveOptions({ permissions: {} }).write).toBe(true);
    expect(resolveOptions({ permissions: { write: false } }).write).toBe(false);
  });

  it("ignores a non-numeric z-index instead of stacking at NaN", () => {
    expect(resolveOptions({ zIndex: Number.NaN }).zIndex).toBe(2_147_483_000);
    expect(resolveOptions({ zIndex: 10 }).zIndex).toBe(10);
  });
});

describe("optionsFromAttributes", () => {
  it("reads every attribute form", () => {
    expect(
      optionsFromAttributes(
        attributes({
          mode: "inline",
          corner: "top-left",
          hotkey: "mod+k",
          "z-index": "42",
          write: "false",
          "initial-query": "SELECT 1",
          "storage-key": "docs",
        }),
      ),
    ).toEqual({
      mode: "inline",
      corner: "top-left",
      hotkey: "mod+k",
      zIndex: 42,
      permissions: { write: false },
      initialQuery: "SELECT 1",
      storageKey: "docs",
    });
  });

  it("omits absent attributes so they do not override explicit options", () => {
    expect(optionsFromAttributes(attributes({}))).toEqual({});
  });

  it("reads a bare attribute as true", () => {
    expect(optionsFromAttributes(attributes({ open: "" })).defaultOpen).toBe(true);
    expect(optionsFromAttributes(attributes({ open: "false" })).defaultOpen).toBe(false);
    expect(optionsFromAttributes(attributes({ write: "" })).permissions).toEqual({ write: true });
  });
});
