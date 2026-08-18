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
      theme: "system",
      height: "",
    });
  });

  it("takes a height as pixels or as any CSS length", () => {
    expect(resolveOptions({ height: 400 }).height).toBe("400px");
    expect(resolveOptions({ height: "400" }).height).toBe("400px");
    expect(resolveOptions({ height: "70vh" }).height).toBe("70vh");
    expect(resolveOptions({ height: "100%" }).height).toBe("100%");
    // Nothing usable leaves the container in charge rather than writing an invalid length.
    expect(resolveOptions({ height: Number.NaN }).height).toBe("");
    expect(resolveOptions({ height: "  " }).height).toBe("");
  });

  it("falls back to the system palette for an unknown theme", () => {
    expect(resolveOptions({ theme: "dark" }).theme).toBe("dark");
    expect(resolveOptions({ theme: "midnight" as never }).theme).toBe("system");
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

  it("leaves an inline panel in the page's stacking order", () => {
    // The overlay's z-index on a panel in document flow paints it over the host page's own
    // sticky header as soon as the reader scrolls.
    expect(resolveOptions({ mode: "inline" }).zIndex).toBe(0);
    expect(resolveOptions({ mode: "inline", zIndex: 5 }).zIndex).toBe(5);
    expect(resolveOptions({ mode: "launcher" }).zIndex).toBe(2_147_483_000);
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
          theme: "dark",
          height: "70vh",
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
      theme: "dark",
      height: "70vh",
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
