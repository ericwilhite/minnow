// @vitest-environment happy-dom
import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { afterEach, describe, expect, it } from "vitest";
import { defineMinnowDevtools, type MinnowDevtoolsElement } from "./element.js";

defineMinnowDevtools();

const databases: MinnowDatabase[] = [];
afterEach(async () => {
  document.body.replaceChildren();
  for (const database of databases.splice(0)) await database.close();
});

function mount(): MinnowDevtoolsElement {
  const database = new MinnowDatabase(new MemoryBlockStore());
  databases.push(database);
  const element = document.createElement("minnow-devtools") as MinnowDevtoolsElement;
  element.setAttribute("initial-query", "SELECT 1");
  element.target = database;
  document.body.append(element);
  return element;
}

const panel = (element: MinnowDevtoolsElement) =>
  element.shadowRoot?.querySelector<HTMLElement>(".panel") ?? null;

describe("<minnow-devtools>", () => {
  it("opens and closes in place when `open` changes, keeping the panel it built", () => {
    const element = mount();
    const built = panel(element);
    element.setAttribute("open", "");
    expect(panel(element)).toBe(built);
    expect(built?.hidden).toBe(false);
    element.setAttribute("open", "false");
    expect(panel(element)).toBe(built);
    expect(built?.hidden).toBe(true);
  });

  it("does not rebuild for options that say the same thing again", () => {
    const element = mount();
    element.options = { permissions: { write: false } };
    const built = panel(element);
    element.options = { permissions: { write: false } };
    expect(panel(element)).toBe(built);
    element.options = { permissions: { write: true } };
    expect(panel(element)).not.toBe(built);
  });

  it("adopts one stylesheet however many times it remounts", () => {
    const element = mount();
    const root = element.shadowRoot;
    if (root === null) throw new Error("no shadow root");
    const sheets = () => root.adoptedStyleSheets.length + root.querySelectorAll("style").length;
    const first = sheets();
    for (const corner of ["top-left", "top-right", "bottom-left"]) {
      element.setAttribute("corner", corner);
    }
    element.remove();
    document.body.append(element);
    expect(first).toBe(1);
    expect(sheets()).toBe(1);
  });
});
