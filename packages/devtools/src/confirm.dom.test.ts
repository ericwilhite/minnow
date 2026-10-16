// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";
import { createConfirmLayer } from "./confirm.js";
import { el } from "./dom.js";

function mount() {
  const host = el("div");
  const root = host.attachShadow({ mode: "open" });
  document.body.append(host);
  const behind = el("div", { class: "panel-body" }, [el("button", { text: "behind" })]);
  const layer = createConfirmLayer();
  const panel = el("div", { class: "panel" }, [behind, layer.node]);
  root.append(panel);
  const buttons = () => [...layer.node.querySelectorAll("button")];
  return { root, layer, behind, buttons };
}

const request = { title: "Delete", facts: [], confirmLabel: "Delete row" } as const;

afterEach(() => {
  vi.useRealTimers();
  document.body.replaceChildren();
});

describe("confirm layer", () => {
  it("moves focus only after the key press that opened it is over", () => {
    // Focused during the press, the rest of an Enter that submitted a form lands on the button
    // and answers the prompt unseen; the browser's own activation cannot run in happy-dom, so
    // what is pinned here is that nothing is focused until the press has ended.
    vi.useFakeTimers();
    const { root, layer } = mount();
    void layer.ask(request);
    expect(root.activeElement).toBeNull();
    vi.runAllTimers();
    expect(root.activeElement?.textContent).toBe("Delete row");
  });

  it("starts a destructive prompt on Cancel", () => {
    vi.useFakeTimers();
    const { root, layer } = mount();
    void layer.ask({ ...request, destructive: true });
    vi.runAllTimers();
    expect(root.activeElement?.textContent).toBe("Cancel");
  });

  it("keeps Tab between its buttons and makes the rest of the panel inert while open", async () => {
    vi.useFakeTimers();
    const { root, layer, behind, buttons } = mount();
    const answer = layer.ask(request);
    vi.runAllTimers();
    expect(behind.inert).toBe(true);
    const tab = () =>
      root.activeElement?.dispatchEvent(
        new KeyboardEvent("keydown", { key: "Tab", bubbles: true, cancelable: true }),
      );
    tab();
    expect(root.activeElement?.textContent).toBe("Cancel");
    tab();
    expect(root.activeElement?.textContent).toBe("Delete row");
    buttons()
      .find((button) => button.textContent === "Cancel")
      ?.click();
    await expect(answer).resolves.toBe(false);
    expect(behind.inert).toBe(false);
  });
});
