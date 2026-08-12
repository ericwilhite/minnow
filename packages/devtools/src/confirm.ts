import { button, el, icon, icons } from "./dom.js";

export interface ConfirmRequest {
  title: string;
  /** Label/value pairs spelling out exactly what will happen. */
  facts: ReadonlyArray<readonly [string, string]>;
  /** The statement itself, shown verbatim so nothing is hidden behind a summary. */
  sql?: string;
  warning?: string;
  confirmLabel: string;
  destructive?: boolean;
}

export interface ConfirmLayer {
  node: HTMLElement;
  /** Resolves true only if the person confirms; Escape, Cancel, and the backdrop all decline. */
  ask(request: ConfirmRequest): Promise<boolean>;
  /** Declines any open prompt — used when the panel closes out from under it. */
  dismiss(): void;
}

/**
 * The confirmation every change passes through. It is scoped to the panel rather than the page:
 * the backdrop covers the panel only, so the app underneath stays usable while a prompt is open.
 */
export function createConfirmLayer(): ConfirmLayer {
  const heading = el("h2");
  const facts = el("div", { class: "plate" });
  const statement = el("div", { class: "plate" });
  const warning = el("div", { class: "dialog-warning" });
  const cancel = button("btn", "Cancel");
  const confirm = button("btn primary", "Confirm");
  const body = el("div", { class: "dialog-body" }, [facts, statement, warning]);
  const dialog = el("div", { class: "dialog", attrs: { role: "dialog", "aria-modal": "false" } }, [
    el("div", { class: "dialog-head" }, [heading]),
    body,
    el("div", { class: "dialog-foot" }, [
      el("span", { class: "dialog-note", text: "This cannot be undone" }),
      el("span", { class: "spacer" }),
      cancel,
      confirm,
    ]),
  ]);
  const node = el("div", { class: "scrim" }, [dialog]);
  node.hidden = true;
  dialog.setAttribute("aria-labelledby", "mdt-confirm-title");
  heading.id = "mdt-confirm-title";

  let settle: ((confirmed: boolean) => void) | undefined;

  function close(confirmed: boolean): void {
    if (settle === undefined) return;
    const resolve = settle;
    settle = undefined;
    node.hidden = true;
    resolve(confirmed);
  }

  cancel.addEventListener("click", () => {
    close(false);
  });
  confirm.addEventListener("click", () => {
    close(true);
  });
  node.addEventListener("click", (event) => {
    if (event.target === node) close(false);
  });
  node.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      event.stopPropagation();
      close(false);
    }
  });

  return {
    node,
    dismiss: () => {
      close(false);
    },
    ask: async (request) =>
      new Promise<boolean>((resolve) => {
        // A second prompt while one is open would strand the first; decline it instead.
        close(false);
        settle = resolve;
        heading.textContent = request.title;

        facts.replaceChildren(
          ...request.facts.map(([key, value]) =>
            el("div", { class: "plate-row" }, [
              el("span", { class: "plate-key", text: key }),
              el("span", { class: "plate-value", text: value }),
            ]),
          ),
        );
        facts.hidden = request.facts.length === 0;

        statement.replaceChildren(el("pre", { text: request.sql ?? "" }));
        statement.hidden = request.sql === undefined;

        warning.replaceChildren(icon(icons.warning), el("span", { text: request.warning ?? "" }));
        warning.hidden = request.warning === undefined;

        confirm.textContent = request.confirmLabel;
        confirm.className = request.destructive === true ? "btn danger" : "btn primary";

        node.hidden = false;
        confirm.focus();
      }),
  };
}
