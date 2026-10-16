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
  /**
   * Counts what the statement will touch, shown as a fact line while the prompt is open — "12
   * rows" beside an UPDATE, so nobody confirms a WHERE clause that matches the whole table by
   * accident. It runs after the prompt appears and never delays it; a failure shows as such.
   */
  preview?: () => Promise<string>;
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
  const statement = el("div", { class: "plate statement" });
  const warning = el("div", { class: "dialog-warning" });
  const cancel = button("btn", "Cancel");
  const confirm = button("btn primary", "Confirm");
  const body = el("div", { class: "dialog-body" }, [facts, statement, warning]);
  const dialog = el("div", { class: "dialog", attrs: { role: "dialog", "aria-modal": "true" } }, [
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
  let returnFocus: HTMLElement | undefined;
  /** The rest of the panel, made inert while a prompt is open so nothing behind it is reachable. */
  let silenced: HTMLElement[] = [];

  /**
   * The prompt is modal to the panel, not to the page: everything else in the panel stops taking
   * focus and clicks, so Tab cannot wander behind the backdrop and Escape there cannot close the
   * panel out from under an unanswered question. The host page stays usable.
   */
  function silence(on: boolean): void {
    for (const sibling of silenced) sibling.inert = false;
    silenced = [];
    if (!on) return;
    for (const sibling of node.parentElement?.children ?? []) {
      if (sibling === node || !(sibling instanceof HTMLElement) || sibling.inert) continue;
      sibling.inert = true;
      silenced.push(sibling);
    }
  }

  function close(confirmed: boolean): void {
    if (settle === undefined) return;
    const resolve = settle;
    settle = undefined;
    node.hidden = true;
    silence(false);
    resolve(confirmed);
    if (returnFocus?.isConnected === true) returnFocus.focus();
    returnFocus = undefined;
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
    // Two buttons are the only things to reach; Tab cycles between them rather than leaving.
    if (event.key === "Tab") {
      event.preventDefault();
      const root = node.getRootNode() as ShadowRoot | Document;
      (root.activeElement === cancel ? confirm : cancel).focus();
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
        const root = node.getRootNode();
        returnFocus =
          root instanceof ShadowRoot && root.activeElement instanceof HTMLElement
            ? root.activeElement
            : undefined;
        heading.textContent = request.title;

        const rows = request.facts.map(([key, value]) =>
          el("div", { class: "plate-row" }, [
            el("span", { class: "plate-key", text: key }),
            el("span", { class: "plate-value", text: value }),
          ]),
        );
        const previewValue = el("span", { class: "plate-value", text: "counting…" });
        if (request.preview !== undefined) {
          rows.push(
            el("div", { class: "plate-row" }, [
              el("span", { class: "plate-key", text: "affects" }),
              previewValue,
            ]),
          );
          const mine = settle;
          request.preview().then(
            (text) => {
              if (settle === mine) previewValue.textContent = text;
            },
            (error: unknown) => {
              if (settle === mine) {
                previewValue.textContent = `could not count: ${error instanceof Error ? error.message : String(error)}`;
              }
            },
          );
        }
        facts.replaceChildren(...rows);
        facts.hidden = rows.length === 0;

        statement.replaceChildren(el("pre", { text: request.sql ?? "" }));
        statement.hidden = request.sql === undefined;

        warning.replaceChildren(icon(icons.warning), el("span", { text: request.warning ?? "" }));
        warning.hidden = request.warning === undefined;

        confirm.textContent = request.confirmLabel;
        confirm.className = request.destructive === true ? "btn danger" : "btn primary";

        node.hidden = false;
        silence(true);
        statement.scrollTop = 0;
        // Focus moves once the key press that opened the prompt is over. Moved during it, the
        // rest of that press — an Enter that submitted a form — lands on the button and answers
        // the prompt before it was ever seen. A destructive prompt starts on Cancel.
        const mine = settle;
        const first = request.destructive === true ? cancel : confirm;
        setTimeout(() => {
          if (settle === mine) first.focus({ preventScroll: true });
        }, 0);
      }),
  };
}
