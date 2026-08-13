import type { QueryValue } from "@minnowdb/core";
import { button, el } from "../dom.js";
import { formatForInput, inputHint, parseInput } from "../values.js";
import type { TableInfo } from "./catalog.js";

export interface InsertForm {
  node: HTMLElement;
  open(table: TableInfo): void;
  close(): void;
  isOpen(): boolean;
}

export interface InsertFormDeps {
  /** Called with the parsed row once every field is valid. */
  onSubmit(values: Record<string, QueryValue>): void;
}

/**
 * One input per column, typed as the column is typed. Values are parsed before the confirmation
 * appears, so a bad number is caught in the form rather than surfacing as an engine error after
 * someone has already agreed to the write.
 */
export function createInsertForm(deps: InsertFormDeps): InsertForm {
  const fields = el("div", { class: "insert-fields" });
  const error = el("div", { class: "insert-error" });
  error.hidden = true;
  const save = button("btn primary", "Review insert");
  const cancel = button("btn", "Cancel");
  const heading = el("h3", { class: "insert-title" });

  const node = el("div", { class: "insert-sheet" }, [
    el("div", { class: "insert-head" }, [heading]),
    fields,
    error,
    el("div", { class: "insert-foot" }, [
      el("span", { class: "insert-note", text: "Blank means NULL where the column allows it" }),
      el("span", { class: "spacer" }),
      cancel,
      save,
    ]),
  ]);
  node.hidden = true;

  let table: TableInfo | undefined;
  let inputs: Array<{ column: TableInfo["columns"][number]; input: HTMLInputElement }> = [];

  function close(): void {
    node.hidden = true;
    table = undefined;
    inputs = [];
    error.hidden = true;
  }

  function submit(): void {
    if (table === undefined) return;
    const values: Record<string, QueryValue> = {};
    for (const { column, input } of inputs) {
      // A column the engine fills is left out of the row entirely when blank, rather than being
      // sent as NULL — that is what lets it choose the value.
      if (column.hasDefault === true && input.value.trim().length === 0) continue;
      const parsed = parseInput(input.value, column.type, column.nullable);
      if (!parsed.ok) {
        error.textContent = `${column.name}: ${parsed.message}`;
        error.hidden = false;
        input.focus();
        return;
      }
      values[column.name] = parsed.value;
    }
    error.hidden = true;
    deps.onSubmit(values);
  }

  save.addEventListener("click", submit);
  cancel.addEventListener("click", close);
  node.addEventListener("keydown", (event) => {
    if (event.key === "Escape") close();
    if (event.key === "Enter") submit();
  });

  return {
    node,
    isOpen: () => !node.hidden,
    close,
    open: (next) => {
      table = next;
      error.hidden = true;
      heading.textContent = `New row in ${next.name}`;
      inputs = next.columns.map((column) => {
        const input = el("input", {
          class: "insert-input",
          type: "text",
          attrs: { spellcheck: "false", "aria-label": column.name },
        });
        input.placeholder =
          column.hasDefault === true
            ? "set automatically"
            : inputHint(column.type, column.nullable);
        input.value = formatForInput(null);
        return { column, input };
      });
      fields.replaceChildren(
        ...inputs.map(({ column, input }) =>
          el("label", { class: "insert-field" }, [
            el("span", { class: "insert-label" }, [
              el("span", { class: "insert-name", text: column.name }),
              el("span", {
                class: "insert-type",
                text: [
                  column.type,
                  column.isUniqueKey ? "key" : "",
                  column.hasDefault === true ? "auto" : "",
                ]
                  .filter((part) => part.length > 0)
                  .join(" · "),
              }),
            ]),
            input,
          ]),
        ),
      );
      node.hidden = false;
      inputs[0]?.input.focus();
    },
  };
}
