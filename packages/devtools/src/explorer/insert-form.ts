import type { QueryValue } from "@minnowdb/core";
import { button, el } from "../dom.js";
import { formatForInput, inputHint, parseInput } from "../values.js";
import type { ColumnInfo, TableInfo } from "./catalog.js";

export interface InsertForm {
  node: HTMLElement;
  open(table: TableInfo, initial?: Record<string, QueryValue>): void;
  close(): void;
}

export interface InsertFormDeps {
  /** Called with the parsed row once every field is valid. */
  onSubmit(values: Record<string, QueryValue>): void;
}

/** The control for one column: a menu where the values are a closed set, a box otherwise. */
export function fieldFor(column: ColumnInfo): HTMLInputElement | HTMLSelectElement {
  const choices = column.enumValues ?? (column.type === "boolean" ? ["true", "false"] : undefined);
  if (choices === undefined) {
    const input = el("input", {
      class: "insert-input",
      type: "text",
      attrs: { spellcheck: "false", "aria-label": column.name },
    });
    input.placeholder =
      column.hasDefault === true ? "set automatically" : inputHint(column.type, column.nullable);
    return input;
  }
  const select = el("select", { class: "insert-input", attrs: { "aria-label": column.name } });
  // The blank choice is what "leave it to the default" and NULL both look like; which one it
  // means follows the column, the same way a blank box does.
  const blank = el("option", {
    text:
      column.hasDefault === true ? "(set automatically)" : column.nullable ? "(NULL)" : "(choose)",
  });
  blank.value = "";
  select.append(blank, ...choices.map((choice) => el("option", { text: choice })));
  return select;
}

/**
 * One input per column, typed as the column is typed. Values are parsed before the confirmation
 * appears, so a bad number is caught in the form rather than surfacing as an engine error after
 * someone has already agreed to the write. A generated column is shown but never asked for: the
 * engine computes it from the row.
 */
export function createInsertForm(deps: InsertFormDeps): InsertForm {
  const fields = el("div", { class: "insert-fields" });
  const error = el("div", { class: "insert-error", attrs: { role: "alert" } });
  error.hidden = true;
  const save = button("btn primary", "Review insert");
  const cancel = button("btn", "Cancel");
  const heading = el("h3", { class: "insert-title", attrs: { id: "mdt-insert-title" } });

  const node = el(
    "div",
    {
      class: "insert-sheet",
      attrs: { role: "dialog", "aria-modal": "false", "aria-labelledby": "mdt-insert-title" },
    },
    [
      el("div", { class: "insert-head" }, [heading]),
      fields,
      error,
      el("div", { class: "insert-foot" }, [
        el("span", { class: "insert-note", text: "Blank means NULL where the column allows it" }),
        el("span", { class: "spacer" }),
        cancel,
        save,
      ]),
    ],
  );
  node.hidden = true;

  let table: TableInfo | undefined;
  let inputs: Array<{ column: ColumnInfo; input: HTMLInputElement | HTMLSelectElement }> = [];

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
      const parsed = parseInput(input.value, column.type, column.nullable, column.enumValues);
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
    if (event.key === "Escape") {
      // Closing the form is the whole of what Escape does here; the panel behind it must not
      // read the same press as its own dismissal.
      event.preventDefault();
      event.stopPropagation();
      close();
    }
    if (event.key === "Enter" && !(event.target instanceof HTMLSelectElement)) submit();
  });

  return {
    node,
    close,
    open: (next, initial = {}) => {
      table = next;
      error.hidden = true;
      heading.textContent = `New row in ${next.name}`;
      inputs = next.columns
        .filter((column) => column.generated === undefined)
        .map((column) => {
          const input = fieldFor(column);
          input.value = formatForInput(initial[column.name] ?? null);
          return { column, input };
        });
      const computed = next.columns.filter((column) => column.generated !== undefined);
      fields.replaceChildren(
        ...inputs.map(({ column, input }) =>
          el("label", { class: "insert-field" }, [
            el("span", { class: "insert-label" }, [
              el("span", { class: "insert-name", text: column.name }),
              el("span", {
                class: "insert-type",
                text: [
                  column.typeLabel ?? column.type,
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
        ...computed.map((column) =>
          el("div", { class: "insert-field" }, [
            el("span", { class: "insert-label" }, [
              el("span", { class: "insert-name", text: column.name }),
              el("span", { class: "insert-type", text: "generated" }),
            ]),
            el("code", { class: "insert-generated", text: column.generated ?? "" }),
          ]),
        ),
      );
      node.hidden = false;
      inputs[0]?.input.focus();
    },
  };
}
