import type { QueryValue } from "@minnowdb/core";
import { button, el } from "../dom.js";
import { parseInput } from "../values.js";
import type { ColumnInfo, TableInfo } from "./catalog.js";
import {
  describeFilter,
  isComplete,
  operatorArity,
  operatorHint,
  operatorLabel,
  operatorsFor,
  type Filter,
  type FilterOperator,
} from "./filters.js";

export interface FilterBar {
  node: HTMLElement;
  setTable(table: TableInfo | undefined): void;
  filters(): Filter[];
  /** Adds a filter from outside — a cell's "filter to this value" — and reports the change. */
  add(filter: Filter): void;
  /** Replaces the filters without reporting a change, for a table opened already filtered. */
  setFilters(filters: readonly Filter[]): void;
}

type Parsed = { ok: true; values: QueryValue[] } | { ok: false; message: string };

/**
 * The typed values for one filter, or the first reason one of them cannot be read. A value is
 * parsed as the column is typed — `abc` in a number column is refused here, in the editor, rather
 * than compiled into `amount = NULL` and matching nothing without a word. Blank means "no value
 * yet", never NULL: the `is null` operator is how NULL is asked for.
 */
export function parseFilterValues(
  column: ColumnInfo,
  operator: FilterOperator,
  first: string,
  second: string,
): Parsed {
  const arity = operatorArity(operator);
  if (arity === 0) return { ok: true, values: [] };
  const parts = arity === "many" ? first.split(",") : arity === 2 ? [first, second] : [first];
  const values: QueryValue[] = [];
  for (const part of parts) {
    if (part.trim().length === 0) {
      if (arity === "many") continue;
      return { ok: false, message: arity === 2 ? "Both values are needed." : "Enter a value." };
    }
    const parsed = parseInput(part, column.type, false, column.enumValues);
    if (!parsed.ok) return { ok: false, message: parsed.message };
    values.push(parsed.value);
  }
  if (values.length === 0) return { ok: false, message: "Enter at least one value." };
  return { ok: true, values };
}

/**
 * The filter row: pick a column, pick a comparison, type a value. Values are typed here rather
 * than at the SQL layer, so `score > 10` compares numbers and never strings.
 */
export function createFilterBar(onChange: () => void): FilterBar {
  const chips = el("div", { class: "chips" });
  const columnSelect = el("select", { class: "mini", attrs: { "aria-label": "Filter column" } });
  const operatorSelect = el("select", { class: "mini", attrs: { "aria-label": "Comparison" } });
  const valueInput = el("input", {
    class: "mini value",
    type: "text",
    attrs: { placeholder: "value", "aria-label": "Value" },
  });
  const secondInput = el("input", {
    class: "mini value",
    type: "text",
    attrs: { placeholder: "and", "aria-label": "Second value" },
  });
  const add = button("btn mini", "Add");
  const error = el("span", { class: "filter-error", attrs: { role: "alert" } });
  error.hidden = true;
  const editor = el("div", { class: "filter-editor" }, [
    columnSelect,
    operatorSelect,
    valueInput,
    secondInput,
    add,
    error,
  ]);
  editor.hidden = true;

  const open = button("btn mini dashed", "+ filter");
  const node = el("div", { class: "filters" }, [chips, open, editor]);

  let table: TableInfo | undefined;
  let active: Filter[] = [];

  function currentColumn(): ColumnInfo | undefined {
    return table?.columns.find(({ name }) => name === columnSelect.value);
  }

  function showError(message: string | undefined): void {
    error.textContent = message ?? "";
    error.hidden = message === undefined;
  }

  function renderOperators(): void {
    const type = currentColumn()?.type ?? "string";
    const available = operatorsFor(type);
    operatorSelect.replaceChildren(
      ...available.map((operator) => {
        const option = el("option", { text: operatorLabel(operator) });
        option.value = operator;
        return option;
      }),
    );
    renderValueInputs();
  }

  function renderValueInputs(): void {
    const operator = operatorSelect.value as FilterOperator;
    const arity = operatorArity(operator);
    valueInput.hidden = arity === 0;
    secondInput.hidden = arity !== 2;
    // The hint carries the shape — `%crea%` for a raw pattern, a bare word for `contains` — so the
    // difference between the two is visible before anything is typed.
    valueInput.placeholder = operatorHint(operator) ?? (arity === "many" ? "a, b, c" : "value");
    showError(undefined);
  }

  function renderChips(): void {
    chips.replaceChildren(
      ...active.map((filter, index) => {
        const remove = button("chip-x", "×", { title: "Remove filter" });
        remove.addEventListener("click", () => {
          active.splice(index, 1);
          renderChips();
          onChange();
        });
        return el("span", { class: "chip" }, [
          el("span", { text: describeFilter(filter) }),
          remove,
        ]);
      }),
    );
  }

  function submit(): void {
    const column = currentColumn();
    if (table === undefined || column === undefined) return;
    const operator = operatorSelect.value as FilterOperator;
    const parsed = parseFilterValues(column, operator, valueInput.value, secondInput.value);
    if (!parsed.ok) {
      showError(parsed.message);
      valueInput.focus();
      return;
    }
    const filter: Filter = {
      column: column.name,
      type: column.type,
      operator,
      values: parsed.values,
    };
    if (!isComplete(filter)) return;
    active.push(filter);
    valueInput.value = "";
    secondInput.value = "";
    editor.hidden = true;
    showError(undefined);
    renderChips();
    onChange();
  }

  open.addEventListener("click", () => {
    editor.hidden = !editor.hidden;
    if (!editor.hidden) valueInput.focus();
  });
  columnSelect.addEventListener("change", renderOperators);
  operatorSelect.addEventListener("change", renderValueInputs);
  add.addEventListener("click", submit);
  for (const input of [valueInput, secondInput]) {
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") submit();
    });
    input.addEventListener("input", () => {
      showError(undefined);
    });
  }

  return {
    node,
    filters: () => active.filter(isComplete),
    add: (filter) => {
      active.push(filter);
      renderChips();
      onChange();
    },
    setFilters: (filters) => {
      active = [...filters];
      renderChips();
    },
    setTable: (next) => {
      table = next;
      active = [];
      renderChips();
      showError(undefined);
      columnSelect.replaceChildren(
        ...(next?.columns ?? []).map((column) => {
          const option = el("option", { text: column.name });
          option.value = column.name;
          return option;
        }),
      );
      renderOperators();
      node.hidden = next === undefined;
    },
  };
}
