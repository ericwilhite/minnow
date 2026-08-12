import type { QueryValue } from "@minnowdb/core";
import { button, el } from "../dom.js";
import type { ColumnType } from "../sql/literal.js";
import { parseInput } from "../values.js";
import type { TableInfo } from "./catalog.js";
import {
  describeFilter,
  isComplete,
  operatorArity,
  operatorLabel,
  operatorsFor,
  type Filter,
  type FilterOperator,
} from "./filters.js";

export interface FilterBar {
  node: HTMLElement;
  setTable(table: TableInfo | undefined): void;
  filters(): Filter[];
  clear(): void;
}

/**
 * A filter value is always optional — a blank box means "no filter yet", not "match NULL", which
 * is what the `is null` operator is for. So parsing treats every column as nullable and the
 * incomplete filter is simply dropped.
 */
function parseValue(text: string, type: ColumnType): QueryValue {
  const parsed = parseInput(text, type, true);
  return parsed.ok ? parsed.value : null;
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
  const editor = el("div", { class: "filter-editor" }, [
    columnSelect,
    operatorSelect,
    valueInput,
    secondInput,
    add,
  ]);
  editor.hidden = true;

  const open = button("btn mini dashed", "+ filter");
  const node = el("div", { class: "filters" }, [chips, open, editor]);

  let table: TableInfo | undefined;
  let active: Filter[] = [];

  function currentType(): ColumnType {
    return table?.columns.find(({ name }) => name === columnSelect.value)?.type ?? "string";
  }

  function renderOperators(): void {
    const type = currentType();
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
    const arity = operatorArity(operatorSelect.value as FilterOperator);
    valueInput.hidden = arity === 0;
    secondInput.hidden = arity !== 2;
    valueInput.placeholder = arity === "many" ? "a, b, c" : "value";
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
    if (table === undefined) return;
    const column = table.columns.find(({ name }) => name === columnSelect.value);
    if (column === undefined) return;
    const operator = operatorSelect.value as FilterOperator;
    const arity = operatorArity(operator);
    const values =
      arity === 0
        ? []
        : arity === "many"
          ? valueInput.value
              .split(",")
              .map((part) => parseValue(part, column.type))
              .filter((value) => value !== null)
          : arity === 2
            ? [
                parseValue(valueInput.value, column.type),
                parseValue(secondInput.value, column.type),
              ]
            : [parseValue(valueInput.value, column.type)];

    const filter: Filter = { column: column.name, type: column.type, operator, values };
    if (!isComplete(filter)) return;
    active.push(filter);
    valueInput.value = "";
    secondInput.value = "";
    editor.hidden = true;
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
  }

  return {
    node,
    filters: () => active.filter(isComplete),
    clear: () => {
      active = [];
      editor.hidden = true;
      renderChips();
    },
    setTable: (next) => {
      table = next;
      active = [];
      renderChips();
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
