import { el } from "../dom.js";

/** Table name to its column names — everything the completion needs from the catalog. */
export type EditorSchema = Record<string, string[]>;

/**
 * Names inserted from the rail are separated from whatever precedes them, so clicking two of them
 * in a row produces `people people` rather than `peoplepeople`. Punctuation that legitimately
 * abuts a name — an opening paren, a comma, a dot — is left alone.
 */
export function spaced(before: string, text: string): string {
  const previous = before.slice(-1);
  const abuts = previous === "" || /[\s(.,]/.test(previous);
  return abuts ? text : ` ${text}`;
}

export interface SqlEditorDeps {
  initial: string;
  /** Cmd/Ctrl + Enter, wherever the caret is. */
  onRun(): void;
}

/**
 * What the console drives. Both the plain textarea and the CodeMirror editor satisfy it, so the
 * console never learns which one it has and the upgrade is invisible to everything downstream.
 */
export interface SqlEditor {
  node: HTMLElement;
  value(): string;
  setValue(text: string): void;
  focus(): void;
  /** Puts the caret over a range — how a located compile error is shown. */
  selectRange(from: number, to: number): void;
  /** Inserts at the caret, replacing any selection, and leaves the caret after it. */
  insert(text: string): void;
  /** The selected text, empty when nothing is selected — what "run the selection" runs. */
  selectedText(): string;
  /** Where the selection starts, so an error inside a run selection can be pointed at. */
  selectionStart(): number;
  /** Feeds the catalog to completion; the textarea ignores it. */
  setSchema(schema: EditorSchema): void;
  /** Repaints for a palette switch; the textarea is styled by tokens and ignores it. */
  setDark(dark: boolean): void;
  destroy(): void;
}

/**
 * The editor the panel opens with: no dependency, no load, no wait. It stays the editor entirely
 * if CodeMirror fails to load, so a blocked chunk costs syntax highlighting rather than the panel.
 */
export function createTextareaEditor(deps: SqlEditorDeps): SqlEditor {
  const node = el("textarea", {
    class: "editor",
    attrs: { spellcheck: "false", "aria-label": "SQL", placeholder: "SELECT * FROM …" },
  });
  node.value = deps.initial;

  node.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      deps.onRun();
    }
  });

  return {
    node,
    value: () => node.value,
    setValue: (text) => {
      node.value = text;
    },
    focus: () => {
      node.focus();
    },
    selectRange: (from, to) => {
      node.focus();
      node.setSelectionRange(from, to);
    },
    insert: (text) => {
      const start = node.selectionStart;
      const end = node.selectionEnd;
      const insert = spaced(node.value.slice(0, start), text);
      node.value = `${node.value.slice(0, start)}${insert}${node.value.slice(end)}`;
      node.focus();
      node.setSelectionRange(start + insert.length, start + insert.length);
    },
    selectedText: () => node.value.slice(node.selectionStart, node.selectionEnd),
    selectionStart: () => node.selectionStart,
    setSchema: () => undefined,
    setDark: () => undefined,
    destroy: () => undefined,
  };
}
