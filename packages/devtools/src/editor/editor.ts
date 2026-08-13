import { el } from "../dom.js";

/** Table name to its column names — everything the completion needs from the catalog. */
export type EditorSchema = Record<string, string[]>;

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
  /** Feeds the catalog to completion; the textarea ignores it. */
  setSchema(schema: EditorSchema): void;
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
    setSchema: () => undefined,
    destroy: () => undefined,
  };
}
