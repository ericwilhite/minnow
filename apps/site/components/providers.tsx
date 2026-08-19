"use client";
/**
 * The site's providers, and one correction to the theme shortcut.
 *
 * Fumadocs toggles light and dark on a bare `d`, and decides whether the reader was typing by
 * looking at the tag name of the event's target. That test does not survive either console on
 * this site. CodeMirror lives in a shadow root, so by the time the event reaches the window it
 * has been retargeted to the plain host `<div>`; Monaco writes through an `EditContext` on a
 * `<div>` that is not `contentEditable`. Both look like "not typing", so a `d` in a query — in
 * `discount`, `id`, `product_id` — flipped the whole page from under the reader mid-word.
 *
 * `composedPath()` answers the question the shortcut meant to ask. It lists the real nodes the
 * event passed through, shadow roots included, so the editable element is visible whichever
 * console the keystroke came from.
 */
import type { ReactNode } from "react";
import { RootProvider } from "fumadocs-ui/provider/next";
import SearchDialog from "@/components/search";

/** Editors that take keystrokes without presenting an element the DOM calls editable. */
const EDITORS = ["monaco-editor", "cm-editor"];

function typing(event: KeyboardEvent): boolean {
  return event.composedPath().some((node) => {
    if (!(node instanceof HTMLElement)) return false;
    if (node.isContentEditable) return true;
    if (["INPUT", "TEXTAREA", "SELECT"].includes(node.tagName)) return true;
    return EDITORS.some((editor) => node.classList.contains(editor));
  });
}

/** Fumadocs' own `d`, minus the keystrokes that were meant for an editor. */
function themeHotKey(event: KeyboardEvent): boolean {
  if (event.metaKey || event.ctrlKey || event.altKey) return false;
  if (event.key.toLowerCase() !== "d") return false;
  return !typing(event);
}

export function Providers({ children }: { children: ReactNode }) {
  return (
    <RootProvider search={{ SearchDialog }} theme={{ hotKey: themeHotKey }}>
      {children}
    </RootProvider>
  );
}
