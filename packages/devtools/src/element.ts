import { createDevtools, type DevtoolsHandle } from "./devtools.js";
import {
  optionsFromAttributes,
  type DevtoolsOptions,
  type DevtoolsTheme,
  themeFromAttribute,
} from "./options.js";
import type { DevtoolsAttachable } from "./target.js";

export const elementName = "minnow-devtools";

/** Every attribute the element reads, so a change to any of them after connect takes effect. */
const observedAttributes = [
  "mode",
  "corner",
  "hotkey",
  "open",
  "z-index",
  "write",
  "initial-query",
  "storage-key",
  "theme",
  "height",
] as const;

/**
 * `<minnow-devtools>`. A custom element rather than a framework component: it works unchanged in
 * React, Vue, Svelte, Solid, Astro, and plain HTML, and its shadow root keeps the host page's
 * styles out. Set the database with the `target` property (a `MinnowDatabase`,
 * `MinnowDatabaseClient`, or `Minnow` facade); everything else reads from attributes.
 *
 * Attributes are live: a framework that re-renders `hotkey` or `corner` remounts the panel with
 * the new value, while `theme` repaints and `open` opens or closes it in place, so the query being
 * written survives either.
 */
export class MinnowDevtoolsElement extends HTMLElement {
  static get observedAttributes(): readonly string[] {
    return observedAttributes;
  }

  readonly #root: ShadowRoot;
  #handle: DevtoolsHandle | undefined;
  #target: DevtoolsAttachable | undefined;
  /** Set before the element upgrades or connects; applied when it mounts. */
  #options: DevtoolsOptions = {};
  /**
   * Mounting writes the `theme` attribute back onto the host, which would otherwise read as a
   * change from outside and mount again; attribute callbacks are ignored while it runs.
   */
  #mounting = false;

  constructor() {
    super();
    this.#root = this.attachShadow({ mode: "open" });
  }

  /** The database, worker client, or facade the panel drives. Assigning it remounts. */
  get target(): DevtoolsAttachable | undefined {
    return this.#target;
  }

  set target(value: DevtoolsAttachable | undefined) {
    this.#target = value;
    if (this.isConnected) this.#mount();
  }

  /** Options that have no attribute form, merged over the ones read from attributes. */
  get options(): DevtoolsOptions {
    return this.#options;
  }

  set options(value: DevtoolsOptions) {
    // A framework hands over a fresh object on every render; one that says the same thing must
    // not rebuild the panel and throw away the query being written.
    if (sameOptions(this.#options, value)) return;
    this.#options = value;
    if (this.isConnected) this.#mount();
  }

  open(): void {
    this.#handle?.open();
  }

  close(): void {
    this.#handle?.close();
  }

  toggle(): void {
    this.#handle?.toggle();
  }

  /** Same as writing the `theme` attribute, for a page that switches themes from script. */
  setTheme(theme: DevtoolsTheme): void {
    this.#handle?.setTheme(theme);
  }

  connectedCallback(): void {
    this.#mount();
  }

  disconnectedCallback(): void {
    this.#handle?.destroy();
    this.#handle = undefined;
  }

  attributeChangedCallback(name: string, previous: string | null, next: string | null): void {
    if (this.#mounting || !this.isConnected || this.#handle === undefined || previous === next) {
      return;
    }
    // The palette is the one option that can change without rebuilding anything, and the one a
    // page flips while a query is half-written; everything else describes how the panel was
    // built and takes a remount.
    if (name === "theme") this.#handle.setTheme(themeFromAttribute(next));
    // `open` says whether the panel is showing, which the panel can change where it stands.
    else if (name === "open") {
      if (next === null || next === "false") this.#handle.close();
      else this.#handle.open();
    } else this.#mount();
  }

  #mount(): void {
    this.#handle?.destroy();
    this.#handle = undefined;
    if (this.#target === undefined) return;
    this.#mounting = true;
    try {
      this.#handle = createDevtools(this.#root, this.#target, {
        ...optionsFromAttributes(this),
        ...this.#options,
      });
    } finally {
      this.#mounting = false;
    }
  }
}

/** Equal when every option is the same value, comparing one level into nested objects. */
function sameOptions(a: DevtoolsOptions, b: DevtoolsOptions): boolean {
  const left = a as Record<string, unknown>;
  const right = b as Record<string, unknown>;
  const keys = new Set([...Object.keys(left), ...Object.keys(right)]);
  for (const key of keys) {
    const x = left[key];
    const y = right[key];
    if (Object.is(x, y)) continue;
    if (typeof x !== "object" || typeof y !== "object" || x === null || y === null) return false;
    const inner = new Set([...Object.keys(x), ...Object.keys(y)]);
    for (const name of inner) {
      if (!Object.is((x as Record<string, unknown>)[name], (y as Record<string, unknown>)[name])) {
        return false;
      }
    }
  }
  return true;
}

/** Registers the element. Safe to call more than once; a second call is a no-op. */
export function defineMinnowDevtools(name: string = elementName): void {
  if (customElements.get(name) !== undefined) return;
  customElements.define(name, MinnowDevtoolsElement);
}
