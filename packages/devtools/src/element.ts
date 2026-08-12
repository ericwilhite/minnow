import { createDevtools, type DevtoolsHandle } from "./devtools.js";
import { optionsFromAttributes, type DevtoolsOptions } from "./options.js";
import type { DevtoolsAttachable } from "./target.js";

export const elementName = "minnow-devtools";

/**
 * `<minnow-devtools>`. A custom element rather than a framework component: it works unchanged in
 * React, Vue, Svelte, Solid, Astro, and plain HTML, and its shadow root keeps the host page's
 * styles out. Set the database with the `target` property (a `MinnowDatabase`,
 * `MinnowDatabaseClient`, or `Minnow` facade); everything else reads from attributes.
 */
export class MinnowDevtoolsElement extends HTMLElement {
  readonly #root: ShadowRoot;
  #handle: DevtoolsHandle | undefined;
  #target: DevtoolsAttachable | undefined;
  /** Set before the element upgrades or connects; applied when it mounts. */
  #options: DevtoolsOptions = {};

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

  connectedCallback(): void {
    this.#mount();
  }

  disconnectedCallback(): void {
    this.#handle?.destroy();
    this.#handle = undefined;
  }

  #mount(): void {
    this.#handle?.destroy();
    this.#handle = undefined;
    if (this.#target === undefined) return;
    this.#handle = createDevtools(this.#root, this.#target, {
      ...optionsFromAttributes(this),
      ...this.#options,
    });
  }
}

/** Registers the element. Safe to call more than once; a second call is a no-op. */
export function defineMinnowDevtools(name: string = elementName): void {
  if (customElements.get(name) !== undefined) return;
  customElements.define(name, MinnowDevtoolsElement);
}
