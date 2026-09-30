/** One bounded owner of background evidence. Observers receive the original error; history
 * keeps only bounded text and timestamps so error graphs cannot retain database resources. */
function boundedText(read: () => unknown, fallback: string): string {
  try {
    return String(read()).slice(0, 1024);
  } catch {
    return fallback;
  }
}
export class BackgroundDiagnostics {
  readonly #history: Array<{
    sequence: number;
    context: string;
    name: string;
    message: string;
    at: number;
  }> = [];
  #sequence = 0;
  constructor(
    private readonly now: () => number,
    private readonly observer: ((error: unknown, context: string) => void) | undefined,
  ) {}

  report(error: unknown, context: string): void {
    this.#sequence += 1;
    this.#history.push({
      sequence: this.#sequence,
      context: context.slice(0, 1024),
      ...backgroundErrorSummary(error),
      at: this.now(),
    });
    if (this.#history.length > 32) this.#history.shift();
    if (this.observer !== undefined) {
      try {
        this.observer(error, context);
        return;
      } catch (hookError) {
        console.error("[minnowdb] onBackgroundError callback failed:", hookError);
      }
    }
    console.error(`[minnowdb] background failure (${context}):`, error);
  }

  snapshot() {
    return {
      backgroundFailureCount: this.#sequence,
      backgroundErrors: this.#history.map((entry) => ({ ...entry, at: new Date(entry.at) })),
    };
  }
}

export function backgroundErrorSummary(error: unknown) {
  return {
    name: boundedText(() => (error instanceof Error ? error.name : "Error"), "Error"),
    message: boundedText(
      () => (error instanceof Error ? error.message : error),
      "[unprintable error]",
    ),
  };
}
