/**
 * Turning a compiled snippet into something that runs, and collecting what it prints.
 *
 * The editor compiles to an ES module, and an ES module cannot be handed to `new Function`:
 * `import` is a declaration the parser only accepts at the top of a real module. Loading the
 * result as a module instead — a blob URL with an import map — would need the map installed
 * before the page loaded, which is not something a lazily-mounted console gets to arrange.
 *
 * So the imports are rewritten into lookups against a registry of modules the page has already
 * loaded. That keeps the snippet's line numbers intact, which matters: a runtime error reports
 * the line the reader is looking at, not a line in a wrapper they cannot see.
 */

/** The forms TypeScript emits for an `import`, in the order they have to be matched. */
const IMPORT = /^[ \t]*import(?:\s+([\s\S]*?)\s+from)?\s*(['"])([^'"]+)\2\s*;?[ \t]*$/gm;

/**
 * Rewrites a compiled module into the body of an async function.
 *
 * `__require` resolves a specifier against the page's own modules, and `await` works at the top
 * level because the body becomes an async function — the one thing this transformation gains
 * over compiling to CommonJS, and the reason a snippet reads like the code in the docs.
 */
export function toRunnableBody(compiled: string): string {
  return compiled
    .replace(IMPORT, (whole, clause: string | undefined, _quote, specifier: string) => {
      if (clause === undefined) return `__require(${JSON.stringify(specifier)});`;
      return `const ${binding(clause)} = __require(${JSON.stringify(specifier)});`;
    })
    .replace(/^([ \t]*)export\s+(?=(?:async\s+)?(?:const|let|var|function|class)\b)/gm, "$1")
    .replace(/^[ \t]*export\s*\{[^}]*\}\s*;?[ \t]*$/gm, "");
}

/**
 * The left-hand side of the `const` an import clause becomes. A namespace import binds a name
 * directly; everything else destructures, with the default export read from `.default`.
 */
function binding(clause: string): string {
  const trimmed = clause.trim();

  if (trimmed.startsWith("*")) {
    const namespace = /^\*\s+as\s+(\w+)$/.exec(trimmed);
    if (namespace === null) throw new Error(`The console cannot run \`import ${trimmed} from …\``);
    return namespace[1] ?? "";
  }

  const named = (pattern: string): string => pattern.replace(/(\w+)\s+as\s+(\w+)/g, "$1: $2");
  if (trimmed.startsWith("{")) return named(trimmed);

  const comma = trimmed.indexOf(",");
  if (comma === -1) return `{ default: ${trimmed} }`;

  // `import def, { a } from …`. The other pairing — a default beside a namespace — would need
  // two declarations, which one `const` cannot express.
  const rest = trimmed.slice(comma + 1).trim();
  if (!rest.startsWith("{")) throw new Error(`The console cannot run \`import ${trimmed} from …\``);
  return `{ default: ${trimmed.slice(0, comma).trim()}, ${named(rest).replace(/^\{\s*/, "")}`;
}

/** One line the snippet printed, kept as values so a row array can be drawn as a table. */
export interface OutputEntry {
  level: "log" | "warn" | "error";
  values: unknown[];
}

export interface RunResult {
  output: OutputEntry[];
  /** Whatever the snippet returned, if it returned anything. */
  returned?: unknown;
  /** Wall-clock milliseconds the snippet spent running. */
  elapsedMs: number;
  failure?: string;
}

export interface RunScope {
  modules: Record<string, unknown>;
  /** Names bound in the snippet's scope: `db`, `database`, `sql`. */
  globals: Record<string, unknown>;
}

/** `AsyncFunction` is not a global; its constructor is only reachable through an instance. */
type AsyncFunctionConstructor = new (...args: string[]) => (...args: unknown[]) => Promise<unknown>;

const AsyncFunction = (
  Object.getPrototypeOf(async () => {
    /* an async function, borrowed for its prototype */
  }) as { constructor: AsyncFunctionConstructor }
).constructor;

/**
 * Runs a compiled snippet and returns what it printed. A snippet that throws is a result, not an
 * exception: the console shows the message the way a terminal would.
 */
export async function runSnippet(compiled: string, scope: RunScope): Promise<RunResult> {
  const output: OutputEntry[] = [];
  const record =
    (level: OutputEntry["level"]) =>
    (...values: unknown[]): void => {
      output.push({ level, values });
    };
  const printer = {
    log: record("log"),
    info: record("log"),
    debug: record("log"),
    warn: record("warn"),
    error: record("error"),
    table: record("log"),
  };

  const require = (specifier: string): unknown => {
    const found = scope.modules[specifier];
    if (found === undefined) {
      throw new Error(
        `The console has no "${specifier}". It can import ${Object.keys(scope.modules)
          .map((name) => `"${name}"`)
          .join(" and ")}.`,
      );
    }
    return found;
  };

  const names = [...Object.keys(scope.globals), "console", "__require"];
  const values = [...Object.values(scope.globals), printer, require];
  const started = performance.now();
  try {
    const returned = await new AsyncFunction(...names, toRunnableBody(compiled))(...values);
    return {
      output,
      elapsedMs: performance.now() - started,
      ...(returned === undefined ? {} : { returned }),
    };
  } catch (error) {
    return {
      output,
      elapsedMs: performance.now() - started,
      failure: error instanceof Error ? error.message : String(error),
    };
  }
}
