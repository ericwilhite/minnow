import {
  MAX_SQL_NESTING_DEPTH,
  MAX_SQL_PATTERN_MATCH_STEPS,
  MAX_SQL_SCALAR_RESULT_CHARACTERS,
} from "./cache-limits.js";

import {
  captureSpans,
  type RegexNode as Node,
  type RegexInstruction as Instruction,
  type RegexNodeRange,
} from "./sql-regex-captures.js";

function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Invalid SQL regular expression program");
  return value;
}

const classes: Readonly<Record<string, string>> = {
  alnum: "A-Za-z0-9",
  alpha: "A-Za-z",
  blank: " \\t",
  cntrl: "\\x00-\\x1f\\x7f",
  digit: "0-9",
  graph: "\\x21-\\x7e",
  lower: "a-z",
  print: "\\x20-\\x7e",
  punct: "!-/:-@\\[-`{-~",
  space: "\\s",
  upper: "A-Z",
  xdigit: "A-Fa-f0-9",
};

/** SQL regex subset with explicit CPU/allocation bounds and leftmost-longest selection.
 * Only single-character predicates use host RegExp; no input reaches host backtracking. */
export class SqlRegex {
  readonly #instructions: Instruction[] = [{ kind: "match" }];
  readonly #start: number;
  readonly #slots: number;
  readonly #global: boolean;
  readonly #captureRoot: Node | null;
  readonly #ranges: Map<Node, RegexNodeRange> | null;
  /** Actual VM states, distinct from cached AST/dissection metadata. */
  get instructionCount(): number {
    return this.#instructions.length;
  }

  get retainedSize(): number {
    return this.#instructions.length * 96 + (this.#ranges?.size ?? 0) * 256;
  }

  /** Every interpreter arrival spends from the fixed per-operation SQL work ceiling. */
  spend(work: { remaining: number }): void {
    if (--work.remaining < 0) throw new RangeError("SQL pattern match exceeds its work limit");
  }

  constructor(pattern: string, flags: string) {
    this.#global = flags.includes("g");
    const insensitive = flags.lastIndexOf("i") > flags.lastIndexOf("c");
    const newline = flags.includes("n");
    let cursor = 0;
    let groups = 0;
    const fail = (reason: string): never => {
      throw new TypeError(`Invalid regular expression: ${reason}`);
    };
    const character = (source: string): Node => {
      let expression: RegExp;
      try {
        expression = new RegExp(`^(?:${source})$`, insensitive ? "iu" : "u");
      } catch {
        return fail("invalid character or class");
      }
      return { kind: "character", matches: (value) => expression.test(value) };
    };
    const sequence = (depth: number): Node => {
      const children: Node[] = [];
      while (cursor < pattern.length && pattern[cursor] !== ")" && pattern[cursor] !== "|") {
        const current = pattern[cursor++];
        let node: Node;
        if (current === "(") {
          if (depth >= MAX_SQL_NESTING_DEPTH)
            throw new RangeError("Regular expression nesting limit exceeded");
          let capturing = true;
          if (pattern[cursor] === "?") {
            if (pattern.slice(cursor, cursor + 2) !== "?:")
              return fail("lookaround and inline flags are unsupported");
            cursor += 2;
            capturing = false;
          }
          const group = capturing ? ++groups : 0;
          if (groups > 32) throw new RangeError("Regular expression capture limit is 32");
          const child = alternative(depth + 1);
          if (pattern[cursor++] !== ")") return fail("unmatched opening parenthesis");
          node = capturing ? { kind: "group", child, group } : child;
        } else if (current === "[") {
          let source = "[";
          if (pattern[cursor] === "^") {
            source += "^";
            cursor += 1;
          }
          if (pattern[cursor] === "]") {
            source += "\\]";
            cursor += 1;
          }
          while (cursor < pattern.length && pattern[cursor] !== "]") {
            if (pattern.slice(cursor, cursor + 2) === "[:") {
              const end = pattern.indexOf(":]", cursor + 2);
              const name = end < 0 ? "" : pattern.slice(cursor + 2, end);
              const expanded = classes[name];
              if (expanded === undefined) return fail("unsupported POSIX character class");
              source += expanded;
              cursor = end + 2;
            } else {
              const member = pattern[cursor++];
              if (member === "\\") {
                const escaped = pattern[cursor++];
                if (escaped === undefined) return fail("unterminated class escape");
                source += `\\${escaped}`;
              } else source += required(member);
            }
          }
          if (pattern[cursor++] !== "]") return fail("unterminated character class");
          if (source === "[" || source === "[^") return fail("empty character class");
          const predicate = character(`${source}]`);
          node =
            newline && source.startsWith("[^") && predicate.kind === "character"
              ? {
                  kind: "character",
                  matches: (value) => value !== "\n" && predicate.matches(value),
                }
              : predicate;
        } else if (current === "^") {
          node = {
            kind: "assert",
            accepts: (input, position) =>
              position === 0 || (newline && input[position - 1] === "\n"),
          };
        } else if (current === "$") {
          node = {
            kind: "assert",
            accepts: (input, position) =>
              position === input.length || (newline && input[position] === "\n"),
          };
        } else if (current === ".") {
          node = { kind: "character", matches: (value) => !newline || value !== "\n" };
        } else if (current === "\\") {
          const escaped = pattern[cursor++];
          if (escaped === undefined) return fail("trailing escape");
          if (/\d/.test(escaped)) return fail("pattern backreferences are unsupported");
          if ("dDsSwW".includes(escaped)) node = character(`\\${escaped}`);
          else if ("nrtfv".includes(escaped)) node = character(`\\${escaped}`);
          else if (/[A-Za-z]/.test(escaped)) return fail(`unsupported escape \\${escaped}`);
          else node = character(`\\${escaped}`);
        } else {
          if (current === "*" || current === "+" || current === "?" || current === "{")
            return fail("quantifier has no expression");
          const point = pattern.codePointAt(cursor - 1);
          const literal = point === undefined ? "" : String.fromCodePoint(point);
          cursor += literal.length - 1;
          node = character(literal.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&"));
        }
        const quantifier = pattern[cursor];
        if (quantifier === "*" || quantifier === "+" || quantifier === "?") {
          cursor += 1;
          node = {
            kind: "repeat",
            child: node,
            min: quantifier === "+" ? 1 : 0,
            max: quantifier === "?" ? 1 : Infinity,
          };
        } else if (quantifier === "{") {
          const match = /^\{(\d+)(?:,(\d*))?\}/.exec(pattern.slice(cursor));
          if (match === null) return fail("invalid repetition bounds");
          const min = Number(match[1]);
          const max = match[2] === undefined ? min : match[2] === "" ? Infinity : Number(match[2]);
          if (min > 1000 || (max !== Infinity && max > 1000))
            throw new RangeError("Regular expression repetition limit is 1000");
          if (min > max) return fail("reversed repetition bounds");
          cursor += match[0].length;
          node = { kind: "repeat", child: node, min, max };
        }
        if (["*", "+", "?", "{"].includes(pattern[cursor] ?? ""))
          return fail("repeated or non-greedy quantifiers are unsupported");
        children.push(node);
      }
      return { kind: "sequence", children };
    };
    const alternative = (depth: number): Node => {
      const children = [sequence(depth)];
      while (pattern[cursor] === "|") {
        cursor += 1;
        children.push(sequence(depth));
      }
      return children.length === 1 ? required(children[0]) : { kind: "alternative", children };
    };
    const root = alternative(0);
    if (cursor !== pattern.length) fail("unmatched closing parenthesis");
    this.#slots = 2 * (groups + 1);
    this.#captureRoot = groups ? root : null;
    this.#ranges = groups ? new Map() : null;
    const emit = (instruction: Instruction): number => {
      if (this.#instructions.length >= 65_536)
        throw new RangeError("Regular expression state limit exceeded");
      this.#instructions.push(instruction);
      return this.#instructions.length - 1;
    };
    const descriptions = new Map<Node, { captures: boolean; width: number | null }>();
    const describe = (node: Node): { captures: boolean; width: number | null } => {
      const old = descriptions.get(node);
      if (old) return old;
      const descendants =
        node.kind === "group" || node.kind === "repeat"
          ? [node.child]
          : node.kind === "sequence" || node.kind === "alternative"
            ? node.children
            : [];
      const values = descendants.map(describe),
        widths = values.map((value) => value.width);
      const width =
        node.kind === "character"
          ? 1
          : node.kind === "assert"
            ? 0
            : node.kind === "group"
              ? (widths[0] ?? null)
              : node.kind === "sequence"
                ? widths.every((w) => w !== null)
                  ? widths.reduce<number>((n, w) => n + w, 0)
                  : null
                : node.kind === "alternative"
                  ? widths.every((w) => w === widths[0])
                    ? (widths[0] ?? null)
                    : null
                  : widths[0] === 0
                    ? 0
                    : node.min === node.max && widths[0] !== null && widths[0] !== undefined
                      ? widths[0] * node.min
                      : null;
      const value = {
        captures: node.kind === "group" || values.some((value) => value.captures),
        width: width !== null && Number.isSafeInteger(width) ? width : null,
      };
      descriptions.set(node, value);
      return value;
    };
    const compile = (node: Node, next: number): number => {
      const children: Array<{ start: number; stop: number }> = [];
      let prefix: { start: number; stop: number } | null = null;
      const build = (): number => {
        if (node.kind === "character" || node.kind === "assert") return emit({ ...node, next });
        if (node.kind === "group")
          return emit({
            kind: "save",
            slot: node.group * 2,
            next: compile(node.child, emit({ kind: "save", slot: node.group * 2 + 1, next })),
          });
        if (node.kind === "sequence") {
          let start = next;
          for (let i = node.children.length - 1; i >= 0; i--) {
            const stop = start;
            start = compile(required(node.children[i]), stop);
            if (this.#ranges) children[i] = { start, stop };
          }
          return start;
        }
        if (node.kind === "alternative")
          return node.children.reduceRight((second, child, index) => {
            const first = compile(child, next);
            return index === node.children.length - 1
              ? first
              : emit({ kind: "split", first, second });
          }, next);
        let start = next;
        if (node.max === Infinity) {
          const loop = { kind: "split" as const, first: -1, second: next };
          start = emit(loop);
          loop.first = compile(node.child, start);
        } else {
          for (let index = node.min; index < node.max; index += 1)
            start = emit({ kind: "split", first: compile(node.child, start), second: start });
        }
        for (let index = 0; index < node.min; index += 1) {
          if (this.#ranges) prefix = { start, stop: next };
          start = compile(node.child, start);
        }
        return start;
      };
      const start = build();
      if (this.#ranges)
        this.#ranges.set(node, { start, stop: next, children, prefix, ...describe(node) });
      return start;
    };
    this.#start = compile(root, 0);
  }

  #search(input: string, from: number, capture: false, work?: { remaining: number }): boolean;
  #search(
    input: string,
    from: number,
    capture?: true,
    work?: { remaining: number },
  ): RegExpExecArray | null;
  #search(
    input: string,
    from: number,
    capture = true,
    work = { remaining: MAX_SQL_PATTERN_MATCH_STEPS },
  ): RegExpExecArray | null | boolean {
    if (input.length > MAX_SQL_SCALAR_RESULT_CHARACTERS)
      throw new RangeError("Regular expression input exceeds scalar size limit");
    // Captures cannot affect the supported language (backreferences/lookaround are refused).
    // Completed-start failures can therefore be shared across starts in boolean mode. Keep
    // current cycle guards when old memo entries are evicted at the existing state ceiling.
    const visited = new Map<string | number, number>();
    for (let start = from; start <= input.length;) {
      if (capture) visited.clear();
      const stack: Array<{ pc: number; position: number; captures: number[] | null }> = [
        {
          pc: this.#start,
          position: start,
          captures: capture ? new Array<number>(2) : null,
        },
      ];
      let best: number[] | undefined;
      // Select only the whole span here. Subcaptures cannot affect this language and
      // are dissected afterwards; repeated empty paths must still terminate.
      while (stack.length > 0) {
        const state = required(stack.pop());
        let { pc, position } = state;
        const captures = state.captures;
        for (;;) {
          this.spend(work); // Charge duplicate arrivals too, before deduplication.
          // PCs fit 16 bits; bounded UTF-16 positions keep the packed numeric key exact.
          const key =
            captures === null
              ? position * 65_536 + pc
              : `${String(pc)}:${String(position)}:${captures.join(",")}`;
          if (visited.has(key)) break;
          if (visited.size >= 65_536)
            for (const [previous, owner] of visited) if (owner < start) visited.delete(previous);
          if (visited.size >= 65_536)
            throw new RangeError("Regular expression match state limit exceeded");
          visited.set(key, start);
          const instruction = required(this.#instructions[pc]);
          if (instruction.kind === "match") {
            if (captures === null) return true;
            captures[0] = start;
            captures[1] = position;
            if (best === undefined || preferredCaptures(captures, best)) best = captures;
            break;
          }
          if (instruction.kind === "split") {
            if (stack.length >= 65_536)
              throw new RangeError("Regular expression branch limit exceeded");
            stack.push({
              pc: instruction.second,
              position,
              captures: captures === null ? null : [...captures],
            });
            pc = instruction.first;
          } else if (instruction.kind === "save") {
            if (captures !== null && instruction.slot < 2) captures[instruction.slot] = position;
            pc = instruction.next;
          } else if (instruction.kind === "assert") {
            if (!instruction.accepts(input, position)) break;
            pc = instruction.next;
          } else {
            const point = input.codePointAt(position);
            if (point === undefined) break;
            const character = String.fromCodePoint(point);
            if (!instruction.matches(character)) break;
            position += character.length;
            pc = instruction.next;
          }
        }
      }
      if (best !== undefined) {
        const result: Array<string | undefined> = [];
        for (let group = 0; group < best.length; group += 2) {
          const begin = best[group] ?? -1;
          const end = best[group + 1] ?? -1;
          result.push(begin < 0 || end < 0 ? undefined : input.slice(begin, end));
        }
        // Native RegExp also returns undefined for an unmatched optional capture; lib.d.ts
        // describes this array as string[] even though these entries can be absent.
        return Object.assign(result, { index: start, input }) as RegExpExecArray;
      }
      if (start === input.length) break;
      start += String.fromCodePoint(required(input.codePointAt(start))).length;
    }
    return capture ? null : false;
  }

  #match(input: string, from: number, work: { remaining: number }): RegExpExecArray | null {
    const whole = this.#search(input, from, true, work);
    if (whole === null || this.#captureRoot === null || this.#ranges === null) return whole;
    const spans = captureSpans(
      this.#captureRoot,
      this.#ranges,
      this.#instructions,
      input,
      whole.index,
      whole.index + whole[0].length,
      this.#slots,
      () => this.spend(work),
    );
    const values: Array<string | undefined> = [whole[0]];
    for (let i = 2; i < spans.length; i += 2) {
      const begin = spans[i] ?? -1,
        end = spans[i + 1] ?? -1;
      values.push(begin < 0 || end < 0 ? undefined : input.slice(begin, end));
    }
    return Object.assign(values, { index: whole.index, input }) as RegExpExecArray;
  }
  exec(input: string): RegExpExecArray | null {
    return this.#match(input, 0, { remaining: MAX_SQL_PATTERN_MATCH_STEPS });
  }
  test(input: string): boolean {
    return this.#search(input, 0, false);
  }

  [Symbol.replace](input: string, replacement: string): string {
    const work = { remaining: MAX_SQL_PATTERN_MATCH_STEPS };
    const parts: string[] = [];
    let cursor = 0;
    let from = 0;
    let length = 0;
    const append = (part: string): void => {
      length += part.length;
      if (length > MAX_SQL_SCALAR_RESULT_CHARACTERS)
        throw new RangeError("REGEXP_REPLACE result exceeds scalar size limit");
      parts.push(part);
    };
    for (;;) {
      const match = this.#match(input, from, work);
      if (match === null) break;
      append(input.slice(cursor, match.index));
      append(
        replacement.replace(/\$(\$|&|[1-9])/g, (_, token: string) =>
          token === "$" ? "$" : token === "&" ? match[0] : (match[Number(token)] ?? ""),
        ),
      );
      cursor = match.index + match[0].length;
      if (!this.#global) break;
      from = cursor;
      if (match[0].length === 0) {
        if (from === input.length) break;
        from += String.fromCodePoint(required(input.codePointAt(from))).length;
      }
    }
    append(input.slice(cursor));
    return parts.join("");
  }
}

function preferredCaptures(left: readonly number[], right: readonly number[]): boolean {
  for (let index = 0; index < left.length; index += 1) {
    const value = left[index] ?? -1;
    const other = right[index] ?? -1;
    if (value !== other)
      return index % 2 === 0 ? value >= 0 && (other < 0 || value < other) : value > other;
  }
  return false;
}
