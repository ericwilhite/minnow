import { StringRegexInput, type RegexInput } from "./input.js";

import {
  MAX_SQL_NESTING_DEPTH,
  SqlPatternWorkLimitError,
  MAX_SQL_PATTERN_MATCH_STEPS,
  MAX_SQL_SCALAR_RESULT_CHARACTERS,
} from "./limits.js";

import {
  captureSpans,
  type RegexNode as Node,
  type RegexInstruction as Instruction,
  type RegexNodeRange,
  type MatchTrace,
} from "./captures.js";

interface NodeDescription {
  readonly captures: boolean;
  readonly width: number | null;
  readonly minimum: number;
  readonly firstGroup: number;
  readonly lastGroup: number;
  readonly alphabet: (value: string, spend: () => void) => boolean;
}

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
    if (--work.remaining < 0) throw new SqlPatternWorkLimitError();
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
              position === 0 || (newline && input.newline(input.previous(position))),
          };
        } else if (current === "$") {
          node = {
            kind: "assert",
            accepts: (input, position) =>
              position === input.length || (newline && input.newline(position)),
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
    this.#ranges = groups ? new Map() : null;
    const emit = (instruction: Instruction): number => {
      if (this.#instructions.length >= 65_536)
        throw new RangeError("Regular expression state limit exceeded");
      this.#instructions.push(instruction);
      return this.#instructions.length - 1;
    };
    const descriptions = new Map<Node, NodeDescription>();
    const describe = (node: Node): NodeDescription => {
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
                  : node.max === 0 || widths[0] === 0
                    ? 0
                    : node.min === node.max && widths[0] !== null && widths[0] !== undefined
                      ? widths[0] * node.min
                      : null;
      // A witnessed prefix cannot consume a following character outside this
      // subtree's alphabet. This proves maximality without replaying its VM.
      const alphabet = (value: string, spend: () => void): boolean => {
        spend();
        return node.kind === "character"
          ? node.matches(value)
          : node.kind === "repeat" && node.max === 0
            ? false
            : values.some((child) => child.alphabet(value, spend));
      };
      const minimum =
        node.kind === "character"
          ? 1
          : node.kind === "assert"
            ? 0
            : node.kind === "group"
              ? (values[0]?.minimum ?? 0)
              : node.kind === "sequence"
                ? values.reduce((total, child) => total + child.minimum, 0)
                : node.kind === "alternative"
                  ? values.reduce((smallest, child) => Math.min(smallest, child.minimum), Infinity)
                  : (values[0]?.minimum ?? 0) * node.min;
      const firstGroup =
        node.kind === "group"
          ? node.group
          : (values.find((value) => value.firstGroup > 0)?.firstGroup ?? 0);
      const lastGroup = values.reduce(
        (last, value) => Math.max(last, value.lastGroup),
        node.kind === "group" ? node.group : 0,
      );
      const value = {
        minimum,
        firstGroup,
        lastGroup,
        alphabet,
        captures:
          node.kind === "group" ||
          ((node.kind !== "repeat" || node.max !== 0) && values.some((value) => value.captures)),
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
    this.#captureRoot = groups && describe(root).captures ? root : null;
  }

  #search(
    input: RegexInput,
    from: number,
    capture: false,
    work?: { remaining: number },
    trace?: MatchTrace,
  ): boolean;
  #search(
    input: RegexInput,
    from: number,
    capture?: true,
    work?: { remaining: number },
    trace?: MatchTrace,
  ): number[] | null;
  #search(
    input: RegexInput,
    from: number,
    capture = true,
    work = { remaining: MAX_SQL_PATTERN_MATCH_STEPS },
    trace?: MatchTrace,
  ): number[] | null | boolean {
    if (input.length > input.limit)
      throw new RangeError("Regular expression input exceeds scalar size limit");
    // Captures do not affect this language. The whole VM owns only pc/position
    // states; submatch priority is resolved by the same dissection afterward.
    const visited = new Map<number, number>();
    for (let start = from; start <= input.length;) {
      if (capture) visited.clear();
      trace?.predecessors.clear();
      const stack: Array<{ pc: number; position: number; previous: number }> = [
        { pc: this.#start, position: start, previous: -1 },
      ];
      let bestEnd = -1;
      while (stack.length) {
        let { pc, position, previous } = required(stack.pop());
        for (;;) {
          this.spend(work);
          const key = position * 65536 + pc;
          if (visited.has(key)) break;
          if (visited.size >= 65536)
            for (const [previous, owner] of visited) if (owner < start) visited.delete(previous);
          if (visited.size >= 65536)
            throw new RangeError("Regular expression match state limit exceeded");
          visited.set(key, start);
          trace?.predecessors.set(key, previous);
          previous = key;
          const instruction = required(this.#instructions[pc]);
          if (instruction.kind === "match") {
            if (!capture) return true;
            if (position > bestEnd) {
              bestEnd = position;
              if (trace) trace.last = key;
            }
            break;
          }
          if (instruction.kind === "split") {
            if (stack.length >= 65536)
              throw new RangeError("Regular expression branch limit exceeded");
            stack.push({ pc: instruction.second, position, previous });
            pc = instruction.first;
          } else if (instruction.kind === "save") pc = instruction.next;
          else if (instruction.kind === "assert") {
            if (!instruction.accepts(input, position)) break;
            pc = instruction.next;
          } else {
            const point = input.codePointAt(position);
            if (point === undefined || !instruction.matches(String.fromCodePoint(point))) break;
            position = input.advance(position, point);
            pc = instruction.next;
          }
        }
      }
      if (bestEnd >= 0) return [start, bestEnd];
      if (start === input.length) break;
      start = input.advance(start, required(input.codePointAt(start)));
    }
    return capture ? null : false;
  }

  /** Numeric captures in the input's native units. The caller keeps its borrow alive. */
  spans(
    input: RegexInput,
    from = 0,
    work = { remaining: MAX_SQL_PATTERN_MATCH_STEPS },
  ): number[] | null {
    const trace: MatchTrace | undefined =
      this.#captureRoot === null ? undefined : { predecessors: new Map(), last: -1 };
    const whole = this.#search(input, from, true, work, trace);
    if (whole === null) return null;
    if (this.#captureRoot === null || this.#ranges === null) {
      if (this.#slots === 2) return whole;
      // Syntactic groups inside {0} still occupy result slots, all unmatched.
      const empty = new Array<number>(this.#slots).fill(-1);
      empty[0] = required(whole[0]);
      empty[1] = required(whole[1]);
      return empty;
    }
    return captureSpans(
      this.#captureRoot,
      this.#ranges,
      this.#instructions,
      input,
      required(whole[0]),
      required(whole[1]),
      this.#slots,
      () => this.spend(work),
      trace,
    );
  }
  exec(input: string): RegExpExecArray | null {
    const spans = this.spans(new StringRegexInput(input, MAX_SQL_SCALAR_RESULT_CHARACTERS));
    if (spans === null) return null;
    const result: Array<string | undefined> = [];
    for (let i = 0; i < spans.length; i += 2) {
      const begin = spans[i] ?? -1,
        end = spans[i + 1] ?? -1;
      result.push(begin < 0 || end < 0 ? undefined : input.slice(begin, end));
    }
    return Object.assign(result, { index: required(spans[0]), input }) as RegExpExecArray;
  }
  test(input: string): boolean {
    return this.#search(new StringRegexInput(input, MAX_SQL_SCALAR_RESULT_CHARACTERS), 0, false);
  }

  [Symbol.replace](input: string, replacement: string): string {
    const work = { remaining: MAX_SQL_PATTERN_MATCH_STEPS },
      source = new StringRegexInput(input, MAX_SQL_SCALAR_RESULT_CHARACTERS);
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
      const match = this.spans(source, from, work);
      if (match === null) break;
      append(input.slice(cursor, required(match[0])));
      append(
        replacement.replace(/\$(\$|&|[1-9])/g, (_, token: string) =>
          token === "$" ? "$" : captureText(input, match, token === "&" ? 0 : Number(token)),
        ),
      );
      cursor = required(match[1]);
      if (!this.#global) break;
      from = cursor;
      if (match[0] === match[1]) {
        if (from === input.length) break;
        from = source.advance(from, required(source.codePointAt(from)));
      }
    }
    append(input.slice(cursor));
    return parts.join("");
  }
}

function captureText(input: string, spans: readonly number[], group: number): string {
  const begin = spans[group * 2] ?? -1,
    end = spans[group * 2 + 1] ?? -1;
  return begin < 0 || end < 0 ? "" : input.slice(begin, end);
}
