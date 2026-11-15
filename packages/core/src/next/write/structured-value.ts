/** Streaming structured-value grammar for portable snapshots. One forward pass,
 * bounded workspace, no decoded graph. Parts stay borrowed until the consume callback settles. */
import { DURABLE_FORMATS } from "../durable-format.js";
import type { PhysicalType } from "../types.js";
import { arrayElementTag, MAX_ARRAY_ELEMENTS, numericFits } from "../vector/array-value.js";

export class StructuredValueCorruptionError extends Error {
  readonly code = "MINNOW_VALUE_CORRUPT";
  constructor(message: string) {
    super(message);
    this.name = "StructuredValueCorruptionError";
  }
}
export interface ValidationAccount {
  take(bytes: number): void;
  free(bytes: number): void;
  pace(): Promise<void>;
}
function check(condition: boolean, message: string): void {
  if (!condition) throw new StructuredValueCorruptionError(message);
}
class Cursor {
  readonly scalar = new Uint8Array(8);
  readonly view = new DataView(this.scalar.buffer);
  at = 0;
  #part: Uint8Array | null = null;
  #offset = 0;
  #done = false;
  constructor(
    readonly input: AsyncIterator<Uint8Array>,
    readonly end: number,
    readonly account: ValidationAccount,
    readonly consume: (part: Uint8Array) => void | Promise<void>,
  ) {}
  need(length: number): void {
    check(
      Number.isSafeInteger(length) && length >= 0 && this.at + length <= this.end,
      "Truncated structured portable value",
    );
  }
  async #consumed(): Promise<void> {
    const part = this.#part;
    if (part === null) return;
    check(this.#offset === part.length, "Unconsumed structured value bytes");
    await this.consume(part);
    this.#part = null;
    this.#offset = 0;
  }
  async chunk(length: number): Promise<Uint8Array> {
    this.need(length);
    check(length > 0, "Empty structured read");
    for (;;) {
      if (this.#part !== null && this.#offset < this.#part.length) {
        const take = Math.min(length, this.#part.length - this.#offset, 8192),
          at = this.#offset;
        this.#offset += take;
        this.at += take;
        await this.account.pace();
        return this.#part.subarray(at, at + take);
      }
      await this.#consumed();
      const next = await this.input.next();
      if (next.done === true) {
        this.#done = true;
        throw new StructuredValueCorruptionError("Truncated structured value producer");
      }
      const part = next.value;
      check(
        part instanceof Uint8Array && this.at + part.length <= this.end,
        "Structured value producer length differs",
      );
      this.#part = part;
      if (part.length === 0) await this.account.pace();
    }
  }
  async number(width: number, signed = false): Promise<number> {
    this.need(width);
    let copied = 0;
    while (copied < width) {
      const part = await this.chunk(width - copied);
      this.scalar.set(part, copied);
      copied += part.length;
    }
    return width === 1
      ? (this.scalar[0] ?? 0)
      : width === 2
        ? this.view.getUint16(0, true)
        : signed
          ? this.view.getInt32(0, true)
          : this.view.getUint32(0, true);
  }
  async i64(): Promise<bigint> {
    await this.number(8);
    return this.view.getBigInt64(0, true);
  }
  async skip(length: number): Promise<void> {
    this.need(length);
    while (length > 0) length -= (await this.chunk(length)).length;
  }
  async finish(): Promise<void> {
    check(this.at === this.end, "Trailing structured value bytes");
    await this.#consumed();
    for (;;) {
      const next = await this.input.next();
      if (next.done === true) {
        this.#done = true;
        return;
      }
      check(
        next.value instanceof Uint8Array && next.value.length === 0,
        "Structured value producer has trailing bytes",
      );
      await this.account.pace();
      await this.consume(next.value);
    }
  }
  async close(): Promise<void> {
    this.#part = null;
    if (!this.#done) await this.input.return?.();
  }
}
async function text(
  cursor: Cursor,
  length: number,
  type: PhysicalType,
  account: ValidationAccount,
): Promise<void> {
  cursor.need(length);
  const end = cursor.at + length,
    decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  const labels = type.kind === "enum" ? type.labels : null;
  const charge = labels === null ? 0 : labels.length + 128;
  account.take(charge);
  try {
    const candidates = labels === null ? null : new Uint8Array(labels.length).fill(1);
    let position = 0,
      characters = 0;
    const consume = (decoded: string) => {
      if (labels !== null && candidates !== null)
        for (let i = 0; i < labels.length; i++)
          if (candidates[i] === 1 && !labels[i]?.startsWith(decoded, position)) candidates[i] = 0;
      position += decoded.length;
      if (type.kind === "text" && type.maxLength !== undefined)
        for (const _ of decoded) {
          void _;
          characters++;
        }
    };
    const decode = (part?: Uint8Array) => {
      try {
        return part === undefined ? decoder.decode() : decoder.decode(part, { stream: true });
      } catch {
        throw new StructuredValueCorruptionError("Invalid structured UTF-8");
      }
    };
    while (cursor.at < end) consume(decode(await cursor.chunk(end - cursor.at)));
    consume(decode());
    if (labels !== null && candidates !== null)
      check(
        labels.some((label, i) => candidates[i] === 1 && label.length === position),
        "Unknown portable enum label",
      );
    if (type.kind === "text" && type.maxLength !== undefined)
      check(characters <= type.maxLength, "Portable text exceeds declared length");
  } finally {
    account.free(charge);
  }
}
async function decimal(cursor: Cursor, account: ValidationAccount): Promise<void> {
  const special = await cursor.number(1);
  check(special <= 3, "Invalid array NUMERIC special");
  if (special !== 0) return;
  const scale = await cursor.number(2),
    length = await cursor.number(4);
  check(scale <= 16383, "Invalid array NUMERIC scale");
  cursor.need(length);
  // A valid coefficient is <=491,521 bits. Keep a fixed 64 KiB signed prefix and
  // verify any redundant sign extension in streaming windows before BigInt conversion.
  const charge = 4 << 20;
  account.take(charge);
  try {
    const count = Math.min(length, 65536),
      bytes = new Uint8Array(count);
    let at = 0;
    while (at < count) {
      const part = await cursor.chunk(count - at);
      bytes.set(part, at);
      at += part.length;
    }
    const sign = ((bytes[count - 1] ?? 0) & 128) === 0 ? 0 : 255;
    let remaining = length - count;
    while (remaining > 0) {
      const part = await cursor.chunk(remaining);
      for (const byte of part) check(byte === sign, "Oversized array NUMERIC coefficient");
      remaining -= part.length;
    }
    const parts: string[] = [];
    for (let end = count; end > 0; end -= 1024) {
      await account.pace();
      let hex = "";
      for (let i = end - 1; i >= Math.max(0, end - 1024); i--)
        hex += (bytes[i] ?? 0).toString(16).padStart(2, "0");
      parts.push(hex);
    }
    const value = count === 0 ? 0n : BigInt.asIntN(count * 8, BigInt(`0x${parts.join("")}`));
    check(numericFits(value, scale), "Array NUMERIC exceeds supported precision");
  } finally {
    account.free(charge);
  }
}
async function array(
  cursor: Cursor,
  element: PhysicalType,
  account: ValidationAccount,
): Promise<void> {
  check(
    element.kind !== "json" && element.kind !== "array" && element.kind !== "locator",
    "Unsupported portable array element type",
  );
  const version = await cursor.number(1),
    tag = await cursor.number(1),
    dimensions = await cursor.number(1),
    flags = await cursor.number(1);
  check(
    version === DURABLE_FORMATS.arrayValue.version &&
      tag === arrayElementTag(element) &&
      dimensions <= 6 &&
      (flags & ~1) === 0,
    "Invalid portable array header",
  );
  let count = dimensions === 0 ? 0 : 1;
  for (let d = 0; d < dimensions; d++) {
    const length = await cursor.number(4, true),
      lower = await cursor.number(4, true);
    check(length > 0 && lower + length - 1 <= 2147483647, "Invalid portable array dimension");
    count *= length;
    check(
      count <= MAX_ARRAY_ELEMENTS && count <= cursor.end * 8,
      "Portable array dimensions exceed value",
    );
  }
  let nonnull = count;
  if ((flags & 1) !== 0) {
    let remaining = Math.ceil(count / 8);
    while (remaining > 0) {
      const part = await cursor.chunk(remaining);
      if (part.length === remaining && (count & 7) !== 0)
        check((part[part.length - 1] ?? 0) >>> (count & 7) === 0, "Nonzero array bitmap tail bits");
      for (let bits of part) {
        bits -= (bits >>> 1) & 0x55;
        bits = (bits & 0x33) + ((bits >>> 2) & 0x33);
        nonnull -= (bits + (bits >>> 4)) & 15;
      }
      remaining -= part.length;
    }
  }
  const fixed =
    element.kind === "date" ||
    element.kind === "float32" ||
    (element.kind === "int32" && element.bits === 32)
      ? 4
      : element.kind === "int64" || element.kind === "timestamp" || element.kind === "float64"
        ? 8
        : element.kind === "uuid" || element.kind === "interval"
          ? 16
          : 0;
  if (fixed !== 0) await cursor.skip(nonnull * fixed);
  else if (element.kind === "bool") {
    while (nonnull > 0) {
      const part = await cursor.chunk(nonnull);
      for (const value of part) check(value <= 1, "Invalid array boolean");
      nonnull -= part.length;
    }
  } else
    for (let i = 0; i < nonnull; i++) {
      if ((i & 1023) === 0) await account.pace();
      switch (element.kind) {
        case "int32": {
          const value = await cursor.number(4, true);
          check(value >= -32768 && value <= 32767, "Array SMALLINT out of range");
          break;
        }
        case "time": {
          const value = await cursor.i64();
          check(value >= 0n && value <= 86400000000n, "Array TIME out of range");
          break;
        }
        case "decimal":
          await decimal(cursor, account);
          break;
        case "bytes":
          await cursor.skip(await cursor.number(4));
          break;
        case "text":
        case "enum":
          await text(cursor, await cursor.number(4), element, account);
          break;
        default:
          check(false, "Unsupported portable array element type");
      }
    }
  check(cursor.at === cursor.end, "Trailing portable array bytes");
}

/** Streaming JSON syntax recognizer: no parsed object/string/number value is retained. */
async function json(cursor: Cursor, account: ValidationAccount, binary: boolean): Promise<void> {
  account.take(256);
  let stack = new Uint8Array(256),
    depth = 0;
  const parser: { root: boolean; token: "none" | "string" | "number" | "literal" } = {
    root: false,
    token: "none",
  };
  const push = (state: number) => {
    if (depth === stack.length) {
      account.take(stack.length * 2);
      const grown = new Uint8Array(stack.length * 2);
      grown.set(stack);
      account.free(stack.length);
      stack = grown;
    }
    stack[depth++] = state;
  };
  const state = () => (depth === 0 ? (parser.root ? 8 : 9) : (stack[depth - 1] ?? 8));
  const value = () => {
    const s = state();
    check(s === 9 || s === 3 || s === 5 || s === 6, "Unexpected JSON value");
    if (depth === 0) parser.root = true;
    else stack[depth - 1] = s === 3 ? 4 : 7;
  };
  let key = false,
    escaped = false,
    unicode = 0,
    unicodeValue = 0,
    pendingHigh = false,
    fractionDigits = 0,
    significantDigits = 0,
    nonzero = false,
    exponent = 0,
    exponentNegative = false,
    numberState = 0,
    literal = "",
    literalAt = 0;
  // Native JSONB semantics (exec/semantic.ts parseJsonb; PG NUMERIC/Unicode limits).
  // Keep only counters and one pending UTF-16 unit, never token text or parsed values.
  const unit = (code: number) => {
    if (!binary) return;
    if (pendingHigh) {
      check(code >= 0xdc00 && code <= 0xdfff, "Unpaired JSONB surrogate");
      pendingHigh = false;
    } else {
      check(code !== 0 && (code < 0xdc00 || code > 0xdfff), "Invalid JSONB Unicode unit");
      pendingHigh = code >= 0xd800 && code <= 0xdbff;
    }
  };
  const mantissa = (c: string, fraction: boolean) => {
    if (fraction) fractionDigits++;
    if (c !== "0") nonzero = true;
    if (nonzero) significantDigits++;
  };
  const exponentDigit = (c: string) => {
    exponent = Math.min(1073741824, exponent * 10 + Number(c));
  };
  const finishNumber = () => {
    if (!binary) return;
    check(exponent <= 1073741823, "JSONB numeric exponent out of range");
    const scale = fractionDigits - (exponentNegative ? -exponent : exponent);
    check(
      scale <= 16383 && (significantDigits === 0 || significantDigits - scale <= 131072),
      "JSONB numeric value out of range",
    );
  };
  const digit = (c: string) => c >= "0" && c <= "9";
  const feed = (character: string) => {
    const c = character;
    for (;;) {
      if (parser.token === "string") {
        if (unicode > 0) {
          check(/^[0-9a-fA-F]$/.test(c), "Invalid JSON unicode escape");
          unicodeValue = unicodeValue * 16 + Number.parseInt(c, 16);
          if (--unicode === 0) unit(unicodeValue);
          return;
        }
        if (escaped) {
          check('"\\/bfnrtu'.includes(c), "Invalid JSON escape");
          if (c === "u") {
            unicode = 4;
            unicodeValue = 0;
          } else check(!binary || !pendingHigh, "Unpaired JSONB surrogate");
          escaped = false;
          return;
        }
        if (c === "\\") {
          escaped = true;
          return;
        }
        if (c === '"') {
          check(!binary || !pendingHigh, "Unpaired JSONB surrogate");
          parser.token = "none";
          if (key) stack[depth - 1] = 2;
          return;
        }
        check(c.charCodeAt(0) >= 32, "Unescaped JSON control character");
        unit(c.codePointAt(0) ?? 0);
        return;
      }
      if (parser.token === "literal") {
        check(c === literal[literalAt++], "Invalid JSON literal");
        if (literalAt === literal.length) parser.token = "none";
        return;
      }
      if (parser.token === "number") {
        if (numberState === 0) {
          check(digit(c), "Truncated JSON number");
          mantissa(c, false);
          numberState = c === "0" ? 1 : 2;
          return;
        }
        if (numberState === 3) {
          check(digit(c), "Truncated JSON fraction");
          mantissa(c, true);
          numberState = 4;
          return;
        }
        if (numberState === 5) {
          if (c === "+" || c === "-") {
            exponentNegative = c === "-";
            numberState = 6;
            return;
          }
          check(digit(c), "Truncated JSON exponent");
          exponentDigit(c);
          numberState = 7;
          return;
        }
        if (numberState === 6) {
          check(digit(c), "Truncated JSON exponent");
          exponentDigit(c);
          numberState = 7;
          return;
        }
        if (digit(c)) {
          check(numberState !== 1, "JSON leading zero");
          if (numberState === 7) exponentDigit(c);
          else mantissa(c, numberState === 4);
          return;
        }
        if (c === "." && (numberState === 1 || numberState === 2)) {
          numberState = 3;
          return;
        }
        if (
          (c === "e" || c === "E") &&
          (numberState === 1 || numberState === 2 || numberState === 4)
        ) {
          numberState = 5;
          return;
        }
        finishNumber();
        parser.token = "none"; // Validate the delimiter through the container grammar below.
      }
      if (c === " " || c === "\t" || c === "\n" || c === "\r") return;
      const s = state();
      if (c === '"') {
        key = s === 0 || s === 1;
        if (!key) value();
        pendingHigh = false;
        parser.token = "string";
        return;
      }
      if (c === "}") {
        check(s === 0 || s === 4, "Unexpected JSON object end");
        depth--;
        return;
      }
      if (c === "]") {
        check(s === 5 || s === 7, "Unexpected JSON array end");
        depth--;
        return;
      }
      if (c === ":") {
        check(s === 2, "Unexpected JSON colon");
        stack[depth - 1] = 3;
        return;
      }
      if (c === ",") {
        check(s === 4 || s === 7, "Unexpected JSON comma");
        stack[depth - 1] = s === 4 ? 1 : 6;
        return;
      }
      value();
      if (c === "{") {
        push(0);
        return;
      }
      if (c === "[") {
        push(5);
        return;
      }
      if (c === "t" || c === "f" || c === "n") {
        literal = c === "t" ? "true" : c === "f" ? "false" : "null";
        literalAt = 1;
        parser.token = "literal";
        return;
      }
      check(c === "-" || digit(c), "Invalid JSON token");
      parser.token = "number";
      fractionDigits = 0;
      significantDigits = 0;
      nonzero = false;
      exponent = 0;
      exponentNegative = false;
      if (c !== "-") mantissa(c, false);
      numberState = c === "-" ? 0 : c === "0" ? 1 : 2;
      return;
    }
  };
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  const decode = (part?: Uint8Array) => {
    try {
      return part === undefined ? decoder.decode() : decoder.decode(part, { stream: true });
    } catch {
      throw new StructuredValueCorruptionError("Invalid JSON UTF-8");
    }
  };
  try {
    while (cursor.at < cursor.end) {
      const chunk = await cursor.chunk(cursor.end - cursor.at),
        decoded = decode(chunk);
      for (const c of decoded) feed(c);
    }
    for (const c of decode()) feed(c);
    check(
      parser.token === "none" || (parser.token === "number" && [1, 2, 4, 7].includes(numberState)),
      "Truncated JSON token",
    );
    if (parser.token === "number") finishNumber();
    check(parser.root && depth === 0, "Incomplete portable JSON");
  } finally {
    account.free(stack.length);
  }
}

/** Funding precedes cursor/parser allocation. The caller owns the account and its retry policy;
 * typed quota, IO and cancellation errors pass through without becoming corruption. */
export async function validateStructuredParts(
  type: PhysicalType,
  parts: () => AsyncIterable<Uint8Array>,
  length: number,
  account: ValidationAccount,
  consume: (part: Uint8Array) => void | Promise<void>,
): Promise<void> {
  account.take(131072);
  let cursor: Cursor | undefined;
  try {
    check(Number.isSafeInteger(length) && length >= 0, "Invalid structured value length");
    cursor = new Cursor(parts()[Symbol.asyncIterator](), length, account, consume);
    await account.pace();
    if (type.kind === "array") await array(cursor, type.element, account);
    else if (type.kind === "json") await json(cursor, account, type.binary);
    else if (type.kind === "text" || type.kind === "enum")
      await text(cursor, length, type, account);
    else {
      check(type.kind === "bytes", "Unsupported structured portable type");
      await cursor.skip(length);
    }
    await cursor.finish();
  } finally {
    try {
      await cursor?.close();
    } finally {
      account.free(131072);
    }
  }
}
