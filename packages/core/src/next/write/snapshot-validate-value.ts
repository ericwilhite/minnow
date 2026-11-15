/** Semantic validation of untrusted structured values before native publication.
 * No element/value graph: bounded read windows, at most a null-bitmap window, and charged
 * JSON nesting state. NUMERIC workspace is bounded by PostgreSQL's coefficient limit. */
import { SnapshotFormatError } from "../format/snapshot.js";
import { DURABLE_FORMATS } from "../durable-format.js";
import type { PhysicalType } from "../types.js";
import { arrayElementTag, MAX_ARRAY_ELEMENTS, numericFits } from "../vector/array-value.js";
import {
  SnapshotAccount,
  SnapshotColumnBytes,
  snapshotCheck,
  snapshotPace,
  type SnapshotStagedColumn,
} from "./snapshot-stage.js";
import type { ExtentReader } from "../storage/extents.js";

class Cursor {
  readonly bytes = new Uint8Array(8192);
  readonly scalar = new Uint8Array(8);
  readonly view = new DataView(this.scalar.buffer);
  at: number;
  #start = -1;
  #length = 0;
  constructor(
    readonly source: SnapshotColumnBytes,
    readonly begin: number,
    readonly end: number,
  ) {
    this.at = begin;
  }
  need(length: number): void {
    snapshotCheck(
      Number.isSafeInteger(length) && length >= 0 && this.at + length <= this.end,
      "Truncated structured portable value",
    );
  }
  async chunk(length: number): Promise<Uint8Array> {
    this.need(length);
    if (this.at < this.#start || this.at >= this.#start + this.#length) {
      this.#start = this.at;
      this.#length = Math.min(this.bytes.length, this.end - this.at);
      await this.source.fill(this.at, this.bytes.subarray(0, this.#length));
    }
    const take = Math.min(length, this.#start + this.#length - this.at),
      start = this.at - this.#start;
    this.at += take;
    return this.bytes.subarray(start, start + take);
  }
  async number(width: number, signed = false): Promise<number> {
    this.need(width);
    let copied = 0;
    while (copied < width) {
      const part = await this.chunk(width - copied);
      this.scalar.set(part, copied);
      copied += part.length;
    }
    if (width === 1) return this.scalar[0] ?? 0;
    if (width === 2) return this.view.getUint16(0, true);
    return signed ? this.view.getInt32(0, true) : this.view.getUint32(0, true);
  }
  async i64(): Promise<bigint> {
    await this.number(8);
    return this.view.getBigInt64(0, true);
  }
  skip(length: number): void {
    this.need(length);
    this.at += length;
  }
}
async function text(
  cursor: Cursor,
  length: number,
  type: PhysicalType,
  account: SnapshotAccount,
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
        throw new SnapshotFormatError("Invalid structured UTF-8");
      }
    };
    while (cursor.at < end) consume(decode(await cursor.chunk(end - cursor.at)));
    consume(decode());
    if (labels !== null && candidates !== null)
      snapshotCheck(
        labels.some((label, i) => candidates[i] === 1 && label.length === position),
        "Unknown portable enum label",
      );
    if (type.kind === "text" && type.maxLength !== undefined)
      snapshotCheck(characters <= type.maxLength, "Portable text exceeds declared length");
  } finally {
    account.free(charge);
  }
}
async function decimal(cursor: Cursor, account: SnapshotAccount): Promise<void> {
  const special = await cursor.number(1);
  snapshotCheck(special <= 3, "Invalid array NUMERIC special");
  if (special !== 0) return;
  const scale = await cursor.number(2),
    length = await cursor.number(4);
  snapshotCheck(scale <= 16383, "Invalid array NUMERIC scale");
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
      for (const byte of part) snapshotCheck(byte === sign, "Oversized array NUMERIC coefficient");
      remaining -= part.length;
    }
    const parts: string[] = [];
    for (let end = count; end > 0; end -= 1024) {
      await snapshotPace(cursor.source.column.work);
      let hex = "";
      for (let i = end - 1; i >= Math.max(0, end - 1024); i--)
        hex += (bytes[i] ?? 0).toString(16).padStart(2, "0");
      parts.push(hex);
    }
    const value = count === 0 ? 0n : BigInt.asIntN(count * 8, BigInt(`0x${parts.join("")}`));
    snapshotCheck(numericFits(value, scale), "Array NUMERIC exceeds supported precision");
  } finally {
    account.free(charge);
  }
}
async function array(
  cursor: Cursor,
  element: PhysicalType,
  bitmap: Cursor,
  account: SnapshotAccount,
): Promise<void> {
  snapshotCheck(
    element.kind !== "json" && element.kind !== "array" && element.kind !== "locator",
    "Unsupported portable array element type",
  );
  const version = await cursor.number(1),
    tag = await cursor.number(1),
    dimensions = await cursor.number(1),
    flags = await cursor.number(1);
  snapshotCheck(
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
    snapshotCheck(
      length > 0 && lower + length - 1 <= 2147483647,
      "Invalid portable array dimension",
    );
    count *= length;
    snapshotCheck(
      count <= MAX_ARRAY_ELEMENTS && count <= (cursor.end - cursor.begin) * 8,
      "Portable array dimensions exceed value",
    );
  }
  const nulls = (flags & 1) !== 0,
    bitmapBytes = nulls ? Math.ceil(count / 8) : 0;
  bitmap.at = cursor.at;
  cursor.skip(bitmapBytes);
  let bits = 0;
  for (let i = 0; i < count; i++) {
    if ((i & 1023) === 0) await snapshotPace(cursor.source.column.work);
    if (nulls && (i & 7) === 0) {
      bits = await bitmap.number(1);
      if (i + 8 > count)
        snapshotCheck(bits >>> (count - i) === 0, "Nonzero array bitmap tail bits");
    }
    if (nulls && (bits & (1 << (i & 7))) !== 0) continue;
    switch (element.kind) {
      case "bool":
        snapshotCheck((await cursor.number(1)) <= 1, "Invalid array boolean");
        break;
      case "int32": {
        const value = await cursor.number(4, true);
        snapshotCheck(
          element.bits !== 16 || (value >= -32768 && value <= 32767),
          "Array SMALLINT out of range",
        );
        break;
      }
      case "time": {
        const value = await cursor.i64();
        snapshotCheck(value >= 0n && value <= 86400000000n, "Array TIME out of range");
        break;
      }
      case "date":
      case "float32":
        cursor.skip(4);
        break;
      case "int64":
      case "timestamp":
      case "float64":
        cursor.skip(8);
        break;
      case "uuid":
      case "interval":
        cursor.skip(16);
        break;
      case "decimal":
        await decimal(cursor, account);
        break;
      case "bytes":
        cursor.skip(await cursor.number(4));
        break;
      case "text":
      case "enum":
        await text(cursor, await cursor.number(4), element, account);
        break;
    }
  }
  snapshotCheck(cursor.at === cursor.end, "Trailing portable array bytes");
}

/** Streaming JSON syntax recognizer: no parsed object/string/number value is retained. */
async function json(cursor: Cursor, account: SnapshotAccount, binary: boolean): Promise<void> {
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
    snapshotCheck(s === 9 || s === 3 || s === 5 || s === 6, "Unexpected JSON value");
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
      snapshotCheck(code >= 0xdc00 && code <= 0xdfff, "Unpaired JSONB surrogate");
      pendingHigh = false;
    } else {
      snapshotCheck(code !== 0 && (code < 0xdc00 || code > 0xdfff), "Invalid JSONB Unicode unit");
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
    snapshotCheck(exponent <= 1073741823, "JSONB numeric exponent out of range");
    const scale = fractionDigits - (exponentNegative ? -exponent : exponent);
    snapshotCheck(
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
          snapshotCheck(/^[0-9a-fA-F]$/.test(c), "Invalid JSON unicode escape");
          unicodeValue = unicodeValue * 16 + Number.parseInt(c, 16);
          if (--unicode === 0) unit(unicodeValue);
          return;
        }
        if (escaped) {
          snapshotCheck('"\\/bfnrtu'.includes(c), "Invalid JSON escape");
          if (c === "u") {
            unicode = 4;
            unicodeValue = 0;
          } else snapshotCheck(!binary || !pendingHigh, "Unpaired JSONB surrogate");
          escaped = false;
          return;
        }
        if (c === "\\") {
          escaped = true;
          return;
        }
        if (c === '"') {
          snapshotCheck(!binary || !pendingHigh, "Unpaired JSONB surrogate");
          parser.token = "none";
          if (key) stack[depth - 1] = 2;
          return;
        }
        snapshotCheck(c.charCodeAt(0) >= 32, "Unescaped JSON control character");
        unit(c.codePointAt(0) ?? 0);
        return;
      }
      if (parser.token === "literal") {
        snapshotCheck(c === literal[literalAt++], "Invalid JSON literal");
        if (literalAt === literal.length) parser.token = "none";
        return;
      }
      if (parser.token === "number") {
        if (numberState === 0) {
          snapshotCheck(digit(c), "Truncated JSON number");
          mantissa(c, false);
          numberState = c === "0" ? 1 : 2;
          return;
        }
        if (numberState === 3) {
          snapshotCheck(digit(c), "Truncated JSON fraction");
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
          snapshotCheck(digit(c), "Truncated JSON exponent");
          exponentDigit(c);
          numberState = 7;
          return;
        }
        if (numberState === 6) {
          snapshotCheck(digit(c), "Truncated JSON exponent");
          exponentDigit(c);
          numberState = 7;
          return;
        }
        if (digit(c)) {
          snapshotCheck(numberState !== 1, "JSON leading zero");
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
        snapshotCheck(s === 0 || s === 4, "Unexpected JSON object end");
        depth--;
        return;
      }
      if (c === "]") {
        snapshotCheck(s === 5 || s === 7, "Unexpected JSON array end");
        depth--;
        return;
      }
      if (c === ":") {
        snapshotCheck(s === 2, "Unexpected JSON colon");
        stack[depth - 1] = 3;
        return;
      }
      if (c === ",") {
        snapshotCheck(s === 4 || s === 7, "Unexpected JSON comma");
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
      snapshotCheck(c === "-" || digit(c), "Invalid JSON token");
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
      throw new SnapshotFormatError("Invalid JSON UTF-8");
    }
  };
  try {
    while (cursor.at < cursor.end) {
      const chunk = await cursor.chunk(cursor.end - cursor.at),
        decoded = decode(chunk);
      for (const c of decoded) feed(c);
    }
    for (const c of decode()) feed(c);
    snapshotCheck(
      parser.token === "none" || (parser.token === "number" && [1, 2, 4, 7].includes(numberState)),
      "Truncated JSON token",
    );
    if (parser.token === "number") finishNumber();
    snapshotCheck(parser.root && depth === 0, "Incomplete portable JSON");
  } finally {
    account.free(stack.length);
  }
}

export async function validateSnapshotValue(
  column: SnapshotStagedColumn,
  space: ExtentReader,
  offset: number,
  length: number,
): Promise<void> {
  const type = column.column.type;
  if (type.kind === "bytes") return;
  snapshotCheck(
    ["array", "json", "enum", "text"].includes(type.kind),
    "Unsupported structured portable type",
  );
  const account = new SnapshotAccount(column.budget);
  let source: SnapshotColumnBytes | undefined, bitmapSource: SnapshotColumnBytes | undefined;
  try {
    // Two 8KiB windows, scalar scratch, UTF-8 decoding strings and parser wrappers.
    account.take(131072);
    source = new SnapshotColumnBytes(column, space);
    bitmapSource = new SnapshotColumnBytes(column, space);
    const cursor = new Cursor(source, offset, offset + length),
      bitmap = new Cursor(bitmapSource, offset, offset + length);
    await snapshotPace(column.work);
    if (type.kind === "array") await array(cursor, type.element, bitmap, account);
    else if (type.kind === "json") await json(cursor, account, type.binary);
    else await text(cursor, length, type, account);
  } finally {
    source?.close();
    bitmapSource?.close();
    account.close();
  }
}
