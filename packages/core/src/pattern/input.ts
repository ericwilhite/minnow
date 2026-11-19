/** Matching positions are representation-native boundaries: UTF16 for legacy SQL,
 * UTF8 for the native executor. Captures never force an input-sized decoded copy. */
export interface RegexInput {
  readonly length: number;
  readonly limit: number;
  codePointAt(at: number): number | undefined;
  advance(at: number, point: number): number;
  previous(at: number): number;
  newline(at: number): boolean;
}
export class StringRegexInput implements RegexInput {
  readonly length: number;
  constructor(
    readonly text: string,
    readonly limit: number,
  ) {
    this.length = text.length;
  }
  codePointAt(at: number): number | undefined {
    return this.text.codePointAt(at);
  }
  advance(at: number, point: number): number {
    return at + (point > 0xffff ? 2 : 1);
  }
  previous(at: number): number {
    const low = this.text.charCodeAt(at - 1),
      high = this.text.charCodeAt(at - 2);
    return at - (low >= 0xdc00 && low <= 0xdfff && high >= 0xd800 && high <= 0xdbff ? 2 : 1);
  }
  newline(at: number): boolean {
    return this.text.charCodeAt(at) === 10;
  }
}
