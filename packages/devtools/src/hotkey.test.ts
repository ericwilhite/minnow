import { describe, expect, it } from "vitest";
import { matchesHotkey } from "./hotkey.js";

function press(key: string, modifiers: Partial<KeyboardEvent> = {}): KeyboardEvent {
  return {
    key,
    metaKey: false,
    ctrlKey: false,
    shiftKey: false,
    altKey: false,
    ...modifiers,
  } as KeyboardEvent;
}

describe("matchesHotkey", () => {
  it("matches mod through either Cmd or Ctrl, so one string covers every platform", () => {
    expect(matchesHotkey("mod+shift+d", press("d", { metaKey: true, shiftKey: true }))).toBe(true);
    expect(matchesHotkey("mod+shift+d", press("d", { ctrlKey: true, shiftKey: true }))).toBe(true);
  });

  it("requires every named modifier", () => {
    expect(matchesHotkey("mod+shift+d", press("d", { metaKey: true }))).toBe(false);
    expect(matchesHotkey("mod+shift+d", press("d", { shiftKey: true }))).toBe(false);
  });

  it("rejects extra modifiers so a different chord cannot trigger it", () => {
    expect(matchesHotkey("mod+d", press("d", { metaKey: true, shiftKey: true }))).toBe(false);
    expect(matchesHotkey("mod+d", press("d", { metaKey: true, altKey: true }))).toBe(false);
  });

  it("ignores case in both the hotkey and the pressed key", () => {
    expect(matchesHotkey("MOD+D", press("D", { ctrlKey: true }))).toBe(true);
  });

  it("matches an unmodified key", () => {
    expect(matchesHotkey("f2", press("F2"))).toBe(true);
    expect(matchesHotkey("f2", press("F2", { ctrlKey: true }))).toBe(false);
  });

  it("treats an empty hotkey as off", () => {
    expect(matchesHotkey("", press("d", { metaKey: true }))).toBe(false);
    expect(matchesHotkey("   ", press(" "))).toBe(false);
  });
});
