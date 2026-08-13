export interface DragDeps {
  /** Called with the total offset from where the press started, not the step since last time. */
  onMove(dx: number, dy: number): void;
  /** Return false to refuse the drag — a maximized window has nowhere to move to. */
  onStart?(): boolean;
  onEnd?(): void;
}

/**
 * One pointer-drag implementation, shared by the window's edges and the console's splitter.
 *
 * The move and end listeners go on the window rather than the handle, so a drag survives the
 * pointer leaving a one-pixel-wide grip — which it does immediately. Pointer capture is a
 * best-effort addition on top: it stops the host page reacting to a pointer that is mid-drag, but
 * it throws for a pointer the browser does not consider active, and losing it must not cost the
 * drag.
 *
 * A press that lands on a control is left alone, because starting a drag calls preventDefault and
 * that cancels the click which would otherwise follow.
 */
export function draggable(handle: HTMLElement, deps: DragDeps): void {
  handle.addEventListener("pointerdown", (event: PointerEvent) => {
    if (event.button !== 0) return;
    if (event.target instanceof Element && event.target.closest("button, input, select, a")) {
      return;
    }
    if (deps.onStart !== undefined && !deps.onStart()) return;

    const originX = event.clientX;
    const originY = event.clientY;
    const { pointerId } = event;
    let captured = false;
    try {
      handle.setPointerCapture(pointerId);
      captured = true;
    } catch {
      // Not an active pointer; the window listeners below carry the gesture regardless.
    }
    event.preventDefault();

    const move = (moved: PointerEvent): void => {
      deps.onMove(moved.clientX - originX, moved.clientY - originY);
    };
    const end = (): void => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", end);
      window.removeEventListener("pointercancel", end);
      if (captured) handle.releasePointerCapture(pointerId);
      deps.onEnd?.();
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", end);
    window.addEventListener("pointercancel", end);
  });
}
