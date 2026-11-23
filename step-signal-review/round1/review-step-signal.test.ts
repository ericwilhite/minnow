import { expect, it } from 'vitest';
import { stepSignal, heldStepSignals } from '../step-signal.js';
it('preserves first parent reason during a reentrant abort of another parent', () => {
 const a = new AbortController(), b = new AbortController();
 const first = new Error('caller cancelled'), second = new Error('cleanup stopped');
 a.signal.addEventListener('abort', () => b.abort(second));
 const native = AbortSignal.any([a.signal, b.signal]);
 const before = heldStepSignals();
 const step = stepSignal('review', a.signal, b.signal);
 a.abort(first);
 expect(native.reason).toBe(first);
 expect(heldStepSignals()).toBe(before);
 expect(step.signal.reason).toBe(first);
});
it('re-adds after last close and removes a closed sibling during abort', () => {
 const parent = new AbortController();
 const before = heldStepSignals();
 const old = stepSignal('old', parent.signal); old.close();
 const a = stepSignal('a', parent.signal), b = stepSignal('b', parent.signal);
 a.signal.addEventListener('abort', () => b.close());
 const reason = new Error('stop'); parent.abort(reason);
 expect(a.signal.reason).toBe(reason);
 expect(heldStepSignals()).toBe(before);
 const late = stepSignal('late', parent.signal);
 expect(late.signal.reason).toBe(reason);
 expect(heldStepSignals()).toBe(before);
});
