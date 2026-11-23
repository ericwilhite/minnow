import {stepSignal,abortableStep,checkStepOpensDuringAbort,heldStepSignals} from './step-signal.mjs';
import assert from 'node:assert/strict';
for(const close of [false,true]) {
 const p=new AbortController(),q=new AbortController();const a=abortableStep('a',p.signal,q.signal),c=abortableStep('c');let refused;
 p.signal.addEventListener('abort',()=>q.abort('inner'));
 q.signal.addEventListener('abort',()=>{try{const b=abortableStep('b',a.signal);b.signal.addEventListener('abort',()=>c.abort('extra'));if(close)b.close();}catch(e){refused=e.code}});
 p.abort('outer');assert.equal(c.signal.aborted,false);assert.equal(refused,'MINNOW_STEP_SIGNAL_INVARIANT');a.close();c.close();console.log({close,victim:c.signal.aborted,refused});
}
function mixed(native){
 const p=new AbortController();const events=[];
 const open=(name,...parents)=>{const s=native?{signal:AbortSignal.any(parents),close(){}}:stepSignal(name,...parents);s.signal.addEventListener('abort',()=>events.push(name));return s};
 const a=open('a',p.signal),b=open('b',p.signal);
 const x=AbortSignal.any([a.signal]);x.addEventListener('abort',()=>events.push('x'));
 p.abort('p');a.close();b.close();return events;
}
console.log('mixed events',JSON.stringify({native:mixed(true),step:mixed(false)}));
function victim(native){
 const p=new AbortController(),q=new AbortController();const open=(...ps)=>native?{signal:AbortSignal.any(ps),close(){}}:stepSignal('mixed',...ps);
 const a=open(p.signal);const x=AbortSignal.any([p.signal]);const b=open(p.signal);const c=open(q.signal);
 x.addEventListener('abort',()=>q.abort('x'));b.signal.addEventListener('abort',()=>q.abort('b'));
 p.abort('p');const reason=c.signal.reason;[a,b,c].forEach(s=>s.close());return reason;
}
console.log('mixed final reason',JSON.stringify({native:victim(true),step:victim(false)}));
checkStepOpensDuringAbort(true);const p=new AbortController();p.abort('done');stepSignal('finished',p.signal).close();assert.deepEqual(checkStepOpensDuringAbort(false),[]);console.log('held',heldStepSignals());
function observePending(native){
 const p=new AbortController();const open=(...parents)=>{if(native){const own=new AbortController();return {signal:AbortSignal.any([...parents,own.signal]),abort:r=>own.abort(r),close(){}}}return abortableStep('reads',...parents)};
 const a=open(p.signal),c=open();let observed;
 p.signal.addEventListener('abort',()=>{observed=a.signal.aborted;if(!a.signal.aborted)c.abort('observed live');});
 p.abort('p');const result={observed,victim:c.signal.aborted,reason:c.signal.reason};a.close();c.close();return result;
}
console.log('pending read final state',JSON.stringify({native:observePending(true),step:observePending(false)}));
