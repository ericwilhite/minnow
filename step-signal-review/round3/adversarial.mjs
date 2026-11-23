import {stepSignal,abortableStep,checkStepOpensDuringAbort} from './review/packages/core/src/next/step-signal.ts';
function run(native, close) {
 const handles=[];
 const open=(...parents)=>{if(native){const own=new AbortController();return {signal:AbortSignal.any([...parents,own.signal]),abort:r=>own.abort(r),close(){}};} const s=abortableStep('probe',...parents);handles.push(s);return s;};
 const p=new AbortController(),q=new AbortController();
 const a=open(p.signal,q.signal),victim=open();let b;
 p.signal.addEventListener('abort',()=>q.abort('inner'));
 q.signal.addEventListener('abort',()=>{b=open(a.signal);b.signal.addEventListener('abort',()=>victim.abort('extra event'));if(close)b.close();});
 p.abort('outer');
 const result=[a,b,victim].map(s=>({aborted:s.signal.aborted,reason:s.signal.reason}));handles.forEach(s=>s.close());return result;
}
for(const close of [false,true]) console.log(JSON.stringify({close,native:run(true,close),step:run(false,close)}));
checkStepOpensDuringAbort(true);
const p=new AbortController();p.abort('finished');stepSignal('outside dispatch',p.signal).close();
console.log('false positive',checkStepOpensDuringAbort(false));
checkStepOpensDuringAbort(true);
const q=new AbortController(),a=stepSignal('base',q.signal);q.signal.addEventListener('abort',()=>stepSignal('through parent step',a.signal).close());q.abort('inside');console.log('parent step detection',checkStepOpensDuringAbort(false));
