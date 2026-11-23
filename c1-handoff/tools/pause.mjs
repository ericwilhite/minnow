// pause.mjs PORT SAMPLES: attach to a Node inspector and print the paused call stack SAMPLES times.
const [port, samples = "5"] = process.argv.slice(2);
const list = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
const ws = new WebSocket(list[0].webSocketDebuggerUrl);
let id = 0, n = 0;
const send = (method, params = {}) => ws.send(JSON.stringify({ id: ++id, method, params }));
ws.onopen = () => { send("Debugger.enable"); setTimeout(() => send("Debugger.pause"), 500); };
ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m.method !== "Debugger.paused") return;
  console.log(`[sample ${++n}]`);
  for (const f of m.params.callFrames.slice(0, 18))
    console.log(`  ${f.functionName || "<anon>"} ${f.url.replace(/.*\/next\//, "")}:${f.location.lineNumber + 1}`);
  send("Debugger.resume");
  if (n < Number(samples)) setTimeout(() => send("Debugger.pause"), 400);
  else { ws.close(); process.exit(0); }
};
setTimeout(() => { console.log("no pause"); process.exit(1); }, 20000);
