# WebKit 26.6 replaces a SharedWorker when the page that created it closes

Found 2026-11-06 while running the owner lane on Playwright's Linux WebKit build 2359 (WebKit
26.6). Not yet confirmed in Safari on macOS; report it upstream once it is.

## What happens

On a cross-origin-isolated page (`Cross-Origin-Opener-Policy: same-origin` and
`Cross-Origin-Embedder-Policy: require-corp`), when the page that first created a SharedWorker
closes while another page still has a port to it:

- WebKit starts a new SharedWorker instance. Its global state is empty.
- The surviving pages' existing `MessagePort`s are reattached to the new instance: `onconnect`
  fires again for each, and the ports keep working.
- Messages a page posts during the switch are dropped without any error.

Without cross-origin isolation, and in WebKit 26.5 (build 2336), Chromium and Firefox, the
instance survives and nothing is lost.

## Measured

A bare page pair (no Minnow code), page B posting a ping to the SharedWorker every 20 ms while
page A closes, WebKit 2359 driven by Playwright 1.63:

| Run | Pings lost (of 40) | New instance after the close began |
| --- | ------------------ | ---------------------------------- |
| 1   | 4                  | ~210 ms                            |
| 2   | 6                  | ~270 ms                            |
| 3   | 14                 | ~440 ms                            |
| 4   | 5                  | ~240 ms                            |

Every run got a new instance (connection count back to 1, age reset). The same script without the
isolation headers, and on 2336, Chromium and Firefox, lost none.

## What it broke here

The broker (`broker.ts`) is that SharedWorker. When the owner's tab closed, the surviving tab
saw the host lock free at once and asked the broker for an owner. The question went out during
the switch and was dropped, and the connector waited for its answer forever (`connecting`, no
lock held or requested). A live owner in another tab would also have lost its registration, so
new tabs would get `unreachable` until it registered again.

The fix is in election.ts "Reaching the owner": bounded, retried broker requests with a request
id the broker answers once, and an instance id that makes owners register again and clients ask
again.

## Repro

Scripts and raw results: `~/.cache/b5-opfs-close/` on the machine where this was found
(`shared.mjs` with `ISOLATED=1 BURST=1`; `FINDINGS.md`). The essentials, to rebuild elsewhere:

1. Serve two pages and a SharedWorker script (`type: "module"`) from one origin with the two
   isolation headers above.
2. In the SharedWorker, answer every message with the number of connections it has seen and its
   age.
3. Open page A, then page B. Both connect (`new SharedWorker(url, { name })`) and ping it once.
4. From page B, post a ping every 20 ms. Close page A 100 ms in.
5. Count the pings that got no answer, and compare connection count and age before and after.
