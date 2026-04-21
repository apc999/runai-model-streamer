# Alluxio plugin — hardening audit

Walk-through of the plugin's external-resource taxonomy using the
`alluxio-feature-harden` skill (3-dim check per row: **Coverage** —
does the code raise/abort or hang/silently succeed? **Message** —
actionable for a user? **State** — any leftover on failure?).

This audit covers the state of the plugin after the shutdown-ordering
fix (cache into `AlluxioInit`), LRU cache cap, probe retry, caller
contract docs, and `_responder` init-race guard.

## backend_api contract summary (from `common/backend_api/object_storage/object_storage.h`)

- `obj_request_read` — "buffer must remain valid until the completion
  event for this request indicates the read is finished or has failed"
- `obj_cancel_all_reads` — "Cancellation is best-effort. Completion
  events should still be expected for requests that were already in
  flight or too late to cancel"
- `obj_remove_client` — no explicit pre-condition on outstanding
  requests or drain status. **Implicit contract** (inferred from the
  buffer-lifetime clause above): caller drains all completions before
  remove.

Framework uses RAII wrappers (`S3ClientWrapper`, and mirror pattern
for other backends) — one wrapper per caller thread, serialized
request issuance per wrapper. Cross-thread sharing of the same
`ObjectClientHandle_t` is not a usage pattern the framework exhibits
today, but the spec doesn't forbid it.

---

## External resources

1. **AWS SDK runtime** — global `Aws::InitAPI`/`ShutdownAPI` state
2. **CRT worker threads** — per-`S3CrtClient` thread pools
3. **Endpoint-keyed CRT client cache** — process-wide LRU in `AlluxioInit`
4. **Caller-owned `destination_buffer`** — raw `char*` passed to `async_read`
5. **Gateway HTTP probe connection** — `_probe_http` keep-alive
6. **Responder queue** (`common::SharedQueue`) — request_id → completion

---

## Audit table

| # | Resource | Failure mode | Coverage | Message | State | Action |
|---|----------|--------------|----------|---------|-------|--------|
| 1 | SDK runtime | `Aws::InitAPI` fails silently (it's `void`) | ❌ post-conditions undefined; later calls segfault | ❌ no user-visible hint | n/a | **NEW**: add a smoke check in `obj_open_backend` (try construct a dummy `S3CrtClient`; if it throws/segfaults we already crashed; if succeeds, continue). Low-cost sanity. |
| 2 | SDK runtime | `Aws::ShutdownAPI` called while CRT clients still alive | ✅ fixed by cache-in-AlluxioInit | ✅ (doesn't reach user) | ✅ | **done** — repro verifies |
| 3 | CRT worker threads | `S3CrtClient` ctor throws (TLS parse, etc.) | ✅ `get_or_create_worker_client` propagates exception | ⚠️ error is generic CRT exception; no endpoint context | ✅ (nothing half-created) | **NEW**: wrap in `try/catch`, re-throw as `Exception(FileAccessError)` with endpoint in LOG(ERROR) |
| 4 | CRT worker threads | `GetObjectAsync` callback lost (not invoked) | ❌ `async_read_response()` blocks forever on `SharedQueue::pop` | ❌ no timeout, no diagnostic | ❌ client handle still thinks work is outstanding | **TODO**: add `pop_for(duration)` to `common::SharedQueue`. Separate PR (touches multi-backend shared util). Documented in client.h |
| 5 | Endpoint cache | Growth unbounded on pod reshuffle | ✅ fixed by LRU with cap (`RUNAI_STREAMER_ALLUXIO_CLIENT_CACHE_MAX`, default 64) | ✅ | ✅ (evicted entry's shared_ptr kept alive by live holders) | **done** |
| 6 | Endpoint cache | Eviction while a `AlluxioClient::_file_routes` still holds evicted `shared_ptr` | ✅ refcount keeps CRT client alive until last holder drops | ✅ | ✅ | no action — correct by construction |
| 7 | Endpoint cache | Two threads simultaneously miss on same key → double construction | ✅ mutex serializes; second thread sees cache hit on retry (but we don't retry — doubleconstruct escapes) | ⚠️ rare, produces 2 CRT clients briefly; the loser is dropped when shared_ptr goes out of scope | ✅ | **IMPROVE**: double-check pattern inside the lock: after creating, re-lookup in case another winner already inserted. Very low priority (pre-fix already exhibited this) |
| 8 | `destination_buffer` | Caller frees buffer before last CRT callback fires | ❌ write-after-free in CRT thread | ❌ no detection | ❌ memory corruption | **NEW**: at minimum document caller contract (**DONE** in client.h comment). Proper fix requires API change (take `shared_ptr<byte[]>`) — out of scope |
| 9 | `destination_buffer` | First-chunk error reported; subsequent in-flight chunks still write | ⚠️ chunks already issued keep running; caller may have assumed "Error → stop writing" | ❌ subtle contract not obvious | ⚠️ buffer-write continues after error response | **DONE** — documented in client.h. Consider `cancelAll` on error in future CRT version |
| 10 | Probe connection | Keep-alive closed by server after 307 | ✅ fixed — 2-attempt retry with reconnect | ✅ LOG(DEBUG) on retry | ✅ | **done** |
| 11 | Probe connection | Gateway unreachable entirely (DNS fail / TCP refused) | ✅ after retry, throws `FileAccessError` | ⚠️ generic "File access error" message; no distinction of DNS/TCP/HTTP | ✅ | **NEW**: borrow 161d936 — post-mortem TCP/DNS check on probe failure, put verdict in LOG(ERROR). Keep user-facing error unchanged (runai framework wraps it) |
| 12 | Probe connection | Location header absent on 3xx | ✅ explicit check, throws | ✅ "got <code> without Location header" with URL | ✅ | no action |
| 13 | Probe connection | Location header refers to a malformed URL | ✅ `parse_endpoint` returns empty → throws | ✅ "failed to parse Location" with URL | ✅ | no action |
| 14 | Probe connection | Non-2xx, non-3xx response (e.g. 401, 500) | ✅ throws with code | ✅ includes `code` in message | ✅ | no action |
| 15 | Responder queue | Caller calls `async_read` then drops client before `async_read_response` | ⚠️ responder is `shared_ptr`, captured by CRT callback → survives. Callback writes to destroyed client's buffer pointer (= resource #8 race) | ❌ no error signal | ❌ write-after-free | **NEW**: document in `stop()` — already done. Consider linking client handle lifetime to outstanding requests |
| 16 | Responder queue | `stop()` is called mid-stream | ✅ `_stop=true` gates further chunks; `responder->stop()` unblocks `pop` | ⚠️ in-flight chunks still complete and push (per contract doc) | ✅ | **DONE** — semantics documented |
| 17 | Responder queue | Multiple `async_read` concurrently (same client) | ⚠️ increments `_running` correctly but share one buffer?? | n/a | ✅ | check: callers pass distinct `destination_buffer` / `request_id`? Assume yes per runai contract |
| 18 | Client lifecycle | `obj_remove_client` during active `async_read` | ⚠️ `AlluxioClientMgr::push` doesn't lock against concurrent callers | ❌ | ❌ UAF of `AlluxioClient *` | **NEW**: hold lock in ClientMgr `push`/`pop`, or document "remove only after stop() + drain" contract |

---

## Summary of gaps to open as tickets

After this pass (and the double-check that revised #4 / #8 / #18):

**P0 — correctness**

- **#4** `async_read_response` hangs forever if CRT callback is lost.
  **Revised scope**: adding `pop_for` alone is insufficient. Full fix
  is a **3-piece package** and should land as one PR:
    - (a) `SharedQueue::pop_for(duration)` + `Semaphore::wait_for` —
      ~20 LOC, additive, backward-compatible (touches multi-backend
      shared util).
    - (b) `AlluxioClient::stop()` also calls
      `worker_client->DisableRequestProcessing()` on involved CRT
      clients — prevents NEW chunks from being issued on that client.
      (True in-flight cancellation is a CRT limitation: the public
      SDK has no per-request cancel; `aws_s3_meta_request_cancel()`
      lives at aws-c-s3 layer and isn't plumbed.)
    - (c) New `drain()` method that blocks until the in-flight
      counter on the shared responder reaches zero, to be called
      after `stop()` before safe destruction.
  Rationale: with (a) only, a caller that times out and destroys the
  client will still have CRT callbacks firing into freed memory. The
  three pieces together give a safe "timeout → cancel → drain →
  destroy" recovery loop.

- **#8/#15** caller-buffer lifetime UAF.
  **Revised scope**: this is not an independent row; it's a
  consequence of #18. In the normal runai RAII flow the framework
  drains completions before destructing the wrapper, so the buffer
  outlives every CRT callback that writes to it. The only paths to a
  real UAF are (a) a bug in the framework's drain loop (out of our
  scope) or (b) the #18 race below. Action items:
    - Keep the caller-contract documentation added in `client.h`.
    - Add a `~AlluxioClient` DCHECK that the responder is empty and
      `_running==0`; on violation, `LOG(FATAL)` so the use-after-free
      becomes a visible crash at destruction instead of silent data
      corruption later.

- **#18** `obj_remove_client` race.
  **Revised scope**: `ClientMgr::push/pop` is **already mutex-protected**
  — the lock covers the pool's data structure integrity, not the
  in-flight safety of the client instance. The real race is:
    Two concurrent `async_read` calls on the same `AlluxioClient*`.
  Static reading of `client.cc:294-301` (the lazy `_responder` init)
  confirms the race: two threads read `_responder==nullptr`, both
  allocate a fresh `Responder`, the second assignment orphans the
  first. Captured-by-value CRT callbacks then push into a mix of
  alive and orphaned responders; `async_read_response()` pops from
  whatever `_responder` ended up winning, with a counter out of sync.
  Not triggered by today's framework (single-thread per wrapper), but
  demonstrable by construction. **Done in this round**: guarded the
  lazy init with `_responder_mutex` (minimal cost, removes the
  correctness footgun). Remaining defensive work:
    - Outstanding-request counter in `AlluxioClient`; `stop()` blocks
      until it reaches zero (couples naturally with the drain path
      from #4).
    - Documentation on `obj_remove_client` that the caller must not
      hold outstanding requests on the handle being removed.

**P1 — diagnostics**
- #1: `Aws::InitAPI` failure undetected — add smoke check.
- #3: `S3CrtClient` ctor failure — wrap with endpoint context.
- #11: opaque probe failure — post-mortem endpoint probe (à la
  monkey-patch commit 161d936).

**P2 — nice to have**
- #7: double-construct on simultaneous miss — double-check inside
  lock.

---

## Items addressed in this round

- **#2** shutdown ordering — fix committed, reproducer added.
- **#5** cache unbounded growth — LRU cap with env knob.
- **#10** probe single-shot — 2-attempt retry.
- **#8/#9/#16** caller contracts — documented in `client.h`.
- **#18 (partial)** concurrent `async_read` on the same client now
  guarded by `_responder_mutex`; full fix still pending `drain()` +
  outstanding-counter.

## Meta observations

Three patterns from the monkey-patch commit history that apply here
even when the mechanism doesn't:

1. **Every fix uncovers a new gap.** #2 (this round) was hidden under
   the perf commit `eef33a6`. The LRU cap itself (#5) only made sense
   after #2 exposed the cache as a durable resource. Don't declare done
   — walk the taxonomy once more next pass.

2. **Injection testing is cheaper than natural reproduction.** Our
   `repro_shutdown_hang.cc` (for #2) should grow: add a `--fault=probe-fail`
   mode (for #10 retry coverage), a `--fault=ctor-throw` mode (for #3),
   etc. Each takes < 30 min and catches a class of regression.

3. **External resource > internal logic.** This table is ~15 rows of
   external-resource failures and ~0 rows of "what if the internal
   logic has a bug" — because that's where real bugs come from.
