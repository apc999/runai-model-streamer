# Alluxio plugin — hardening audit

Walk-through of the plugin's external-resource taxonomy using the
`alluxio-feature-harden` skill (3-dim check per row: **Coverage** —
does the code raise/abort or hang/silently succeed? **Message** —
actionable for a user? **State** — any leftover on failure?).

This audit covers the state of the plugin after the shutdown-ordering
fix (cache into `AlluxioInit`), LRU cache cap, and probe retry.

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

After this pass the remaining items (by priority):

**P0 — correctness**
- #4: `async_read_response` hangs forever if CRT callback is lost → add `pop_for` to `SharedQueue` (separate PR, touches all backends)
- #8, #15: caller-buffer lifetime UAF — at minimum documented; real fix needs API change
- #18: `obj_remove_client` race — lock or document

**P1 — diagnostics**
- #1: `Aws::InitAPI` failure undetected — add smoke check
- #3: `S3CrtClient` ctor failure — wrap with endpoint context
- #11: opaque probe failure — post-mortem endpoint probe (à la monkey-patch commit 161d936)

**P2 — nice to have**
- #7: double-construct on simultaneous miss — double-check inside lock

---

## Items addressed in this round

- **#2** shutdown ordering — fix committed, reproducer added
- **#5** cache unbounded growth — LRU cap with env knob
- **#10** probe single-shot — 2-attempt retry
- **#8/#9/#16** caller contracts — documented in `client.h`

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
