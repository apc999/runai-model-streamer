// Race regression test for the `_responder` lazy-init in
// AlluxioClient::async_read.
//
// Pre-fix (no _responder_mutex) pattern, at client.cc:294-301:
//
//     if (_responder == nullptr) {
//         _responder = std::make_shared<Responder>(1);
//     } else {
//         _responder->increment(1);
//     }
//
// Two concurrent first-entry async_read calls both read nullptr, both
// allocate a fresh Responder with _running=1. The second assignment
// overwrites the first; the orphan's refcount drops to zero and it's
// destroyed. Subsequent push() calls in the error path land on the
// survivor — but the survivor's _running was initialised to 1, not N,
// so after the first push _running drops to 0 and further pushes fail
// the `if (_running > 0)` gate inside SharedQueue::push() and set
// _unexpected_push_error.
//
// The visible symptom a caller sees is: N async_read calls produced
// fewer than N drainable responses. async_read_response() on the Nth
// pop returns FinishedError (or blocks forever, depending on state).
//
// This test:
//   1. Constructs an AlluxioClient with NO endpoint (so
//      resolve_worker_client throws immediately — async_read enters the
//      catch block and pushes one error response per call, without
//      ever talking to a real worker).
//   2. Spawns N threads, synchronised on a barrier, each calling
//      async_read.
//   3. Drains up to N responses with a hard wall-clock deadline.
//   4. Fails if fewer than N responses arrive (indicates the race).
//
// With the _responder_mutex fix applied: N responses always arrive.
// Without the fix: a significant fraction of runs see fewer than N
// responses and an `_unexpected_push_error` log line on stderr.

#include "alluxio/alluxio_init/alluxio_init.h"
#include "alluxio/client/client.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

namespace runai::llm::streamer::impl::alluxio
{

namespace
{

// Small barrier sufficient for this test (C++17 has no std::barrier).
struct Barrier
{
    explicit Barrier(int n) : _n(n), _waiting(0), _gen(0) {}

    void wait()
    {
        std::unique_lock<std::mutex> lk(_m);
        int gen = _gen;
        if (++_waiting == _n)
        {
            ++_gen;
            _waiting = 0;
            _cv.notify_all();
        }
        else
        {
            _cv.wait(lk, [&]{ return _gen != gen; });
        }
    }

 private:
    std::mutex _m;
    std::condition_variable _cv;
    int _n;
    int _waiting;
    int _gen;
};

// Drain up to `expected` responses from the client with a hard deadline.
// Returns how many were actually drained before the deadline fired.
unsigned drain_with_deadline(AlluxioClient& client, unsigned expected,
                             std::chrono::milliseconds deadline)
{
    std::atomic<unsigned> got{0};
    std::atomic<bool> done{false};

    std::thread drainer([&]{
        for (unsigned i = 0; i < expected; ++i)
        {
            auto resp = client.async_read_response();
            if (resp.ret == common::ResponseCode::FinishedError)
            {
                // No more responses are coming — survivor's _running
                // already hit zero while there were still pushes to
                // collect. Classic race symptom.
                break;
            }
            got.fetch_add(1, std::memory_order_relaxed);
        }
        done.store(true, std::memory_order_release);
    });

    auto end = std::chrono::steady_clock::now() + deadline;
    while (!done.load(std::memory_order_acquire)
           && std::chrono::steady_clock::now() < end)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!done.load(std::memory_order_acquire))
    {
        // Drainer is blocked on pop. Cancel responder queue to unstick.
        client.stop();
        drainer.join();
    }
    else
    {
        drainer.join();
    }

    return got.load(std::memory_order_relaxed);
}

} // anon

// The test name signals what it's guarding. With the fix, passes
// deterministically; without the fix, fails or hangs under TSan.
TEST(ResponderRace, ConcurrentFirstAsyncReadAllResponsesDrainable)
{
    // Warm up AWS SDK + the plugin singleton once. AlluxioClient's
    // constructor calls Aws::Http::CreateHttpClient which needs InitAPI.
    AlluxioInit::instance();

    common::backend_api::ObjectClientConfig_t cfg{};
    cfg.endpoint_url = nullptr;  // resolve_worker_client will throw
    cfg.default_storage_chunk_size = 1024 * 1024;
    cfg.initial_params = nullptr;
    cfg.num_initial_params = 0;

    // We want each iteration to exercise a fresh AlluxioClient (and
    // therefore a fresh `_responder == nullptr` condition) so the
    // lazy-init path is hit. Repeat several times to increase
    // probability of seeing the race absent the fix.
    constexpr int kIterations = 20;
    constexpr unsigned kThreadsPerIter = 64;

    for (int iter = 0; iter < kIterations; ++iter)
    {
        AlluxioClient client(cfg);

        Barrier barrier(static_cast<int>(kThreadsPerIter));
        std::vector<std::thread> ts;
        ts.reserve(kThreadsPerIter);

        std::vector<char> buf(4096, 0);
        common::backend_api::ObjectRange_t range{0, 1024};

        for (unsigned i = 0; i < kThreadsPerIter; ++i)
        {
            ts.emplace_back([&, i]{
                barrier.wait();
                (void)client.async_read("s3://bucket/file", range,
                                        buf.data(), i);
            });
        }
        for (auto& t : ts) t.join();

        auto drained = drain_with_deadline(
            client, kThreadsPerIter, std::chrono::seconds(5));

        EXPECT_EQ(drained, kThreadsPerIter)
            << "iter=" << iter
            << ": only drained " << drained
            << " / " << kThreadsPerIter
            << " responses — race on _responder lazy-init lost pushes "
               "(or pushes landed on orphaned Responder that was freed "
               "when the second assignment overwrote _responder).";
    }
}

}  // namespace runai::llm::streamer::impl::alluxio
