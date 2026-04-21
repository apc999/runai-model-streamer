#pragma once

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3-crt/S3CrtClient.h>

#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace runai::llm::streamer::impl::alluxio
{

// AlluxioInit owns two pieces of process-wide state and — critically —
// the ordering between their destruction:
//
//   1. AWS SDK global runtime (Aws::InitAPI / Aws::ShutdownAPI)
//   2. A process-wide cache of S3CrtClient shared_ptrs keyed by worker
//      endpoint. Constructing an S3CrtClient re-parses the system CA
//      bundle (X509 / RSA / EC decode), which profiling identified as
//      a hot path; deduping across AlluxioClient pool slots (8 per
//      process by default) recovered ~10% aggregate throughput.
//
// Previously the cache lived as a class-static on AlluxioClient. Its
// destructor was sequenced by the C++ static-destruction chain, with
// no ordering guarantee vs Aws::ShutdownAPI (another static inside
// obj_open_backend). When ShutdownAPI ran first, the cache's destructor
// then tried to tear down S3CrtClient thread pools with their underlying
// aws-c-* runtime state already gone -> hang on pthread_join. Observed
// at 6-node scale as 518 idle threads, 0 established TCP, declining
// CPU (350% -> 210%) and indefinite process lifetime after the last
// iteration finished. See PR-to-come / commit message for evidence.
//
// Fix: bundle the cache into AlluxioInit. ~AlluxioInit() clears the
// cache (destroying all S3CrtClient instances) before calling
// Aws::ShutdownAPI, so CRT teardown always runs with SDK state still live.
struct AlluxioInit
{
    AlluxioInit();
    ~AlluxioInit();

    AlluxioInit(const AlluxioInit&) = delete;
    AlluxioInit& operator=(const AlluxioInit&) = delete;

    // Singleton accessor. First call constructs; destruction is deferred
    // to program exit (function-local static). Callers inside the plugin
    // should use this rather than constructing their own instance.
    static AlluxioInit& instance();

    // Return (and cache) the CRT client for a given worker endpoint.
    // `base_cfg` is copied; its endpointOverride is replaced with
    // `endpoint`. If `creds` is non-null, the client is constructed
    // with explicit credentials; otherwise the default credential chain.
    std::shared_ptr<Aws::S3Crt::S3CrtClient>
        get_or_create_worker_client(
            const std::string& endpoint,
            const Aws::S3Crt::ClientConfiguration& base_cfg,
            const Aws::Auth::AWSCredentials* creds);

    Aws::SDKOptions options;

 private:
    // LRU cache of S3CrtClient by endpoint. Bounded at _capacity entries
    // so long-running processes with pod reshuffles don't accumulate
    // dead endpoints forever (each CRT client owns ~80 threads and
    // non-trivial memory). Default 64 endpoints; override via
    // RUNAI_STREAMER_ALLUXIO_CLIENT_CACHE_MAX.
    //
    // Implementation: std::list holds (endpoint, client) in LRU order
    // (front = most-recently-used); unordered_map points to list iters
    // for O(1) lookup. On hit, move to front. On miss with size ==
    // capacity, drop back entry (= LRU).
    using CacheEntry =
        std::pair<std::string, std::shared_ptr<Aws::S3Crt::S3CrtClient>>;
    using CacheList = std::list<CacheEntry>;

    CacheList _worker_clients_lru;
    std::unordered_map<std::string, CacheList::iterator> _worker_clients_idx;
    std::mutex _worker_clients_mutex;
    size_t _capacity;
};

}; //namespace runai::llm::streamer::impl::alluxio
