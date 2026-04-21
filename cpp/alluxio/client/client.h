#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <optional>
#include <unordered_map>

#include <aws/core/http/HttpClient.h>

#include "alluxio/client_configuration/client_configuration.h"
#include "common/backend_api/response/response.h"

#include "common/backend_api/object_storage/object_storage.h"
#include "common/client_mgr/client_mgr.h"
#include "common/storage_uri/storage_uri.h"
#include "common/s3_wrapper/s3_wrapper.h"
#include "common/shared_queue/shared_queue.h"
#include "common/range/range.h"

namespace runai::llm::streamer::impl::alluxio
{

struct AlluxioClientBase : common::IClient
{
    AlluxioClientBase(const common::backend_api::ObjectClientConfig_t & config);

    // verify that client's credentials have not changed
    bool verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const;

 protected:
    std::optional<Aws::String> _key;
    std::optional<Aws::String> _secret;
    std::optional<Aws::String> _token;
    std::optional<Aws::String> _region;
    const std::optional<Aws::String> _endpoint;
    std::unique_ptr<Aws::Auth::AWSCredentials> _client_credentials;
    const size_t _chunk_bytesize;

 private:
    bool verify_credentials_member(const std::optional<Aws::String>& client_member, const std::optional<Aws::String>& input_member, const char * name) const;
};

struct AlluxioClient : AlluxioClientBase
{
    AlluxioClient(const common::backend_api::ObjectClientConfig_t & config);

    // Issue an asynchronous read. Splits `range` into chunks and fires
    // N CRT GetObjectAsync requests; each completion pushes into the
    // shared responder queue.
    //
    // CALLER CONTRACT — `destination_buffer`:
    //   MUST remain valid until the matching `async_read_response()`
    //   has returned for this `request_id`. The CRT callback writes
    //   into `destination_buffer` at chunk offsets; the pointer is
    //   captured by value, NOT by ownership. Freeing / reusing the
    //   buffer before response arrival -> use-after-write.
    //
    //   A single error response is emitted on the FIRST failing chunk
    //   (via an atomic `is_success->exchange(false)`); chunks already
    //   in flight still run and may still write into the buffer.
    //   Callers must keep the buffer alive until they've either seen
    //   a Success or retained the buffer through the drain of any
    //   in-flight chunks — there is no guaranteed "stop writing" point
    //   before the responder's completion.
    common::backend_api::ResponseCode_t async_read(const char* path,
                                                   common::backend_api::ObjectRange_t range,
                                                   char* destination_buffer,
                                                   common::backend_api::ObjectRequestId_t request_id);

    // Blocks until the next chunk-level response is available.
    // NOTE: there is NO timeout — if the underlying CRT callback is
    // never invoked (e.g. a CRT-internal bug), this call waits
    // indefinitely. Upper-layer cancellation is via `stop()`.
    // TODO: consider a timed variant once `common::SharedQueue`
    // exposes `pop_for(duration)`.
    common::backend_api::Response async_read_response();

    // Best-effort abort of future work. Sets a stop flag checked at
    // chunk-issue boundaries in `async_read`, and cancels the responder
    // queue so any blocked `async_read_response()` returns promptly.
    //
    // NOTE: does NOT cancel CRT requests that were already issued.
    // In-flight `GetObjectAsync` callbacks will still fire and still
    // write into their `destination_buffer` — same caller contract as
    // above. If deterministic in-flight cancellation becomes necessary,
    // switch to CRT's per-operation cancellation API.
    void stop();

    using AlluxioClientBase::verify_credentials;

 private:
    // Resolve the owner worker for this file via a 1-byte Range GET to the
    // Gateway endpoint. Caches the result; subsequent calls for the same file
    // return immediately. The per-endpoint CRT client cache is owned by
    // AlluxioInit (shared process-wide); see alluxio_init.h for the ordering
    // rationale between that cache and Aws::ShutdownAPI.
    std::shared_ptr<Aws::S3Crt::S3CrtClient>
        resolve_worker_client(const common::s3::StorageUri& uri);

    std::atomic<bool> _stop;
    ClientConfiguration _client_config;

    // HTTP client used for the redirect probe only. Configured with
    // followRedirects=NEVER so we can read the 307 Location header.
    std::shared_ptr<Aws::Http::HttpClient> _probe_http;

    // Per-instance cache: "bucket/path" -> CRT client (shortcut to the worker
    // that owns this file). Stays per-instance because file ownership may
    // differ per caller in exotic setups.
    std::unordered_map<std::string, std::shared_ptr<Aws::S3Crt::S3CrtClient>> _file_routes;

    std::mutex _routing_mutex;

    // queue of asynchronous responses
    using Responder = common::SharedQueue<common::backend_api::Response>;
    std::shared_ptr<Responder> _responder;
};

}; //namespace runai::llm::streamer::impl::alluxio
