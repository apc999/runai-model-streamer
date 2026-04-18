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

    common::backend_api::ResponseCode_t async_read(const char* path,
                                                   common::backend_api::ObjectRange_t range,
                                                   char* destination_buffer,
                                                   common::backend_api::ObjectRequestId_t request_id);

    common::backend_api::Response async_read_response();

    // Stop sending requests to the object store.
    void stop();

    using AlluxioClientBase::verify_credentials;

 private:
    // Resolve the owner worker for this file via a 1-byte Range GET to the
    // Gateway endpoint. Caches the result; subsequent calls for the same file
    // return immediately.
    std::shared_ptr<Aws::S3Crt::S3CrtClient>
        resolve_worker_client(const common::s3::StorageUri& uri);

    // Return the CRT client for a given worker endpoint, creating it if needed.
    std::shared_ptr<Aws::S3Crt::S3CrtClient>
        get_or_create_worker_client(const std::string& endpoint);

    std::atomic<bool> _stop;
    ClientConfiguration _client_config;

    // HTTP client used for the redirect probe only. Configured with
    // followRedirects=NEVER so we can read the 307 Location header.
    std::shared_ptr<Aws::Http::HttpClient> _probe_http;

    // worker endpoint (e.g. "http://10.0.0.5:29998") -> CRT client
    std::unordered_map<std::string, std::shared_ptr<Aws::S3Crt::S3CrtClient>> _worker_clients;

    // "bucket/path" -> CRT client (shortcut to the worker that owns this file)
    std::unordered_map<std::string, std::shared_ptr<Aws::S3Crt::S3CrtClient>> _file_routes;

    std::mutex _routing_mutex;

    // queue of asynchronous responses
    using Responder = common::SharedQueue<common::backend_api::Response>;
    std::shared_ptr<Responder> _responder;
};

}; //namespace runai::llm::streamer::impl::alluxio
