#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

#include <cstring>
#include <algorithm>
#include <string>
#include <utility>
#include <optional>

#include "common/backend_api/object_storage/object_storage.h"
#include "alluxio/client/client.h"

#include "common/exception/exception.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"
#include "utils/fd/fd.h"

namespace runai::llm::streamer::impl::alluxio
{

namespace
{

std::optional<Aws::String> convert(const char * input)
{
    std::optional<Aws::String> result = std::nullopt;
    if (input)
    {
        result = Aws::String(input);
    }
    return result;
}

// Extract "scheme://host:port" from a URL (typically the 307 Location header).
// Returns empty string on parse failure.
std::string parse_endpoint(const std::string& url)
{
    auto scheme_end = url.find("://");
    if (scheme_end == std::string::npos) return "";

    auto host_start = scheme_end + 3;
    auto path_start = url.find('/', host_start);
    std::string host_port = (path_start == std::string::npos)
        ? url.substr(host_start)
        : url.substr(host_start, path_start - host_start);

    return url.substr(0, scheme_end + 3) + host_port;
}

} // anonymous namespace

AlluxioClientBase::AlluxioClientBase(const common::backend_api::ObjectClientConfig_t & config) :
    _endpoint(convert(config.endpoint_url)),
    _chunk_bytesize(config.default_storage_chunk_size)
{
    auto ptr = config.initial_params;
    if (ptr)
    {
        for (size_t i = 0; i < config.num_initial_params; ++i, ++ptr)
        {
            const char* key = ptr->key;
            const char* value = ptr->value;
            if (strcmp(key, common::s3::Credentials::ACCESS_KEY_ID_KEY) == 0)
            {
                _key = convert(value);
            }
            else if (strcmp(key, common::s3::Credentials::SECRET_ACCESS_KEY_KEY) == 0)
            {
                _secret = convert(value);
            }
            else if (strcmp(key, common::s3::Credentials::SESSION_TOKEN_KEY) == 0)
            {
                _token = convert(value);
            }
            else if (strcmp(key, common::s3::Credentials::REGION_KEY) == 0)
            {
                _region = convert(value);
            }
            else
            {
                LOG(WARNING) << "Unknown initial parameter: " << key;
            }
        }
    }
}

bool AlluxioClientBase::verify_credentials_member(const std::optional<Aws::String>& member, const std::optional<Aws::String>& value, const char * name) const
{
    if (member.has_value())
    {
        if (!value.has_value())
        {
            LOG(DEBUG) << "credentials member " << name << " is set, but provided member is nullptr";
            return false;
        }
        if (member.value() != value.value())
        {
            LOG(DEBUG) << "credentials member " << name << " doesn't match the provided value";
            return false;
        }
    }
    else if (value.has_value())
    {
        LOG(DEBUG) << "credentials member " << name << " is not set, but provided member is not nullptr";
        return false;
    }
    LOG(DEBUG) << "credentials member " << name << " verified";
    return true;
}

bool AlluxioClientBase::verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const
{
    AlluxioClientBase other(config);
    return (verify_credentials_member(_key, other._key, "access key") &&
            verify_credentials_member(_secret, other._secret, "secret") &&
            verify_credentials_member(_token, other._token, "session token") &&
            verify_credentials_member(_region, other._region, "region") &&
            verify_credentials_member(_endpoint, other._endpoint, "endpoint"));
}

AlluxioClient::AlluxioClient(const common::backend_api::ObjectClientConfig_t & config) :
    AlluxioClientBase(config),
    _stop(false),
    _responder(nullptr)
{
    if (_endpoint.has_value())
    {
        _client_config.config.endpointOverride = _endpoint.value();
    }

    if (utils::try_getenv("RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING", _client_config.config.useVirtualAddressing))
    {
        LOG(DEBUG) << "Setting alluxio configuration useVirtualAddressing to " << _client_config.config.useVirtualAddressing;
    }

    if (_region.has_value())
    {
        LOG(DEBUG) << "Setting alluxio region to " << _region.value();
        _client_config.config.region = _region.value();
    }

    if (utils::try_getenv("AWS_CA_BUNDLE", _client_config.config.caFile))
    {
        LOG(DEBUG) << "Setting alluxio configuration ca certificate file to " << _client_config.config.caFile;

        if (!utils::Fd::exists(_client_config.config.caFile))
        {
            LOG(ERROR) << "CA cert file not found: " << _client_config.config.caFile;
            throw common::Exception(common::ResponseCode::CaFileNotFound);
        }
    }

    // Probe HTTP client: plain non-CRT HTTP client. Its sole job is to send a
    // 1-byte Range GET and read the 307 Location header. followRedirects MUST
    // be NEVER — otherwise we never see the 307 response.
    Aws::Client::ClientConfiguration probe_cfg;
    probe_cfg.followRedirects = Aws::Client::FollowRedirectsPolicy::NEVER;
    probe_cfg.requestTimeoutMs = static_cast<long>(
        utils::getenv<unsigned long>("RUNAI_STREAMER_ALLUXIO_PROBE_TIMEOUT_MS", 5000));
    probe_cfg.connectTimeoutMs = static_cast<long>(
        utils::getenv<unsigned long>("RUNAI_STREAMER_ALLUXIO_PROBE_CONNECT_TIMEOUT_MS", 2000));
    probe_cfg.maxConnections = 2;
    probe_cfg.verifySSL = _client_config.config.verifySSL;
    probe_cfg.caFile = _client_config.config.caFile;
    _probe_http = Aws::Http::CreateHttpClient(probe_cfg);
    LOG(DEBUG) << "Alluxio probe HTTP client created";
}

std::shared_ptr<Aws::S3Crt::S3CrtClient>
AlluxioClient::get_or_create_worker_client(const std::string& endpoint)
{
    // Caller holds _routing_mutex; do not lock here.
    auto it = _worker_clients.find(endpoint);
    if (it != _worker_clients.end()) return it->second;

    Aws::S3Crt::ClientConfiguration cfg = _client_config.config;
    cfg.endpointOverride = endpoint;

    std::shared_ptr<Aws::S3Crt::S3CrtClient> client;
    if (_client_credentials == nullptr)
    {
        client = std::make_shared<Aws::S3Crt::S3CrtClient>(cfg);
    }
    else
    {
        client = std::make_shared<Aws::S3Crt::S3CrtClient>(*_client_credentials, cfg);
    }
    LOG(DEBUG) << "Alluxio worker CRT client created for " << endpoint;
    _worker_clients.emplace(endpoint, client);
    return client;
}

std::shared_ptr<Aws::S3Crt::S3CrtClient>
AlluxioClient::resolve_worker_client(const common::s3::StorageUri& uri)
{
    const std::string key = std::string(uri.bucket) + "/" + std::string(uri.path);

    // Fast path: cache hit.
    {
        std::lock_guard<std::mutex> g(_routing_mutex);
        auto it = _file_routes.find(key);
        if (it != _file_routes.end()) return it->second;
    }

    // Slow path: probe the Gateway for this file's owner worker.
    if (!_endpoint.has_value())
    {
        LOG(ERROR) << "Alluxio backend requires an endpoint URL (no Gateway to probe)";
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }

    std::string probe_url = _endpoint.value() + "/" + key;
    LOG(SPAM) << "Alluxio probing " << probe_url;

    auto req = Aws::Http::CreateHttpRequest(
        Aws::String(probe_url.c_str()),
        Aws::Http::HttpMethod::HTTP_GET,
        Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    req->SetHeaderValue("Range", "bytes=0-0");

    auto resp = _probe_http->MakeRequest(req);
    if (!resp)
    {
        LOG(ERROR) << "Alluxio probe returned null response for " << probe_url;
        throw common::Exception(common::ResponseCode::FileAccessError);
    }

    const auto code = static_cast<int>(resp->GetResponseCode());
    std::string target_endpoint;

    if (code == 301 || code == 302 || code == 307 || code == 308)
    {
        if (!resp->HasHeader("Location"))
        {
            LOG(ERROR) << "Alluxio probe got " << code << " without Location header for " << probe_url;
            throw common::Exception(common::ResponseCode::FileAccessError);
        }
        const std::string loc(resp->GetHeader("Location").c_str());
        target_endpoint = parse_endpoint(loc);
        if (target_endpoint.empty())
        {
            LOG(ERROR) << "Alluxio probe: failed to parse Location '" << loc << "'";
            throw common::Exception(common::ResponseCode::FileAccessError);
        }
        LOG(DEBUG) << "Alluxio " << key << " -> " << target_endpoint << " (via " << code << ")";
    }
    else if (code == 200 || code == 206)
    {
        // No redirect: the Gateway is the owner. Single-worker / local-hit case.
        target_endpoint = _endpoint.value();
        LOG(DEBUG) << "Alluxio " << key << " owned by Gateway (code " << code << ")";
    }
    else
    {
        LOG(ERROR) << "Alluxio probe for " << probe_url << " returned unexpected code " << code;
        throw common::Exception(common::ResponseCode::FileAccessError);
    }

    // Cache result. Double-check in case another thread beat us to it.
    std::lock_guard<std::mutex> g(_routing_mutex);
    auto it = _file_routes.find(key);
    if (it != _file_routes.end()) return it->second;
    auto worker_client = get_or_create_worker_client(target_endpoint);
    _file_routes.emplace(key, worker_client);
    return worker_client;
}

common::backend_api::Response AlluxioClient::async_read_response()
{
    if (_responder == nullptr)
    {
        LOG(WARNING) << "Requesting response with uninitialized responder";
        return common::ResponseCode::FinishedError;
    }

    return _responder->pop();
}

common::backend_api::ResponseCode_t AlluxioClient::async_read(const char* path,
                                                              common::backend_api::ObjectRange_t range,
                                                              char* destination_buffer,
                                                              common::backend_api::ObjectRequestId_t request_id)
{
    if (_responder == nullptr)
    {
        _responder = std::make_shared<Responder>(1);
    }
    else
    {
        _responder->increment(1);
    }

    const auto uri = common::s3::StorageUri(path);

    // Resolve (or look up cached) owner worker for this file. First call for
    // each file blocks on a ~1ms in-cluster probe RTT; subsequent calls for the
    // same file are O(1). Different files may route to different worker CRT
    // clients, running concurrent multi-part GETs in parallel.
    std::shared_ptr<Aws::S3Crt::S3CrtClient> worker_client;
    try
    {
        worker_client = resolve_worker_client(uri);
    }
    catch (const std::exception& e)
    {
        LOG(ERROR) << "Failed to resolve worker for " << path << ": " << e.what();
        common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
        _responder->push(std::move(r));
        return common::ResponseCode::FileAccessError;
    }

    Aws::String bucket_name(uri.bucket);
    Aws::String path_name(uri.path);

    char * buffer_ = destination_buffer;
    size_t size = std::max(1UL, range.length/_chunk_bytesize);
    LOG(SPAM) <<"Number of chunks is " << size;

    auto counter = std::make_shared< std::atomic<unsigned> >(size);
    auto is_success = std::make_shared< std::atomic<bool> >(true);

    size_t total_ = range.length;
    size_t offset_ = range.offset;
    for (unsigned i = 0; i < size && !_stop; ++i)
    {
        size_t bytesize_ = (i == size - 1 ? total_ : _chunk_bytesize);

        auto request = std::make_shared<Aws::S3Crt::Model::GetObjectRequest>();

        request->SetBucket(bucket_name);
        request->SetKey(path_name);
        std::string range_str = "bytes=" + std::to_string(offset_) + "-" + std::to_string(offset_ + bytesize_ - 1);
        request->SetRange(range_str.c_str());

        request->SetResponseStreamFactory(
            [buffer_, bytesize_]()
            {
                std::unique_ptr<Aws::StringStream>
                        stream(Aws::New<Aws::StringStream>("RunaiBuffer"));

                stream->rdbuf()->pubsetbuf(buffer_, bytesize_);

                return stream.release();
            });

        worker_client->GetObjectAsync(*request, [request, responder = _responder, request_id, counter, is_success](const Aws::S3Crt::S3CrtClient*, const Aws::S3Crt::Model::GetObjectRequest&,
                                                                        const Aws::S3Crt::Model::GetObjectOutcome& outcome,
                                                                        const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
            if (outcome.IsSuccess())
            {
                const auto running = counter->fetch_sub(1);
                LOG(SPAM) << "Async read request " << request_id << " succeeded - " << running << " running";
                if (running == 1)
                {
                    common::backend_api::Response r(request_id, common::ResponseCode::Success);
                    responder->push(std::move(r));
                }
            }
            else
            {
                bool previous = is_success->exchange(false);
                if (previous)
                {
                    const auto & err = outcome.GetError();
                    LOG(ERROR) << "Failed to download alluxio object of request " << request_id << " " << err.GetExceptionName() << ": " << err.GetMessage();
                    common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                    responder->push(std::move(r));
                }
            }
        });

        total_ -= bytesize_;
        offset_ += bytesize_;
        buffer_ += bytesize_;
    }

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}

void AlluxioClient::stop()
{
    _stop = true;
    if (_responder != nullptr)
    {
        _responder->stop();
    }
}

}; // namespace runai::llm::streamer::impl::alluxio
