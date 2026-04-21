#include "alluxio/alluxio.h"
#include "alluxio/alluxio_init/alluxio_init.h"
#include "alluxio/client/client.h"

#include "common/client_mgr/client_mgr.h"
#include "common/exception/exception.h"
#include "utils/env/env.h"
#include "utils/semver/semver.h"

// Alluxio backend: handles Alluxio S3 API with transparent 307 redirect
// following via per-file worker probing. See alluxio.h for details.

namespace runai::llm::streamer::impl::alluxio
{

inline constexpr char AlluxioClientName[] = "Alluxio";
using AlluxioClientMgr = common::ClientMgr<AlluxioClient, AlluxioClientName>;

// --- Backend API ---

const utils::Semver min_glibc_semver = utils::Semver(common::description(static_cast<int>(common::ResponseCode::GlibcPrerequisite)));
const size_t min_chunk_bytesize = 5 * 1024 * 1024;

common::backend_api::ResponseCode_t obj_open_backend(common::backend_api::ObjectBackendHandle_t* out_backend_handle)
{
    common::ResponseCode ret = common::ResponseCode::Success;

    try
    {
        auto glibc_version = utils::get_glibc_version();
        if (min_glibc_semver > glibc_version)
        {
            LOG(ERROR) << "GLIBC version must be at least " << min_glibc_semver << ", instead of " << glibc_version;
            return common::ResponseCode::GlibcPrerequisite;
        }

        size_t chunk_size;
        if (utils::try_getenv("RUNAI_STREAMER_CHUNK_BYTESIZE", chunk_size))
        {
            LOG_IF(INFO, (chunk_size < min_chunk_bytesize)) << "Minimal chunk size to read is 5 MiB";
        }

        // Warm up the AlluxioInit singleton. It owns both the AWS SDK
        // runtime (Aws::InitAPI/ShutdownAPI) and the process-wide CRT
        // client cache; the destructor tears them down in a deterministic
        // order so CRT client destructors never run post-ShutdownAPI.
        AlluxioInit::instance();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to init Alluxio backend";
        ret = common::ResponseCode::S3NotSupported;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_close_backend(common::backend_api::ObjectBackendHandle_t backend_handle)
{
    // Shutdown is deferred to process exit.
    return common::ResponseCode::Success;
}

common::backend_api::ObjectShutdownPolicy_t obj_get_backend_shutdown_policy()
{
    return common::backend_api::OBJECT_SHUTDOWN_POLICY_ON_PROCESS_EXIT;
}

// --- Client API ---

common::backend_api::ResponseCode_t obj_create_client(common::backend_api::ObjectBackendHandle_t backend_handle,
                                                       const common::backend_api::ObjectClientConfig_t* client_initial_config,
                                                       common::backend_api::ObjectClientHandle_t* out_client_handle)
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        *out_client_handle = AlluxioClientMgr::pop(*client_initial_config);
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to create Alluxio client";
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_remove_client(common::backend_api::ObjectClientHandle_t client_handle)
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        if (client_handle)
        {
           AlluxioClientMgr::push(static_cast<AlluxioClient *>(client_handle));
        }
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove Alluxio client";
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_remove_all_clients()
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        AlluxioClientMgr::clear();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove all Alluxio clients";
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_cancel_all_reads()
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        AlluxioClientMgr::stop();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to stop all Alluxio clients";
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_request_read(common::backend_api::ObjectClientHandle_t client_handle,
                                                     const char* path,
                                                     common::backend_api::ObjectRange_t range,
                                                     char* destination_buffer,
                                                     common::backend_api::ObjectRequestId_t request_id)
{
    try
    {
        if (!client_handle)
        {
            LOG(ERROR) << "Attempt to read with null alluxio client";
            return common::ResponseCode::UnknownError;
        }
        auto ptr = static_cast<AlluxioClient *>(client_handle);
        return ptr->async_read(path, range, destination_buffer, request_id);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while sending async request";
    }
    return common::ResponseCode::UnknownError;
}

common::backend_api::ResponseCode_t obj_wait_for_completions(common::backend_api::ObjectClientHandle_t client_handle,
                                                              common::backend_api::ObjectCompletionEvent_t* event_buffer,
                                                              unsigned int max_events_to_retrieve,
                                                              unsigned int* out_num_events_retrieved,
                                                              common::backend_api::ObjectWaitMode_t wait_mode)
{
    try
    {
        if (!client_handle)
        {
            LOG(ERROR) << "Attempt to get read response with null alluxio client";
            return common::ResponseCode::UnknownError;
        }
        if (max_events_to_retrieve == 0)
        {
            LOG(ERROR) << "Attempt to get read response with max_events_to_retrieve = 0";
            return common::ResponseCode::UnknownError;
        }
        if (!event_buffer || !out_num_events_retrieved)
        {
            LOG(ERROR) << "Attempt to get read response with null event_buffer or out_num_events_retrieved";
            return common::ResponseCode::UnknownError;
        }

        auto ptr = static_cast<AlluxioClient *>(client_handle);
        auto response = ptr->async_read_response();
        *out_num_events_retrieved = 1;
        event_buffer[0].request_id = response.handle;
        event_buffer[0].response_code = response.ret;
        return common::ResponseCode::Success;
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while sending async request";
    }
    return common::ResponseCode::UnknownError;
}

}; // namespace runai::llm::streamer::impl::alluxio
