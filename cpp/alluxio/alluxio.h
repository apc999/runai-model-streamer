#pragma once

#include "common/backend_api/object_storage/object_storage.h"
#include "common/response/response.h"
#include "common/range/range.h"

// Alluxio S3 API backend.
//
// Handles HTTP 307 redirects that Alluxio workers return when a file is owned
// by a different worker in a multi-worker deployment. The AWS CRT C++ client
// does not follow HTTP redirects, so this backend probes each file once via a
// 1-byte Range GET to discover the owner worker, then routes all subsequent
// chunk reads for that file directly to the owner worker's S3 endpoint.
//
// URI format:  alluxio://bucket/path
// Endpoint:    AWS_ENDPOINT_URL must point at any Alluxio worker's S3 API
//              (typically the local worker). The backend follows 307 redirects
//              transparently to route to the actual owner worker.
//
// Required env:
//   AWS_ENDPOINT_URL                              — Alluxio worker S3 endpoint
//   RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=0    — Alluxio uses path-style URLs
//   AWS_EC2_METADATA_DISABLED=true                — avoid 5s delay, see aws-sdk-cpp#1410
//
// Optional env:
//   RUNAI_STREAMER_ALLUXIO_PROBE_TIMEOUT_MS=5000
//   RUNAI_STREAMER_ALLUXIO_PROBE_CONNECT_TIMEOUT_MS=2000

namespace runai::llm::streamer::impl::alluxio
{

// --- Backend API ---

extern "C" common::backend_api::ResponseCode_t obj_open_backend(common::backend_api::ObjectBackendHandle_t* out_backend_handle);
extern "C" common::backend_api::ResponseCode_t obj_close_backend(common::backend_api::ObjectBackendHandle_t backend_handle);
extern "C" common::backend_api::ObjectShutdownPolicy_t obj_get_backend_shutdown_policy();

// --- Client API ---

extern "C" common::backend_api::ResponseCode_t obj_create_client(
    common::backend_api::ObjectBackendHandle_t backend_handle,
    const common::backend_api::ObjectClientConfig_t* client_initial_config,
    common::backend_api::ObjectClientHandle_t* out_client_handle
);

extern "C" common::backend_api::ResponseCode_t obj_remove_client(
    common::backend_api::ObjectClientHandle_t client_handle
);

extern "C" common::backend_api::ResponseCode_t obj_request_read(
    common::backend_api::ObjectClientHandle_t client_handle,
    const char* path,
    common::backend_api::ObjectRange_t range,
    char* destination_buffer,
    common::backend_api::ObjectRequestId_t request_id
);

extern "C" common::backend_api::ResponseCode_t obj_wait_for_completions(common::backend_api::ObjectClientHandle_t client_handle,
                                                                        common::backend_api::ObjectCompletionEvent_t* event_buffer,
                                                                        unsigned int max_events_to_retrieve,
                                                                        unsigned int* out_num_events_retrieved,
                                                                        common::backend_api::ObjectWaitMode_t wait_mode);

// Stops the responder of each client
extern "C" common::backend_api::ResponseCode_t obj_cancel_all_reads();

// Release clients
extern "C" common::backend_api::ResponseCode_t obj_remove_all_clients();

}; //namespace runai::llm::streamer::impl::alluxio
