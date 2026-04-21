#include <memory>

#include "alluxio/alluxio_init/alluxio_init.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::alluxio
{

AlluxioInit::AlluxioInit()
{
    options.httpOptions.installSigPipeHandler = true;
    auto trace_aws = utils::getenv<bool>("RUNAI_STREAMER_ALLUXIO_TRACE", false);
    if (trace_aws)
    {
        options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
    }

    Aws::InitAPI(options);
}

AlluxioInit::~AlluxioInit()
{
    LOG(DEBUG) << "Shutting down alluxio backend";
    try
    {
        // Destroy cached S3CrtClient instances BEFORE tearing down the
        // SDK runtime. Each S3CrtClient's destructor joins its thread
        // pool, which touches aws-c-* state; running that after
        // Aws::ShutdownAPI hangs on pthread_join waiting for threads
        // the runtime can no longer schedule.
        {
            std::lock_guard<std::mutex> g(_worker_clients_mutex);
            _worker_clients.clear();
        }
        Aws::ShutdownAPI(options);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while shutting down";
    }
}

AlluxioInit& AlluxioInit::instance()
{
    // Function-local static: constructed on first call, destroyed at
    // program exit. Destruction order is deterministic WITHIN this
    // object (see ~AlluxioInit above); that's what matters.
    static AlluxioInit single;
    return single;
}

std::shared_ptr<Aws::S3Crt::S3CrtClient>
AlluxioInit::get_or_create_worker_client(
    const std::string& endpoint,
    const Aws::S3Crt::ClientConfiguration& base_cfg,
    const Aws::Auth::AWSCredentials* creds)
{
    std::lock_guard<std::mutex> g(_worker_clients_mutex);
    auto it = _worker_clients.find(endpoint);
    if (it != _worker_clients.end()) return it->second;

    Aws::S3Crt::ClientConfiguration cfg = base_cfg;
    cfg.endpointOverride = endpoint;

    std::shared_ptr<Aws::S3Crt::S3CrtClient> client;
    if (creds == nullptr)
    {
        client = std::make_shared<Aws::S3Crt::S3CrtClient>(cfg);
    }
    else
    {
        client = std::make_shared<Aws::S3Crt::S3CrtClient>(*creds, cfg);
    }
    LOG(DEBUG) << "Alluxio worker CRT client created for " << endpoint;
    _worker_clients.emplace(endpoint, client);
    return client;
}

}; // namespace runai::llm::streamer::impl::alluxio
