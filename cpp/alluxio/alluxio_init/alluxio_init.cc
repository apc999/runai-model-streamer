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
        Aws::ShutdownAPI(options);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while shutting down";
    }
}

}; // namespace runai::llm::streamer::impl::alluxio
