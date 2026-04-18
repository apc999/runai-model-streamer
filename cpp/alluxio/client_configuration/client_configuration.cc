#include "alluxio/client_configuration/client_configuration.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::alluxio
{

ClientConfiguration::ClientConfiguration()
{
    unsigned long max_connections = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_MAX_CONNECTIONS", 0);
    if (max_connections)
    {
        config.maxConnections = max_connections;
    }

    unsigned long target_gbps = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_TARGET_GBPS", 0);
    if (target_gbps)
    {
        LOG(DEBUG) << "Alluxio target throughput is set to " << target_gbps << " Gbps";
        config.throughputTargetGbps = target_gbps;
    }

    const auto request_timeout_ms = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS", 1000);
    if (request_timeout_ms)
    {
        LOG(DEBUG) << "Alluxio request timeout is set to " << request_timeout_ms << " ms";
        config.requestTimeoutMs = request_timeout_ms;
    }

    const auto low_speed_limit = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_LOW_SPEED_LIMIT", 0);
    if (low_speed_limit)
    {
        LOG(DEBUG) << "Alluxio minimum speed is set to " << low_speed_limit << " bytes in second";
        config.lowSpeedLimit = low_speed_limit;
    }
}

}; // namespace runai::llm::streamer::impl::alluxio
