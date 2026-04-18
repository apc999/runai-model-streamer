#pragma once

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>

namespace runai::llm::streamer::impl::alluxio
{

struct AlluxioInit
{
    AlluxioInit();
    ~AlluxioInit();

    Aws::SDKOptions options;
};

}; //namespace runai::llm::streamer::impl::alluxio
