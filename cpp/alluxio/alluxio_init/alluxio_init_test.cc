#include "alluxio/alluxio_init/alluxio_init.h"

#include <gtest/gtest.h>

#include <memory>

namespace runai::llm::streamer::impl::alluxio
{

TEST(Creation, Sanity)
{
    std::unique_ptr<AlluxioInit> init;
    EXPECT_NO_THROW(init = std::make_unique<AlluxioInit>());
}

}; // namespace runai::llm::streamer::impl::alluxio
