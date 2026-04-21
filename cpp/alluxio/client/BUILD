load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "client",
    srcs = ["client.cc"],
    hdrs = ["client.h"],
    deps = [
        "//alluxio/alluxio_init",
        "//alluxio/client_configuration",
        "//common/backend_api/response",
        "//common/response_code",
        "//common/shared_queue",
        "//common/range",
        "//common/s3_wrapper",
        "//common/exception",
        "//common/storage_uri",
        "//common/client_mgr",
        "//common/backend_api/object_storage",
        "//utils/env",
        "//utils/fd",
    ],
)

runai_cc_test(
    name = "responder_race_test",
    srcs = ["responder_race_test.cc"],
    deps = [
        ":client",
        "//alluxio/alluxio_init",
    ],
)
