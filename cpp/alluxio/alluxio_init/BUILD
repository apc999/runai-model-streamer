load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "alluxio_init",
    deps = [
        "@aws",
        "//utils/env",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "alluxio_init_test",
    srcs = ["alluxio_init_test.cc"],
    deps = [":alluxio_init"],
)
