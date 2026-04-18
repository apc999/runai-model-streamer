load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreameralluxio.so",
    srcs = ["alluxio.h",
            "alluxio.cc",
    ],
    deps = [
        "//common/exception",
        "//common/range",
        "//common/client_mgr",
        "//utils/semver",
        "//alluxio/client",
        "//alluxio/alluxio_init",
    ],
    ldscript = ":alluxio.ldscript",
)
