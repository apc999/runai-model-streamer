load("//:rules.bzl", "runai_cc_binary")

# Standalone reproducer for the S3CrtClient cache shutdown ordering bug.
# See repro_shutdown_hang.cc for the full explanation.
#
# Usage:
#   bazel run //alluxio/repro:repro_shutdown_hang -- --n=6 --mode=static
#   bazel run //alluxio/repro:repro_shutdown_hang -- --n=6 --mode=ordered
#
# Expected:
#   --mode=static   at n=6 → hang; watchdog aborts with exit 124
#   --mode=ordered  at any n → clean exit 0, fast
runai_cc_binary(
    name = "repro_shutdown_hang",
    srcs = ["repro_shutdown_hang.cc"],
    deps = [
        "@aws",
    ],
)
