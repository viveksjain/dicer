# Bazel module extension to download and provide the etcd binary for integration tests.

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Matches the version we use internally, as of 2025.
ETCD_VERSION = "3.6.4"

# Update when updating the version.
ETCD_RELEASE_SHA256 = "4d5f3101daa534e45ccaf3eec8d21c19b7222db377bcfd5e5a9144155238c105"

def _etcd_impl(_ctx):
    """Implements the etcd module extension.

    Downloads the etcd release from GitHub and makes the binary available as target
    `@etcd_release//:etcd_binary`.
    """
    http_archive(
        name = "etcd_release",
        url = "https://github.com/etcd-io/etcd/releases/download/v{version}/etcd-v{version}-linux-amd64.tar.gz".format(version = ETCD_VERSION),
        strip_prefix = "etcd-v{version}-linux-amd64".format(version = ETCD_VERSION),
        build_file_content = """
filegroup(
    name = "etcd_binary",
    srcs = ["etcd"],
    visibility = ["//visibility:public"],
)
""",
        sha256 = ETCD_RELEASE_SHA256,
    )

etcd_extension = module_extension(
    implementation = _etcd_impl,
)
