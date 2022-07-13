workspace(name = "build_buildfarm")

load(":deps.bzl", "buildfarm_dependencies")

buildfarm_dependencies()

load(":defs.bzl", "buildfarm_init")

buildfarm_init()

load("@maven//:compat.bzl", "compat_repositories")

compat_repositories()

load(":images.bzl", "buildfarm_images")

buildfarm_images()

#load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

#http_archive(
#    name = "rules_pkg",
#    urls = [
##        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
#        "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
#    ],
#    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
#)
#load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")
#rules_pkg_dependencies()
