/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0.
 *
 * tantivy-fulltext Stage 1 smoke test: prove the Rust FFI bridge is callable from C++.
 * Intentionally minimal — exercises only paimon_tantivy_version().
 * Later stages add real functional tests.
 */

#include <cstring>
#include <string>

#include "gtest/gtest.h"

extern "C" {
#include "paimon_tantivy_ffi.h"
}

namespace paimon::tantivy {

TEST(TantivySmoke, VersionIsReachable) {
    const char* version = paimon_tantivy_version();
    ASSERT_NE(version, nullptr) << "paimon_tantivy_version returned null";

    const std::string v(version);
    EXPECT_FALSE(v.empty());
    // build.rs pins version from Cargo.toml (CARGO_PKG_VERSION), semver "x.y.z"
    EXPECT_NE(v.find('.'), std::string::npos)
        << "expected semver, got: " << v;
}

TEST(TantivySmoke, VersionPointerIsStable) {
    // The pointer is documented as 'static — two calls should return either
    // the same pointer or at least equivalent string content.
    const char* v1 = paimon_tantivy_version();
    const char* v2 = paimon_tantivy_version();
    ASSERT_NE(v1, nullptr);
    ASSERT_NE(v2, nullptr);
    EXPECT_EQ(std::strcmp(v1, v2), 0);
}

}  // namespace paimon::tantivy
