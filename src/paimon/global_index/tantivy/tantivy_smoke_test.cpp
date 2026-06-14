/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * tantivy-fulltext smoke test: prove the Rust FFI bridge is callable from C++.
 * Intentionally minimal — exercises only paimon_tantivy_version().
 * Later stages add real functional tests.
 */

#include <cstring>
#include <string>

#include "gtest/gtest.h"

extern "C" {
#include "paimon_tantivy_ffi.h"  // NOLINT(build/include_subdir)
}

namespace paimon::tantivy {

TEST(TantivySmoke, VersionIsReachable) {
    const char* version = paimon_tantivy_version();
    ASSERT_NE(version, nullptr) << "paimon_tantivy_version returned null";

    const std::string v(version);
    ASSERT_FALSE(v.empty());
    // build.rs pins version from Cargo.toml (CARGO_PKG_VERSION), semver "x.y.z"
    ASSERT_NE(v.find('.'), std::string::npos) << "expected semver, got: " << v;
}

TEST(TantivySmoke, VersionPointerIsStable) {
    // The pointer is documented as 'static — two calls should return either
    // the same pointer or at least equivalent string content.
    const char* v1 = paimon_tantivy_version();
    const char* v2 = paimon_tantivy_version();
    ASSERT_NE(v1, nullptr);
    ASSERT_NE(v2, nullptr);
    ASSERT_EQ(std::strcmp(v1, v2), 0);
}

}  // namespace paimon::tantivy
