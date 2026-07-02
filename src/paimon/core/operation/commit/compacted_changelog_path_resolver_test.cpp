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
 */

#include "paimon/core/operation/commit/compacted_changelog_path_resolver.h"

#include <string>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(CompactedChangelogPathResolverTest, IsCompactedChangelogPath) {
    std::string regular_changelog =
        "table/bucket-0/changelog-25b05ab0-6f90-4865-a984-8d9629bac735-1426.parquet";
    std::string compacted_changelog =
        "table/bucket-0/"
        "compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351$0-39253.cc-parquet";
    std::string data_file = "table/bucket-0/data-file-1.parquet";

    ASSERT_FALSE(CompactedChangelogPathResolver::IsCompactedChangelogPath(regular_changelog));
    ASSERT_TRUE(CompactedChangelogPathResolver::IsCompactedChangelogPath(compacted_changelog));
    ASSERT_FALSE(CompactedChangelogPathResolver::IsCompactedChangelogPath(data_file));
}

TEST(CompactedChangelogPathResolverTest, ResolveNonCompactedChangelogPath) {
    std::string regular_changelog =
        "table/bucket-0/changelog-25b05ab0-6f90-4865-a984-8d9629bac735-1426.parquet";

    ASSERT_EQ(regular_changelog, CompactedChangelogPathResolver::Resolve(regular_changelog));
}

TEST(CompactedChangelogPathResolverTest, ResolveFakeCompactedChangelogPath) {
    std::string fake_path =
        "table/f1=10/bucket-1/compacted-changelog-abc$0-39253-39253-35699.cc-parquet";
    std::string expected_real_path =
        "table/f1=10/bucket-0/compacted-changelog-abc$0-39253.cc-parquet";

    ASSERT_EQ(expected_real_path, CompactedChangelogPathResolver::Resolve(fake_path));
}

TEST(CompactedChangelogPathResolverTest, KeepRealCompactedChangelogPath) {
    std::string real_path = "table/f1=10/bucket-1/compacted-changelog-abc$1-39253.cc-parquet";

    ASSERT_EQ(real_path, CompactedChangelogPathResolver::Resolve(real_path));
}

TEST(CompactedChangelogPathResolverTest, KeepNonCompactedPath) {
    std::string normal_path = "table/f1=10/bucket-1/data-file.orc";

    ASSERT_EQ(normal_path, CompactedChangelogPathResolver::Resolve(normal_path));
}

TEST(CompactedChangelogPathResolverTest, KeepInvalidCompactedPath) {
    std::string invalid_path = "table/f1=10/bucket-1/compacted-changelog-abc$1.cc-parquet";

    ASSERT_EQ(invalid_path, CompactedChangelogPathResolver::Resolve(invalid_path));
}

TEST(CompactedChangelogPathResolverTest, ResolveWithDifferentFormats) {
    std::string fake_orc_path =
        "table/f1=10/bucket-2/compacted-changelog-abc$0-1024-1024-512.cc-orc";
    std::string expected_orc_path = "table/f1=10/bucket-0/compacted-changelog-abc$0-1024.cc-orc";
    ASSERT_EQ(expected_orc_path, CompactedChangelogPathResolver::Resolve(fake_orc_path));

    std::string fake_avro_path =
        "table/f1=10/bucket-5/compacted-changelog-abc$2-2048-2048-1024.cc-avro";
    std::string expected_avro_path = "table/f1=10/bucket-2/compacted-changelog-abc$2-2048.cc-avro";
    ASSERT_EQ(expected_avro_path, CompactedChangelogPathResolver::Resolve(fake_avro_path));
}

TEST(CompactedChangelogPathResolverTest, ResolveFileWithoutExtension) {
    std::string file_without_extension = "table/f1=10/bucket-0/file";

    ASSERT_EQ(file_without_extension,
              CompactedChangelogPathResolver::Resolve(file_without_extension));
}

}  // namespace paimon::test
