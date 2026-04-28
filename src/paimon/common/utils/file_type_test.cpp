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

#include "paimon/common/utils/file_type.h"

#include "gtest/gtest.h"

namespace paimon::test {

TEST(FileTypeTest, TestIsIndex) {
    ASSERT_TRUE(FileTypeUtils::IsIndex(FileType::kBucketIndex));
    ASSERT_TRUE(FileTypeUtils::IsIndex(FileType::kGlobalIndex));
    ASSERT_TRUE(FileTypeUtils::IsIndex(FileType::kFileIndex));
    ASSERT_FALSE(FileTypeUtils::IsIndex(FileType::kMeta));
    ASSERT_FALSE(FileTypeUtils::IsIndex(FileType::kData));
}

TEST(FileTypeTest, TestMetaPrefix) {
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/snapshot/snapshot-1"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/schema/schema-2"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/statistics/stat-a1b2c3d4-0"),
              FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/tag/tag-3"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/consumer/consumer-myGroup"),
              FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/service/service-4"), FileType::kMeta);
}

TEST(FileTypeTest, TestIndexTypes) {
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/index/bitmap.index"), FileType::kFileIndex);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/index/btree-global-index-a1b2.index"),
              FileType::kGlobalIndex);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/index/bitmap-global-index-1.index"),
              FileType::kGlobalIndex);
    ASSERT_EQ(
        FileTypeUtils::Classify("dfs://cluster/db/index/lumina-vector-ann-global-index-a1b2.index"),
        FileType::kGlobalIndex);
    ASSERT_EQ(
        FileTypeUtils::Classify("dfs://cluster/db/index/tantivy-fulltext-global-index-a1b2.index"),
        FileType::kGlobalIndex);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/p=1/bucket-0/index-abcdef-1"),
              FileType::kBucketIndex);
}

TEST(FileTypeTest, TestMetaSpecialNames) {
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/manifest/manifest-a1b2c3d4-0"),
              FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/manifest/manifest-list-1"),
              FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/manifest/index-manifest-a1b2c3d4-0"),
              FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/_SUCCESS"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/part-1_SUCCESS"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/hint/EARLIEST"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/hint/LATEST"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/changelog/changelog-123"), FileType::kMeta);
}

TEST(FileTypeTest, TestDefaultData) {
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/p=1/bucket-0/data-1.orc"), FileType::kData);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/p=1/bucket-0/data-1.parquet"),
              FileType::kData);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/p=1/bucket-0/changelog-a1b2c3d4-0.orc"),
              FileType::kData);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/p=1/bucket-0/data-a1b2c3d4-0.blob"),
              FileType::kData);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/p=1/bucket-0/data-a1b2c3d4-0.vector.lance"),
              FileType::kData);
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/random/unknown.bin"), FileType::kData);
}

TEST(FileTypeTest, TestChangelogInAncestorPathNotMisclassified) {
    const std::string root = "hdfs://cluster/changelog/warehouse/db.db/table";
    ASSERT_EQ(FileTypeUtils::Classify(root + "/dt=2024-01-01/bucket-0/data-a1b2c3d4-0.orc"),
              FileType::kData);
    ASSERT_EQ(FileTypeUtils::Classify(root + "/dt=2024-01-01/bucket-0/changelog-a1b2c3d4-0.orc"),
              FileType::kData);
}

TEST(FileTypeTest, TestBranchPaths) {
    const std::string branch_root = "hdfs://cluster/warehouse/db.db/table/branch/branch-dev";
    ASSERT_EQ(FileTypeUtils::Classify(branch_root + "/snapshot/snapshot-1"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify(branch_root + "/schema/schema-0"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify(branch_root + "/changelog/changelog-1"), FileType::kMeta);
    ASSERT_EQ(FileTypeUtils::Classify(branch_root + "/index/index-a1b2c3d4-0"),
              FileType::kBucketIndex);
    ASSERT_EQ(FileTypeUtils::Classify(branch_root + "/index/btree-global-index-a1b2c3d4.index"),
              FileType::kGlobalIndex);
}

TEST(FileTypeTest, TestTempWrappedName) {
    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/snapshot/.snapshot-1.12345678-1234-1234-1234-123456789abc.tmp"),
              FileType::kMeta);

    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/schema/.schema-0.12345678-1234-1234-1234-123456789abc.tmp"),
              FileType::kMeta);

    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/tag/.tag-myTag.12345678-1234-1234-1234-123456789abc.tmp"),
              FileType::kMeta);

    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/consumer/.consumer-myGroup.12345678-1234-1234-1234-"
                  "123456789abc.tmp"),
              FileType::kMeta);

    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/service/.service-primary-key-lookup.12345678-1234-1234-1234-"
                  "123456789abc.tmp"),
              FileType::kMeta);

    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/snapshot/.EARLIEST.12345678-1234-1234-1234-123456789abc.tmp"),
              FileType::kMeta);

    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/snapshot/.LATEST.12345678-1234-1234-1234-123456789abc.tmp"),
              FileType::kMeta);

    ASSERT_EQ(
        FileTypeUtils::Classify(
            "dfs://cluster/db/p=1/bucket-0/._SUCCESS.12345678-1234-1234-1234-123456789abc.tmp"),
        FileType::kMeta);

    ASSERT_EQ(
        FileTypeUtils::Classify(
            "dfs://cluster/db/changelog/.changelog-1.12345678-1234-1234-1234-123456789abc.tmp"),
        FileType::kMeta);

    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/statistics/.stat-a1b2c3d4-0.12345678-1234-1234-1234-"
                  "123456789abc.tmp"),
              FileType::kMeta);

    ASSERT_EQ(
        FileTypeUtils::Classify(
            "dfs://cluster/db/p=1/bucket-0/.data-1.orc.12345678-1234-1234-1234-123456789abc.tmp"),
        FileType::kData);

    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/index/.bitmap.index.12345678-1234-1234-1234-123456789abc.tmp"),
              FileType::kFileIndex);

    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/index/.bitmap-global-index-1.index.12345678-1234-1234-1234-"
                  "123456789abc.tmp"),
              FileType::kGlobalIndex);
}

TEST(FileTypeTest, TestInvalidTempWrapperFallsBackToOriginalName) {
    // No leading dot -> should not unwrap.
    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/snapshot/snapshot-1.12345678-1234-1234-1234-123456789abc.tmp"),
              FileType::kMeta);

    // No .tmp suffix -> should not unwrap.
    ASSERT_EQ(FileTypeUtils::Classify(
                  "dfs://cluster/db/snapshot/.snapshot-1.12345678-1234-1234-1234-123456789abc"),
              FileType::kData);

    // Too short -> should not unwrap.
    ASSERT_EQ(FileTypeUtils::Classify("dfs://cluster/db/snapshot/.x.tmp"), FileType::kData);
}

}  // namespace paimon::test
