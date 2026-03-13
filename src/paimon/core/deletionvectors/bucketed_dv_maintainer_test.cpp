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

#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"

#include <map>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

std::shared_ptr<DeletionVectorsIndexFile> CreateDvIndexFile(const std::string& root_path) {
    auto memory_pool = GetDefaultPool();
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", root_path, {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(root_path);
    return std::make_shared<DeletionVectorsIndexFile>(fs, path_factory,
                                                      /*target_size_per_index_file=*/1024 * 1024,
                                                      /*bitmap64=*/false, memory_pool);
}

std::shared_ptr<DeletionVector> CreateDeletionVector(int32_t begin, int32_t end) {
    RoaringBitmap32 bitmap;
    for (int32_t value = begin; value < end; ++value) {
        bitmap.Add(value);
    }
    return std::make_shared<BitmapDeletionVector>(bitmap);
}

}  // namespace

TEST(BucketedDvMaintainerTest, TestDeletionVectorLookupAndIndexFileGetter) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto index_file = CreateDvIndexFile(dir->Str());
    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors = {
        {"file-a", CreateDeletionVector(0, 3)}, {"file-b", CreateDeletionVector(10, 12)}};

    BucketedDvMaintainer maintainer(index_file, deletion_vectors);

    ASSERT_EQ(maintainer.DvIndexFile(), index_file);
    auto lookup_hit = maintainer.DeletionVectorOf("file-a");
    ASSERT_TRUE(lookup_hit.has_value());
    auto lookup_miss = maintainer.DeletionVectorOf("missing");
    ASSERT_FALSE(lookup_miss.has_value());
}

TEST(BucketedDvMaintainerTest, TestWriteDeletionVectorsIndexOnlyWhenModified) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto index_file = CreateDvIndexFile(dir->Str());
    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors = {
        {"file-a", CreateDeletionVector(0, 3)},
        {"file-b", CreateDeletionVector(10, 14)},
    };

    BucketedDvMaintainer maintainer(index_file, deletion_vectors);

    // No modification yet, so no index file should be written.
    ASSERT_OK_AND_ASSIGN(auto not_modified_write, maintainer.WriteDeletionVectorsIndex());
    ASSERT_FALSE(not_modified_write.has_value());

    // Removing a missing key should keep the maintainer unmodified.
    maintainer.RemoveDeletionVectorOf("missing");
    ASSERT_OK_AND_ASSIGN(auto still_not_modified_write, maintainer.WriteDeletionVectorsIndex());
    ASSERT_FALSE(still_not_modified_write.has_value());

    // Removing an existing key marks the maintainer modified and triggers one write.
    maintainer.RemoveDeletionVectorOf("file-a");
    ASSERT_OK_AND_ASSIGN(auto modified_write, maintainer.WriteDeletionVectorsIndex());
    ASSERT_TRUE(modified_write.has_value());
    ASSERT_GT(modified_write.value()->FileSize(), 0);

    // Modification flag should be reset after a successful write.
    ASSERT_OK_AND_ASSIGN(auto write_after_reset, maintainer.WriteDeletionVectorsIndex());
    ASSERT_FALSE(write_after_reset.has_value());
}

}  // namespace paimon::test
