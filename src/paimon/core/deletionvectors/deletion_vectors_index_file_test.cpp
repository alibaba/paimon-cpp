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

#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"

#include <map>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(DeletionVectorsIndexFileTest, Basic) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(
        fs, path_factory, /*target_size_per_index_file=*/1024 * 1024, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    RoaringBitmap32 roaring_1;
    for (int32_t i = 0; i < 10; ++i) {
        roaring_1.Add(i);
    }
    input["dv1"] = std::make_shared<BitmapDeletionVector>(roaring_1);
    RoaringBitmap32 roaring_2;
    for (int32_t i = 100; i < 110; ++i) {
        roaring_2.Add(i);
    }
    input["dv2"] = std::make_shared<BitmapDeletionVector>(roaring_2);

    ASSERT_FALSE(index_file.Bitmap64());
    ASSERT_OK_AND_ASSIGN(auto meta, index_file.WriteSingleFile(input));
    ASSERT_GT(meta->FileSize(), 0);
    ASSERT_EQ(meta->IndexType(), DeletionVectorsIndexFile::DELETION_VECTORS_INDEX);
    ASSERT_EQ(meta->FileName(), "index-0");
    ASSERT_EQ(meta->ExternalPath(), std::nullopt);
}

TEST(DeletionVectorsIndexFileTest, ExternalPathAndIndexFileMeta) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    path_factory->SetExternal(true);
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory,
                                        /*target_size_per_index_file=*/1024 * 1024,
                                        /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    RoaringBitmap32 roaring;
    for (int32_t i = 0; i < 5; ++i) {
        roaring.Add(i);
    }
    input["dv_ext"] = std::make_shared<BitmapDeletionVector>(roaring);

    ASSERT_OK_AND_ASSIGN(auto meta, index_file.WriteSingleFile(input));
    ASSERT_EQ(meta->ExternalPath().value(), PathUtil::JoinPath(dir->Str(), "index-0"));
}

}  // namespace paimon::test
