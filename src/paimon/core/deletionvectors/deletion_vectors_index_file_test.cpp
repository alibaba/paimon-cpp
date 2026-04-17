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
#include <vector>

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
    auto index_file =
        std::make_shared<DeletionVectorsIndexFile>(fs, path_factory, /*bitmap64=*/false, pool);

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

    ASSERT_FALSE(index_file->Bitmap64());
    ASSERT_OK_AND_ASSIGN(auto meta, index_file->WriteSingleFile(input));
    ASSERT_GT(meta->FileSize(), 0);
    ASSERT_OK_AND_ASSIGN(auto size, index_file->FileSize(meta));
    ASSERT_EQ(meta->FileSize(), size);
    ASSERT_EQ(meta->IndexType(), DeletionVectorsIndexFile::DELETION_VECTORS_INDEX);
    ASSERT_EQ(meta->FileName(), "index-0");
    ASSERT_FALSE(index_file->IsExternalPath());
    ASSERT_EQ(meta->ExternalPath(), std::nullopt);

    // Round trip: write then read all deletion vectors from index file.
    ASSERT_OK_AND_ASSIGN(auto read_back, index_file->ReadAllDeletionVectors(meta));
    ASSERT_EQ(read_back.size(), input.size());
    ASSERT_EQ(read_back.at("dv1")->GetCardinality(), 10);
    ASSERT_EQ(read_back.at("dv2")->GetCardinality(), 10);

    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("dv1")->IsDeleted(0));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv1")->IsDeleted(10));
    ASSERT_FALSE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv2")->IsDeleted(100));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv2")->IsDeleted(99));
    ASSERT_FALSE(is_deleted);
}

TEST(DeletionVectorsIndexFileTest, ExternalPathAndIndexFileMeta) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    path_factory->SetExternal(true);
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory,
                                        /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    RoaringBitmap32 roaring;
    for (int32_t i = 0; i < 5; ++i) {
        roaring.Add(i);
    }
    input["dv_ext"] = std::make_shared<BitmapDeletionVector>(roaring);

    ASSERT_OK_AND_ASSIGN(auto meta, index_file.WriteSingleFile(input));
    ASSERT_EQ(meta->ExternalPath().value(), PathUtil::JoinPath(dir->Str(), "index-0"));

    // Round trip for external path index file.
    ASSERT_OK_AND_ASSIGN(auto read_back, index_file.ReadAllDeletionVectors(meta));
    ASSERT_EQ(read_back.size(), 1);
    ASSERT_EQ(read_back.at("dv_ext")->GetCardinality(), 5);
    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("dv_ext")->IsDeleted(0));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv_ext")->IsDeleted(5));
    ASSERT_FALSE(is_deleted);
}

TEST(DeletionVectorsIndexFileTest, RoundTripEmptyInput) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    ASSERT_OK_AND_ASSIGN(auto meta, index_file.WriteSingleFile(input));
    ASSERT_EQ(meta->RowCount(), 0);
    ASSERT_OK_AND_ASSIGN(auto read_back, index_file.ReadAllDeletionVectors(meta));
    ASSERT_TRUE(read_back.empty());
}

TEST(DeletionVectorsIndexFileTest, RoundTripMultipleIndexFilesMerge) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input1;
    RoaringBitmap32 roaring_1;
    roaring_1.Add(1);
    roaring_1.Add(3);
    input1["dv_a"] = std::make_shared<BitmapDeletionVector>(roaring_1);
    ASSERT_OK_AND_ASSIGN(auto meta1, index_file.WriteSingleFile(input1));

    std::map<std::string, std::shared_ptr<DeletionVector>> input2;
    RoaringBitmap32 roaring_2;
    roaring_2.Add(8);
    input2["dv_b"] = std::make_shared<BitmapDeletionVector>(roaring_2);
    ASSERT_OK_AND_ASSIGN(auto meta2, index_file.WriteSingleFile(input2));

    ASSERT_OK_AND_ASSIGN(auto read_back,
                         index_file.ReadAllDeletionVectors(
                             std::vector<std::shared_ptr<IndexFileMeta>>{meta1, meta2}));
    ASSERT_EQ(read_back.size(), 2);

    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("dv_a")->IsDeleted(1));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv_b")->IsDeleted(8));
    ASSERT_TRUE(is_deleted);
}

TEST(DeletionVectorsIndexFileTest, RoundTripMultipleIndexFilesLastWriteWinsOnSameKey) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input1;
    RoaringBitmap32 roaring_old;
    roaring_old.Add(2);
    input1["same_dv"] = std::make_shared<BitmapDeletionVector>(roaring_old);
    ASSERT_OK_AND_ASSIGN(auto meta1, index_file.WriteSingleFile(input1));

    std::map<std::string, std::shared_ptr<DeletionVector>> input2;
    RoaringBitmap32 roaring_new;
    roaring_new.Add(9);
    input2["same_dv"] = std::make_shared<BitmapDeletionVector>(roaring_new);
    ASSERT_OK_AND_ASSIGN(auto meta2, index_file.WriteSingleFile(input2));

    ASSERT_OK_AND_ASSIGN(auto read_back,
                         index_file.ReadAllDeletionVectors(
                             std::vector<std::shared_ptr<IndexFileMeta>>{meta1, meta2}));
    ASSERT_EQ(read_back.size(), 1);

    ASSERT_OK_AND_ASSIGN(bool is_deleted_old, read_back.at("same_dv")->IsDeleted(2));
    ASSERT_FALSE(is_deleted_old);
    ASSERT_OK_AND_ASSIGN(bool is_deleted_new, read_back.at("same_dv")->IsDeleted(9));
    ASSERT_TRUE(is_deleted_new);
}

}  // namespace paimon::test
