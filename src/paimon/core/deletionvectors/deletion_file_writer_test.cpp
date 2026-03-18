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

#include "paimon/core/deletionvectors/deletion_file_writer.h"

#include <map>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(DeletionFileWriterTest, WriteCloseAndReadBack) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    ASSERT_OK_AND_ASSIGN(auto writer, DeletionFileWriter::Create(path_factory, fs, pool));

    RoaringBitmap32 roaring_1;
    roaring_1.Add(1);
    roaring_1.Add(3);
    roaring_1.Add(5);
    auto dv_1 = std::make_shared<BitmapDeletionVector>(roaring_1);

    RoaringBitmap32 roaring_2;
    roaring_2.Add(100);
    auto dv_2 = std::make_shared<BitmapDeletionVector>(roaring_2);

    ASSERT_OK(writer->Write("data-file-1", dv_1));
    ASSERT_OK(writer->Write("data-file-2", dv_2));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(auto meta_unique, writer->GetResult());
    ASSERT_EQ(meta_unique->IndexType(), DeletionVectorsIndexFile::DELETION_VECTORS_INDEX);
    ASSERT_EQ(meta_unique->FileName(), "index-0");
    ASSERT_EQ(meta_unique->RowCount(), 2);
    ASSERT_EQ(meta_unique->ExternalPath(), std::nullopt);
    ASSERT_GT(meta_unique->FileSize(), 0);

    const auto& dv_ranges = meta_unique->DvRanges();
    ASSERT_TRUE(dv_ranges.has_value());
    ASSERT_EQ(dv_ranges->size(), 2);

    auto first = dv_ranges->find("data-file-1");
    auto second = dv_ranges->find("data-file-2");
    ASSERT_NE(first, dv_ranges->end());
    ASSERT_NE(second, dv_ranges->end());
    ASSERT_EQ(first->second.GetOffset(), 1);
    ASSERT_GT(second->second.GetOffset(), first->second.GetOffset());

    std::shared_ptr<IndexFileMeta> meta = std::move(meta_unique);
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);
    ASSERT_OK_AND_ASSIGN(auto read_back, index_file.ReadAllDeletionVectors(meta));

    ASSERT_EQ(read_back.size(), 2);
    ASSERT_EQ(read_back.at("data-file-1")->GetCardinality(), 3);
    ASSERT_EQ(read_back.at("data-file-2")->GetCardinality(), 1);

    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("data-file-1")->IsDeleted(3));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("data-file-1")->IsDeleted(4));
    ASSERT_FALSE(is_deleted);
}

TEST(DeletionFileWriterTest, GetResultWithoutCloseShouldFail) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    ASSERT_OK_AND_ASSIGN(auto writer, DeletionFileWriter::Create(path_factory, fs, pool));
    ASSERT_NOK_WITH_MSG(writer->GetResult(), "result length -1 out of int32 range");
}

TEST(DeletionFileWriterTest, ExternalPathInResult) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    path_factory->SetExternal(true);
    auto pool = GetDefaultPool();

    ASSERT_OK_AND_ASSIGN(auto writer, DeletionFileWriter::Create(path_factory, fs, pool));

    RoaringBitmap32 roaring;
    roaring.Add(0);
    auto dv = std::make_shared<BitmapDeletionVector>(roaring);
    ASSERT_OK(writer->Write("data-file-ext", dv));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(auto meta, writer->GetResult());
    ASSERT_TRUE(meta->ExternalPath().has_value());
    ASSERT_EQ(meta->ExternalPath().value(), PathUtil::JoinPath(dir->Str(), "index-0"));
}

}  // namespace paimon::test
