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

#include "paimon/core/deletionvectors/deletion_vector_index_file_writer.h"

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

namespace {

class FailingDeletionVector : public DeletionVector {
 public:
    Status Delete(int64_t) override {
        return Status::Invalid("injected delete failure");
    }

    Result<bool> CheckedDelete(int64_t) override {
        return Status::Invalid("injected checked-delete failure");
    }

    Result<bool> IsDeleted(int64_t) const override {
        return Status::Invalid("injected is-deleted failure");
    }

    bool IsEmpty() const override {
        return true;
    }

    int64_t GetCardinality() const override {
        return 0;
    }

    Result<int32_t> SerializeTo(const std::shared_ptr<MemoryPool>&, DataOutputStream*) override {
        return Status::Invalid("injected serialize failure");
    }

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeToBytes(const std::shared_ptr<MemoryPool>&) override {
        return Status::Invalid("injected serialize failure");
    }

    Status Merge(const std::shared_ptr<DeletionVector>&) override {
        return Status::Invalid("injected merge failure");
    }
};

}  // namespace

TEST(DeletionVectorIndexFileWriterTest, WriteSingleFileRoundTrip) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    RoaringBitmap32 roaring_1;
    roaring_1.Add(1);
    roaring_1.Add(2);
    input["data-a"] = std::make_shared<BitmapDeletionVector>(roaring_1);

    RoaringBitmap32 roaring_2;
    roaring_2.Add(10);
    input["data-b"] = std::make_shared<BitmapDeletionVector>(roaring_2);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> meta, writer.WriteSingleFile(input));
    ASSERT_EQ(meta->IndexType(), DeletionVectorsIndexFile::DELETION_VECTORS_INDEX);
    ASSERT_EQ(meta->FileName(), "index-0");
    ASSERT_EQ(meta->RowCount(), 2);

    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);
    ASSERT_OK_AND_ASSIGN(auto read_back, index_file.ReadAllDeletionVectors(meta));
    ASSERT_EQ(read_back.size(), 2);

    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("data-a")->IsDeleted(1));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("data-a")->IsDeleted(3));
    ASSERT_FALSE(is_deleted);

    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("data-b")->IsDeleted(10));
    ASSERT_TRUE(is_deleted);
}

TEST(DeletionVectorIndexFileWriterTest, WriteSingleFileShouldReturnSerializeError) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    input["bad"] = std::make_shared<FailingDeletionVector>();

    ASSERT_NOK_WITH_MSG(writer.WriteSingleFile(input), "injected serialize failure");
}

}  // namespace paimon::test
