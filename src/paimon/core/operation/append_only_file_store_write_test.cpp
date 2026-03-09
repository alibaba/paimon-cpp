/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/core/operation/append_only_file_store_write.h"

#include <cstddef>
#include <map>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/file_store_write.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class AppendOnlyFileStoreWriteTest : public testing::Test {
 public:
    void SetUp() override {
        fields_ = {arrow::field("f0", arrow::boolean()),
                   arrow::field("f1", arrow::int8()),
                   arrow::field("f2", arrow::int8()),
                   arrow::field("f3", arrow::int16()),
                   arrow::field("f4", arrow::int16()),
                   arrow::field("f5", arrow::int32()),
                   arrow::field("f6", arrow::int32()),
                   arrow::field("f7", arrow::int64()),
                   arrow::field("f8", arrow::int64()),
                   arrow::field("f9", arrow::float32()),
                   arrow::field("f10", arrow::float64()),
                   arrow::field("f11", arrow::utf8()),
                   arrow::field("f12", arrow::binary()),
                   arrow::field("non-partition-field", arrow::int32())};
        commit_user_ = "test_commit_user";
    }

 private:
    arrow::FieldVector fields_;
    std::string commit_user_;
};

TEST_F(AppendOnlyFileStoreWriteTest, TestWriteWithInvalidBatch) {
    {
        arrow::Schema typed_schema(fields_);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{}, /*options=*/{},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), commit_user_);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        ASSERT_NOK_WITH_MSG(file_store_write->Write(nullptr), "batch is null pointer");
    }
    {
        arrow::Schema typed_schema(fields_);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{}, /*options=*/{},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                            commit_user_);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        auto array = std::make_shared<arrow::Array>();
        arrow::StringBuilder builder;
        for (size_t j = 0; j < 100; j++) {
            ASSERT_TRUE(builder.Append(std::to_string(j)).ok());
        }
        ASSERT_TRUE(builder.Finish(&array).ok());
        ::ArrowArray arrow_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch,
            batch_builder.SetBucket(1).SetPartition({{"f0", "true"}, {"f3", "1"}}).Finish());
        ASSERT_NOK_WITH_MSG(file_store_write->Write(std::move(batch)),
                            "batch bucket is 1 while options bucket is -1");
        ArrowArrayRelease(&arrow_array);
    }
}

TEST_F(AppendOnlyFileStoreWriteTest, TestGetMaxSequenceNumberFromMultiPartition) {
    WriteContextBuilder builder(
        paimon::test::GetDataDir() +
            "/orc/multi_partition_append_table.db/multi_partition_append_table/",
        commit_user_);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<WriteContext> write_context,
        builder.AddOption("file.format", "orc").AddOption("manifest.format", "orc").Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));
    auto write = dynamic_cast<AppendOnlyFileStoreWrite*>(file_store_write.get());
    auto pool = GetDefaultPool();
    {
        BinaryRow partition(2);
        BinaryRowWriter writer(&partition, 20, pool.get());
        writer.WriteInt(0, 20);
        writer.WriteInt(1, 1);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> restore_files,
                             write->ScanExistingFileMetas(partition,
                                                          /*bucket=*/0));
        ASSERT_EQ(-1, restore_files->TotalBuckets().value());
        ASSERT_EQ(0, DataFileMeta::GetMaxSequenceNumber(restore_files->DataFiles()));
    }
    {
        BinaryRow partition(2);
        BinaryRowWriter writer(&partition, 20, pool.get());
        writer.WriteInt(0, 10);
        writer.WriteInt(1, 0);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> restore_files,
                             write->ScanExistingFileMetas(partition,
                                                          /*bucket=*/0));
        ASSERT_EQ(-1, restore_files->TotalBuckets().value());
        ASSERT_EQ(2, DataFileMeta::GetMaxSequenceNumber(restore_files->DataFiles()));
    }
    {
        BinaryRow partition(2);
        BinaryRowWriter writer(&partition, 20, pool.get());
        writer.WriteInt(0, 10);
        writer.WriteInt(1, 0);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> restore_files,
                             write->ScanExistingFileMetas(partition,
                                                          /*bucket=*/1));
        ASSERT_EQ(std::nullopt, restore_files->TotalBuckets());
        ASSERT_EQ(-1, DataFileMeta::GetMaxSequenceNumber(restore_files->DataFiles()));
    }
}

}  // namespace paimon::test
