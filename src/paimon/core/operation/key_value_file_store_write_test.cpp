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

#include "paimon/core/operation/key_value_file_store_write.h"

#include <cstddef>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/file_store_write.h"
#include "paimon/record_batch.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class KeyValueFileStoreWriteTest : public ::testing::Test {
 protected:
    Result<std::unique_ptr<FileStoreWrite>> CreateSingleStringFileStoreWrite(
        const std::map<std::string, std::string>& table_options, bool with_temp_directory) {
        auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(typed_schema, &schema));

        auto dir = UniqueTestDirectory::Create();
        if (!dir) {
            return Status::Invalid("failed to create test directory");
        }
        PAIMON_ASSIGN_OR_RAISE(auto catalog, Catalog::Create(dir->Str(), {}));
        PAIMON_RETURN_NOT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        PAIMON_RETURN_NOT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                                  /*partition_keys=*/{},
                                                  /*primary_keys=*/{"f0"}, table_options,
                                                  /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
        if (with_temp_directory) {
            context_builder.WithTempDirectory(dir->Str());
        }

        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context,
                               context_builder.Finish());
        return FileStoreWrite::Create(std::move(write_context));
    }

    Status WriteSingleStringRow(FileStoreWrite* file_store_write, int32_t bucket,
                                const std::string& value) {
        auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
        auto struct_type = arrow::struct_(fields);
        arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::StringBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder->Append(value));

        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Finish(&array));
        ::ArrowArray arrow_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &arrow_array));

        RecordBatchBuilder batch_builder(&arrow_array);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch,
                               batch_builder.SetBucket(bucket).Finish());
        Status write_status = file_store_write->Write(std::move(batch));
        if (!ArrowArrayIsReleased(&arrow_array)) {
            ArrowArrayRelease(&arrow_array);
        }
        return write_status;
    }
};

TEST_F(KeyValueFileStoreWriteTest, TestWriteWithInvalidBatch) {
    auto fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::int32()),
        arrow::field("f6", arrow::int32()),    arrow::field("f7", arrow::int64()),
        arrow::field("f8", arrow::int64()),    arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};
    std::string commit_user = "test";
    {
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{"f1"}, /*options=*/{{"bucket", "1"}},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), commit_user);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        ASSERT_NOK_WITH_MSG(file_store_write->Write(nullptr), "batch is null pointer");
    }
    {
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{"f1"}, /*options=*/{{"bucket", "-2"}},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                            commit_user);
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
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             batch_builder.SetBucket(1).Finish());
        ASSERT_NOK_WITH_MSG(file_store_write->Write(std::move(batch)),
                            "batch bucket is 1 while options bucket is -2");
        ArrowArrayRelease(&arrow_array);
    }
    {
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{"f1"}, /*options=*/{{"bucket", "2"}},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                            commit_user);
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
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             batch_builder.SetBucket(3).Finish());
        ASSERT_NOK_WITH_MSG(
            file_store_write->Write(std::move(batch)),
            "fixed bucketed mode must specify a bucket which in [0, 2) in RecordBatch");
        ArrowArrayRelease(&arrow_array);
    }
}

TEST_F(KeyValueFileStoreWriteTest, TestPrepareCommitShouldSucceedWhenLookupEnabledWithIOManager) {
    ASSERT_OK_AND_ASSIGN(
        auto file_store_write,
        CreateSingleStringFileStoreWrite({{"bucket", "1"}, {Options::FORCE_LOOKUP, "true"}},
                                         /*with_temp_directory=*/true));

    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "k1"));
    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 1);
}

TEST_F(KeyValueFileStoreWriteTest,
       TestPrepareCommitShouldSucceedWhenDefaultCompactRewriterPathEnabled) {
    ASSERT_OK_AND_ASSIGN(
        auto file_store_write,
        CreateSingleStringFileStoreWrite({{"bucket", "1"}}, /*with_temp_directory=*/false));

    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "k1"));
    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 1);
}

}  // namespace paimon::test
