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

#include "paimon/core/io/external_storage_blob_writer.h"

#include <set>
#include <string>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/long_counter.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class ExternalStorageBlobWriterTest : public ::testing::Test {
 protected:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);

        pool_ = GetDefaultPool();
        seq_num_counter_ = std::make_shared<LongCounter>(0);

        // Create CoreOptions with blob format
        ASSERT_OK_AND_ASSIGN(options_, CoreOptions::FromMap({}));
        file_system_ = options_.GetFileSystem();

        // Create external storage directory
        external_storage_path_ = dir_->Str() + "/external_blob";
        ASSERT_OK(file_system_->Mkdirs(external_storage_path_));

        // Initialize DataFilePathFactory
        path_factory_ = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory_->Init(dir_->Str(), "blob", "data-", nullptr));

        // Schema: int_col (int32) + blob_col (blob)
        auto int_field = arrow::field("int_col", arrow::int32());
        auto blob_field = BlobUtils::ToArrowField("blob_col", false);
        write_schema_ = arrow::schema({int_field, blob_field});
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<LongCounter> seq_num_counter_;
    CoreOptions options_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::string external_storage_path_;
};

TEST_F(ExternalStorageBlobWriterTest, TestEmptyExternalFields) {
    // No external storage fields -> TransformBatch returns original batch
    ExternalStorageBlobWriter writer(write_schema_, /*external_storage_fields=*/{},
                                     external_storage_path_, /*schema_id=*/0, seq_num_counter_,
                                     path_factory_, options_, pool_);

    auto input = std::static_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(write_schema_->fields()),
                                                  R"([[42, "hello"]])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto result, writer.TransformBatch(input));
    ASSERT_TRUE(result->Equals(*input));

    ASSERT_OK(writer.Close());
}

TEST_F(ExternalStorageBlobWriterTest, TestTransformBatchReplacesBlob) {
    std::set<std::string> external_fields = {"blob_col"};
    ExternalStorageBlobWriter writer(write_schema_, external_fields, external_storage_path_,
                                     /*schema_id=*/0, seq_num_counter_, path_factory_, options_,
                                     pool_);

    auto struct_type = arrow::struct_(write_schema_->fields());
    auto input = std::static_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(struct_type, R"([[10, "data1"], [20, "data2"]])")
            .ValueOrDie());

    auto original_int_col = input->field(0);

    ASSERT_OK_AND_ASSIGN(auto result, writer.TransformBatch(input));

    // int_col should be unchanged
    ASSERT_EQ(result->num_fields(), 2);
    ASSERT_TRUE(result->field(0)->Equals(*original_int_col));

    // blob_col should be replaced with serialized BlobDescriptors
    auto descriptor_col = std::static_pointer_cast<arrow::LargeBinaryArray>(result->field(1));
    ASSERT_EQ(descriptor_col->length(), 2);

    for (int64_t i = 0; i < 2; ++i) {
        ASSERT_FALSE(descriptor_col->IsNull(i));
        auto view = descriptor_col->GetView(i);
        ASSERT_OK_AND_ASSIGN(auto descriptor,
                             BlobDescriptor::Deserialize(view.data(), view.size()));
        ASSERT_EQ(descriptor->Length(), 5);
        ASSERT_TRUE(descriptor->Uri().find(external_storage_path_) != std::string::npos);
    }

    ASSERT_OK(writer.Close());
}

TEST_F(ExternalStorageBlobWriterTest, TestAbort) {
    std::set<std::string> external_fields = {"blob_col"};
    ExternalStorageBlobWriter writer(write_schema_, external_fields, external_storage_path_,
                                     /*schema_id=*/0, seq_num_counter_, path_factory_, options_,
                                     pool_);

    auto input = std::static_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(write_schema_->fields()),
                                                  R"([[1, "abort_test"]])")
            .ValueOrDie());

    ASSERT_OK(writer.TransformBatch(input));

    // Verify blob files exist before abort
    std::vector<std::unique_ptr<BasicFileStatus>> files_before;
    ASSERT_OK(file_system_->ListDir(external_storage_path_, &files_before));
    ASSERT_FALSE(files_before.empty());

    // Abort should clean up written blob files
    writer.Abort();

    std::vector<std::unique_ptr<BasicFileStatus>> files_after;
    ASSERT_OK(file_system_->ListDir(external_storage_path_, &files_after));
    ASSERT_TRUE(files_after.empty());
}

}  // namespace paimon::test
