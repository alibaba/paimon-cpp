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

#include <gtest/gtest.h>

#include <arrow/c/bridge.h>
#include <arrow/ipc/json.h>

#include "paimon/common/global_index/btree/btree_global_index_writer.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FakeGlobalIndexFileWriter : public GlobalIndexFileWriter {
public:
    FakeGlobalIndexFileWriter(const std::shared_ptr<FileSystem>& fs, const std::string& base_path)
        : fs_(fs), base_path_(base_path), file_counter_(0) {}

    Result<std::string> NewFileName(const std::string& prefix) const override {
        return prefix + "_" + std::to_string(file_counter_++);
    }

    Result<std::unique_ptr<OutputStream>> NewOutputStream(const std::string& file_name) const override {
        return fs_->CreateOutputStream(base_path_ + "/" + file_name);
    }

    Result<int64_t> GetFileSize(const std::string& file_name) const override {
        PAIMON_ASSIGN_OR_RAISE(auto file_status, fs_->GetFileStatus(base_path_ + "/" + file_name));
        return file_status->Length();
    }

    std::string ToPath(const std::string& file_name) const override {
        return base_path_ + "/" + file_name;
    }

private:
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
    mutable int64_t file_counter_;
};

class BTreeGlobalIndexWriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        test_dir_ = UniqueTestDirectory::Create("local");
        ASSERT_OK(test_dir_.status());
        fs_ = test_dir_->GetFileSystem();
        base_path_ = test_dir_->Str();
    }

    void TearDown() override { test_dir_->Delete(); }

    std::shared_ptr<MemoryPool> pool_;
    Result<std::unique_ptr<UniqueTestDirectory>> test_dir_;
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
};

TEST_F(BTreeGlobalIndexWriterTest, WriteIntData) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create the BTree global index writer
    auto writer = std::make_shared<BTreeGlobalIndexWriter>("int_field", file_writer, pool_);

    // Create an Arrow array with int values
    auto json_array = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::int32(), "[1, 2, 3, 2, 1, 4, 5, 5, 5]");
    ASSERT_OK(json_array.status());

    // Export to ArrowArray
    ArrowArray* c_array;
    ASSERT_OK_FROM_ARROW(arrow::ExportArray(*json_array, &c_array));

    // Add batch
    auto status = writer->AddBatch(c_array);
    ASSERT_OK(status);

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata
    const auto& meta = metas[0];
    EXPECT_FALSE(meta.file_path.empty());
    EXPECT_GT(meta.file_size, 0);
    EXPECT_EQ(meta.range_end, 8);  // 9 elements, 0-indexed

    // Release the ArrowArray
    ArrowArrayRelease(c_array);
}

TEST_F(BTreeGlobalIndexWriterTest, WriteStringData) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create the BTree global index writer
    auto writer = std::make_shared<BTreeGlobalIndexWriter>("string_field", file_writer, pool_);

    // Create an Arrow array with string values
    auto json_array = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::utf8(), R"(["apple", "banana", "cherry", "apple", "banana"])");
    ASSERT_OK(json_array.status());

    // Export to ArrowArray
    ArrowArray* c_array;
    ASSERT_OK_FROM_ARROW(arrow::ExportArray(*json_array, &c_array));

    // Add batch
    auto status = writer->AddBatch(c_array);
    ASSERT_OK(status);

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata
    const auto& meta = metas[0];
    EXPECT_FALSE(meta.file_path.empty());
    EXPECT_GT(meta.file_size, 0);

    // Release the ArrowArray
    ArrowArrayRelease(c_array);
}

TEST_F(BTreeGlobalIndexWriterTest, WriteWithNulls) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create the BTree global index writer
    auto writer = std::make_shared<BTreeGlobalIndexWriter>("int_field", file_writer, pool_);

    // Create an Arrow array with null values
    auto json_array = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::int32(), "[1, null, 3, null, 5]");
    ASSERT_OK(json_array.status());

    // Export to ArrowArray
    ArrowArray* c_array;
    ASSERT_OK_FROM_ARROW(arrow::ExportArray(*json_array, &c_array));

    // Add batch
    auto status = writer->AddBatch(c_array);
    ASSERT_OK(status);

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata
    const auto& meta = metas[0];
    EXPECT_FALSE(meta.file_path.empty());
    EXPECT_GT(meta.file_size, 0);

    // Verify that metadata contains null bitmap info (has_nulls should be true)
    EXPECT_NE(meta.metadata, nullptr);

    // Release the ArrowArray
    ArrowArrayRelease(c_array);
}

TEST_F(BTreeGlobalIndexWriterTest, WriteMultipleBatches) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create the BTree global index writer
    auto writer = std::make_shared<BTreeGlobalIndexWriter>("int_field", file_writer, pool_);

    // Create first batch
    auto json_array1 = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::int32(), "[1, 2, 3]");
    ASSERT_OK(json_array1.status());

    ArrowArray* c_array1;
    ASSERT_OK_FROM_ARROW(arrow::ExportArray(*json_array1, &c_array1));

    // Add first batch
    auto status1 = writer->AddBatch(c_array1);
    ASSERT_OK(status1);
    ArrowArrayRelease(c_array1);

    // Create second batch
    auto json_array2 = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::int32(), "[4, 5, 6]");
    ASSERT_OK(json_array2.status());

    ArrowArray* c_array2;
    ASSERT_OK_FROM_ARROW(arrow::ExportArray(*json_array2, &c_array2));

    // Add second batch
    auto status2 = writer->AddBatch(c_array2);
    ASSERT_OK(status2);
    ArrowArrayRelease(c_array2);

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata
    const auto& meta = metas[0];
    EXPECT_EQ(meta.range_end, 5);  // 6 elements, 0-indexed
}

TEST_F(BTreeGlobalIndexWriterTest, WriteEmptyData) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create the BTree global index writer
    auto writer = std::make_shared<BTreeGlobalIndexWriter>("int_field", file_writer, pool_);

    // Finish without adding any data
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 0);  // No data, no metadata
}

TEST_F(BTreeGlobalIndexWriterTest, WriteAllNulls) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create the BTree global index writer
    auto writer = std::make_shared<BTreeGlobalIndexWriter>("int_field", file_writer, pool_);

    // Create an Arrow array with all null values
    auto json_array = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::int32(), "[null, null, null]");
    ASSERT_OK(json_array.status());

    // Export to ArrowArray
    ArrowArray* c_array;
    ASSERT_OK_FROM_ARROW(arrow::ExportArray(*json_array, &c_array));

    // Add batch
    auto status = writer->AddBatch(c_array);
    ASSERT_OK(status);

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata - should have null bitmap but no keys
    const auto& meta = metas[0];
    EXPECT_NE(meta.metadata, nullptr);

    // Release the ArrowArray
    ArrowArrayRelease(c_array);
}

TEST_F(BTreeGlobalIndexWriterTest, WriteDoubleData) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create the BTree global index writer
    auto writer = std::make_shared<BTreeGlobalIndexWriter>("double_field", file_writer, pool_);

    // Create an Arrow array with double values
    auto json_array = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::float64(), "[1.5, 2.5, 3.5, 1.5]");
    ASSERT_OK(json_array.status());

    // Export to ArrowArray
    ArrowArray* c_array;
    ASSERT_OK_FROM_ARROW(arrow::ExportArray(*json_array, &c_array));

    // Add batch
    auto status = writer->AddBatch(c_array);
    ASSERT_OK(status);

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 1);

    // Release the ArrowArray
    ArrowArrayRelease(c_array);
}

}  // namespace paimon::test