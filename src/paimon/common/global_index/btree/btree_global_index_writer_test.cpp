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

#include "paimon/common/global_index/btree/btree_global_index_writer.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FakeGlobalIndexFileWriter : public GlobalIndexFileWriter {
 public:
    FakeGlobalIndexFileWriter(const std::shared_ptr<FileSystem>& fs, const std::string& base_path)
        : fs_(fs), base_path_(base_path) {}

    Result<std::string> NewFileName(const std::string& prefix) const override {
        return prefix + "_" + std::to_string(file_counter_++);
    }

    Result<std::unique_ptr<OutputStream>> NewOutputStream(
        const std::string& file_name) const override {
        return fs_->Create(base_path_ + "/" + file_name, true);
    }

    Result<int64_t> GetFileSize(const std::string& file_name) const override {
        PAIMON_ASSIGN_OR_RAISE(auto file_status, fs_->GetFileStatus(base_path_ + "/" + file_name));
        return static_cast<int64_t>(file_status->GetLen());
    }

    std::string ToPath(const std::string& file_name) const override {
        return base_path_ + "/" + file_name;
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
    mutable int64_t file_counter_{0};
};

class BTreeGlobalIndexWriterTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        test_dir_ = UniqueTestDirectory::Create("local");
        fs_ = test_dir_->GetFileSystem();
        base_path_ = test_dir_->Str();
        compression_factory_ = BlockCompressionFactory::Create(BlockCompressionType::NONE).value();
    }

    void TearDown() override {}

    // Helper to create ArrowSchema from arrow type
    std::unique_ptr<ArrowSchema> CreateArrowSchema(const std::shared_ptr<arrow::DataType>& type,
                                                   const std::string& field_name) {
        auto schema = arrow::schema({arrow::field(field_name, type)});
        auto c_schema = std::make_unique<ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<BlockCompressionFactory> compression_factory_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
};

TEST_F(BTreeGlobalIndexWriterTest, WriteIntData) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::int32(), "int_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("int_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create an Arrow array with int values
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 3, 2, 1, 4, 5, 5, 5]")
            .ValueOrDie();

    // Export to ArrowArray
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    auto status = writer->AddBatch(&c_array);
    ASSERT_OK(status);

    // Finish writing
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata
    const auto& meta = metas[0];
    EXPECT_FALSE(meta.file_path.empty());
    EXPECT_GT(meta.file_size, 0);
    EXPECT_EQ(meta.range_end, 8);  // 9 elements, 0-indexed

    // Release the ArrowArray
    ArrowArrayRelease(&c_array);

    // Release the ArrowSchema
    ArrowSchemaRelease(c_schema.get());
}

TEST_F(BTreeGlobalIndexWriterTest, WriteStringData) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::utf8(), "string_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("string_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create an Arrow array with string values
    auto array = arrow::ipc::internal::json::ArrayFromJSON(
                     arrow::utf8(), R"(["apple", "banana", "cherry", "apple", "banana"])")
                     .ValueOrDie();

    // Export to ArrowArray
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    auto status = writer->AddBatch(&c_array);
    ASSERT_OK(status);

    // Finish writing
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata
    const auto& meta = metas[0];
    EXPECT_FALSE(meta.file_path.empty());
    EXPECT_GT(meta.file_size, 0);

    // Release the ArrowArray
    ArrowArrayRelease(&c_array);

    // Release the ArrowSchema
    ArrowSchemaRelease(c_schema.get());
}

TEST_F(BTreeGlobalIndexWriterTest, WriteWithNulls) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::int32(), "int_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("int_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create an Arrow array with null values
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, null, 3, null, 5]")
                     .ValueOrDie();

    // Export to ArrowArray
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    auto status = writer->AddBatch(&c_array);
    ASSERT_OK(status);

    // Finish writing
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata
    const auto& meta = metas[0];
    EXPECT_FALSE(meta.file_path.empty());
    EXPECT_GT(meta.file_size, 0);
    EXPECT_NE(meta.metadata, nullptr);

    // Release the ArrowArray
    ArrowArrayRelease(&c_array);

    // Release the ArrowSchema
    ArrowSchemaRelease(c_schema.get());
}

TEST_F(BTreeGlobalIndexWriterTest, WriteMultipleBatches) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::int32(), "int_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("int_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create first batch
    auto array1 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 3]").ValueOrDie();

    ArrowArray c_array1;
    ASSERT_TRUE(arrow::ExportArray(*array1, &c_array1).ok());

    // Add first batch
    auto status1 = writer->AddBatch(&c_array1);
    ASSERT_OK(status1);
    ArrowArrayRelease(&c_array1);

    // Create second batch
    auto array2 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[4, 5, 6]").ValueOrDie();

    ArrowArray c_array2;
    ASSERT_TRUE(arrow::ExportArray(*array2, &c_array2).ok());

    // Add second batch
    auto status2 = writer->AddBatch(&c_array2);
    ASSERT_OK(status2);
    ArrowArrayRelease(&c_array2);

    // Finish writing
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata
    const auto& meta = metas[0];
    EXPECT_EQ(meta.range_end, 5);  // 6 elements, 0-indexed

    // Release the ArrowSchema
    ArrowSchemaRelease(c_schema.get());
}

TEST_F(BTreeGlobalIndexWriterTest, WriteEmptyData) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::int32(), "int_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("int_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Finish without adding any data
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 0);  // No data, no metadata

    // Release the ArrowSchema
    ArrowSchemaRelease(c_schema.get());
}

TEST_F(BTreeGlobalIndexWriterTest, WriteAllNulls) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::int32(), "int_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("int_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create an Arrow array with all null values
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[null, null, null]")
                     .ValueOrDie();

    // Export to ArrowArray
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    auto status = writer->AddBatch(&c_array);
    ASSERT_OK(status);

    // Finish writing
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    // Verify metadata - should have null bitmap but no keys
    const auto& meta = metas[0];
    EXPECT_NE(meta.metadata, nullptr);

    // Release the ArrowArray
    ArrowArrayRelease(&c_array);

    // Release the ArrowSchema
    ArrowSchemaRelease(c_schema.get());
}

TEST_F(BTreeGlobalIndexWriterTest, WriteDoubleData) {
    // Create a fake file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::float64(), "double_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("double_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create an Arrow array with double values
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::float64(), "[1.5, 2.5, 3.5, 1.5]")
                     .ValueOrDie();

    // Export to ArrowArray
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    auto status = writer->AddBatch(&c_array);
    ASSERT_OK(status);

    // Finish writing
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    // Release the ArrowArray
    ArrowArrayRelease(&c_array);

    // Release the ArrowSchema
    ArrowSchemaRelease(c_schema.get());
}

}  // namespace paimon::test
