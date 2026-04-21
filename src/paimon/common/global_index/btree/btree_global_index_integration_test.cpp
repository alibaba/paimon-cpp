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

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/global_index/btree/btree_global_index_writer.h"
#include "paimon/common/global_index/btree/btree_global_indexer.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
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
    mutable int64_t file_counter_ = 0;
};

class FakeGlobalIndexFileReader : public GlobalIndexFileReader {
 public:
    FakeGlobalIndexFileReader(const std::shared_ptr<FileSystem>& fs, const std::string& base_path)
        : fs_(fs), base_path_(base_path) {}

    Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_path) const override {
        return fs_->Open(file_path);
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
};

class BTreeGlobalIndexIntegrationTest : public ::testing::Test {
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

    // Helper to check if a row ID is in the result
    bool ContainsRowId(const std::shared_ptr<GlobalIndexResult>& result, int64_t row_id) {
        auto iterator_result = result->CreateIterator();
        if (!iterator_result.ok()) {
            return false;
        }
        auto iterator = std::move(iterator_result).value();
        while (iterator->HasNext()) {
            if (iterator->Next() == row_id) {
                return true;
            }
        }
        return false;
    }

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<BlockCompressionFactory> compression_factory_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
};

TEST_F(BTreeGlobalIndexIntegrationTest, WriteAndReadIntData) {
    // Create file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::int32(), "int_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("int_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create an Arrow array with int values
    // Row IDs: 0->1, 1->2, 2->3, 3->2, 4->1, 5->4, 6->5, 7->5, 8->5
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 3, 2, 1, 4, 5, 5, 5]")
            .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    ASSERT_OK(writer->AddBatch(&c_array));

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 1);

    // Release ArrowArray
    ArrowArrayRelease(&c_array);

    // Now read back
    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    std::map<std::string, std::string> options;
    BTreeGlobalIndexer indexer(options);

    // Create a new ArrowSchema for reading (the original was consumed by the writer)
    auto c_schema_read = CreateArrowSchema(arrow::int32(), "int_field");

    // Create reader
    auto reader_result = indexer.CreateReader(c_schema_read.get(), file_reader, metas, pool_);
    ASSERT_OK(reader_result.status());
    auto reader = reader_result.value();

    // Test VisitEqual for value 1 (should return row IDs 0 and 4)
    Literal literal_1(static_cast<int32_t>(1));
    auto equal_result = reader->VisitEqual(literal_1);
    ASSERT_OK(equal_result.status());
    EXPECT_TRUE(ContainsRowId(equal_result.value(), 0));
    EXPECT_TRUE(ContainsRowId(equal_result.value(), 4));
    EXPECT_FALSE(ContainsRowId(equal_result.value(), 1));

    // Test VisitEqual for value 5 (should return row IDs 6, 7, 8)
    Literal literal_5(static_cast<int32_t>(5));
    auto equal_result_5 = reader->VisitEqual(literal_5);
    ASSERT_OK(equal_result_5.status());
    EXPECT_TRUE(ContainsRowId(equal_result_5.value(), 6));
    EXPECT_TRUE(ContainsRowId(equal_result_5.value(), 7));
    EXPECT_TRUE(ContainsRowId(equal_result_5.value(), 8));

    // Release ArrowSchema
    ArrowSchemaRelease(c_schema.get());
    ArrowSchemaRelease(c_schema_read.get());
}

TEST_F(BTreeGlobalIndexIntegrationTest, WriteAndReadStringData) {
    // Create file writer
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

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    ASSERT_OK(writer->AddBatch(&c_array));

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 1);

    // Release ArrowArray
    ArrowArrayRelease(&c_array);

    // Now read back
    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    std::map<std::string, std::string> options;
    BTreeGlobalIndexer indexer(options);

    // Create a new ArrowSchema for reading (the original was consumed by the writer)
    auto c_schema_read = CreateArrowSchema(arrow::utf8(), "string_field");

    // Create reader
    auto reader_result = indexer.CreateReader(c_schema_read.get(), file_reader, metas, pool_);
    ASSERT_OK(reader_result.status());
    auto reader = reader_result.value();

    // Test VisitEqual for "apple" (should return row IDs 0 and 3)
    Literal literal_apple(FieldType::STRING, "apple", 5);
    auto equal_result = reader->VisitEqual(literal_apple);
    ASSERT_OK(equal_result.status());
    EXPECT_TRUE(ContainsRowId(equal_result.value(), 0));
    EXPECT_TRUE(ContainsRowId(equal_result.value(), 3));

    // Release ArrowSchema
    ArrowSchemaRelease(c_schema.get());
    ArrowSchemaRelease(c_schema_read.get());
}

TEST_F(BTreeGlobalIndexIntegrationTest, WriteAndReadWithNulls) {
    // Create file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::int32(), "int_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("int_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create an Arrow array with null values
    // Row IDs: 0->1, 1->null, 2->3, 3->null, 4->5
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, null, 3, null, 5]")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    ASSERT_OK(writer->AddBatch(&c_array));

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();
    ASSERT_EQ(metas.size(), 1);

    // Release ArrowArray
    ArrowArrayRelease(&c_array);

    // Now read back
    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    std::map<std::string, std::string> options;
    BTreeGlobalIndexer indexer(options);

    // Create a new ArrowSchema for reading (the original was consumed by the writer)
    auto c_schema_read = CreateArrowSchema(arrow::int32(), "int_field");

    // Create reader
    auto reader_result = indexer.CreateReader(c_schema_read.get(), file_reader, metas, pool_);
    ASSERT_OK(reader_result.status());
    auto reader = reader_result.value();

    // Test VisitIsNull (should return row IDs 1 and 3)
    auto is_null_result = reader->VisitIsNull();
    ASSERT_OK(is_null_result.status());
    EXPECT_TRUE(ContainsRowId(is_null_result.value(), 1));
    EXPECT_TRUE(ContainsRowId(is_null_result.value(), 3));
    EXPECT_FALSE(ContainsRowId(is_null_result.value(), 0));

    // Test VisitIsNotNull (should return row IDs 0, 2, 4)
    auto is_not_null_result = reader->VisitIsNotNull();
    ASSERT_OK(is_not_null_result.status());
    EXPECT_TRUE(ContainsRowId(is_not_null_result.value(), 0));
    EXPECT_TRUE(ContainsRowId(is_not_null_result.value(), 2));
    EXPECT_TRUE(ContainsRowId(is_not_null_result.value(), 4));
    EXPECT_FALSE(ContainsRowId(is_not_null_result.value(), 1));

    // Release ArrowSchema
    ArrowSchemaRelease(c_schema.get());
    ArrowSchemaRelease(c_schema_read.get());
}

TEST_F(BTreeGlobalIndexIntegrationTest, WriteAndReadRangeQuery) {
    // Create file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::int32(), "int_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("int_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create an Arrow array with int values
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 3, 4, 5]").ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    ASSERT_OK(writer->AddBatch(&c_array));

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();

    // Release ArrowArray
    ArrowArrayRelease(&c_array);

    // Now read back
    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    std::map<std::string, std::string> options;
    BTreeGlobalIndexer indexer(options);

    // Create a new ArrowSchema for reading (the original was consumed by the writer)
    auto c_schema_read = CreateArrowSchema(arrow::int32(), "int_field");

    // Create reader
    auto reader_result = indexer.CreateReader(c_schema_read.get(), file_reader, metas, pool_);
    ASSERT_OK(reader_result.status());
    auto reader = reader_result.value();

    // Test VisitLessThan for value 3 (should return row IDs 0, 1)
    Literal literal_3(static_cast<int32_t>(3));
    auto lt_result = reader->VisitLessThan(literal_3);
    ASSERT_OK(lt_result.status());
    EXPECT_TRUE(ContainsRowId(lt_result.value(), 0));
    EXPECT_TRUE(ContainsRowId(lt_result.value(), 1));
    EXPECT_FALSE(ContainsRowId(lt_result.value(), 2));

    // Test VisitGreaterOrEqual for value 3 (should return row IDs 2, 3, 4)
    auto gte_result = reader->VisitGreaterOrEqual(literal_3);
    ASSERT_OK(gte_result.status());
    EXPECT_TRUE(ContainsRowId(gte_result.value(), 2));
    EXPECT_TRUE(ContainsRowId(gte_result.value(), 3));
    EXPECT_TRUE(ContainsRowId(gte_result.value(), 4));
    EXPECT_FALSE(ContainsRowId(gte_result.value(), 1));

    // Release ArrowSchema
    ArrowSchemaRelease(c_schema.get());
    ArrowSchemaRelease(c_schema_read.get());
}

TEST_F(BTreeGlobalIndexIntegrationTest, WriteAndReadInQuery) {
    // Create file writer
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);

    // Create ArrowSchema
    auto c_schema = CreateArrowSchema(arrow::int32(), "int_field");

    // Create the BTree global index writer
    ASSERT_OK_AND_ASSIGN(auto writer,
                         BTreeGlobalIndexWriter::Create("int_field", c_schema.get(), file_writer,
                                                        compression_factory_, pool_, 4096));

    // Create an Arrow array with int values
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 3, 4, 5]").ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Add batch
    ASSERT_OK(writer->AddBatch(&c_array));

    // Finish writing
    auto result = writer->Finish();
    ASSERT_OK(result.status());
    auto metas = result.value();

    // Release ArrowArray
    ArrowArrayRelease(&c_array);

    // Now read back
    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    std::map<std::string, std::string> options;
    BTreeGlobalIndexer indexer(options);

    // Create a new ArrowSchema for reading (the original was consumed by the writer)
    auto c_schema_read = CreateArrowSchema(arrow::int32(), "int_field");

    // Create reader
    auto reader_result = indexer.CreateReader(c_schema_read.get(), file_reader, metas, pool_);
    ASSERT_OK(reader_result.status());
    auto reader = reader_result.value();

    // Test VisitIn for values 1, 3, 5 (should return row IDs 0, 2, 4)
    std::vector<Literal> in_literals = {Literal(static_cast<int32_t>(1)),
                                        Literal(static_cast<int32_t>(3)),
                                        Literal(static_cast<int32_t>(5))};
    auto in_result = reader->VisitIn(in_literals);
    ASSERT_OK(in_result.status());
    EXPECT_TRUE(ContainsRowId(in_result.value(), 0));
    EXPECT_TRUE(ContainsRowId(in_result.value(), 2));
    EXPECT_TRUE(ContainsRowId(in_result.value(), 4));
    EXPECT_FALSE(ContainsRowId(in_result.value(), 1));
    EXPECT_FALSE(ContainsRowId(in_result.value(), 3));

    // Release ArrowSchema
    ArrowSchemaRelease(c_schema.get());
    ArrowSchemaRelease(c_schema_read.get());
}

}  // namespace paimon::test
