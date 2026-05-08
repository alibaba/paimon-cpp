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
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/global_index/btree/btree_global_index_writer.h"
#include "paimon/common/global_index/btree/btree_global_indexer.h"
#include "paimon/common/global_index/btree/lazy_filtered_btree_reader.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/io/buffered_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/io_exception_helper.h"
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

class BTreeGlobalIndexIntegrationTest : public ::testing::Test,
                                        public ::testing::WithParamInterface<std::string> {
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
    std::unique_ptr<ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::Field>& field) const {
        auto schema = arrow::schema({field});
        auto c_schema = std::make_unique<ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

    void CheckResult(const std::shared_ptr<GlobalIndexResult>& result,
                     const std::vector<int64_t>& expected) const {
        auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected).ToString();
    }

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<BlockCompressionFactory> compression_factory_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
};

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadIntData) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "4096"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value):
    //   0->1, 1->1, 2->null, 3->2, 4->2, 5->null, 6->3, 7->4, 8->5, 9->5, 10->5, 11->null
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [1],
        [1],
        [null],
        [2],
        [2],
        [null],
        [3],
        [4],
        [5],
        [5],
        [5],
        [null]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    // Now read back
    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // All non-null row ids: {0,1,3,4,6,7,8,9,10}
    // Null row ids: {2,5,11}

    // --- VisitIsNull ---
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {2, 5, 11});
    }

    // --- VisitIsNotNull ---
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 1, 3, 4, 6, 7, 8, 9, 10});
    }

    // --- VisitEqual ---
    {
        // Equal to 1 -> rows 0,1
        Literal literal_1(1);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_1));
        CheckResult(result, {0, 1});

        // Equal to 3 -> row 6
        Literal literal_3(3);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitEqual(literal_3));
        CheckResult(result, {6});

        // Equal to 5 -> rows 8,9,10
        Literal literal_5(5);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitEqual(literal_5));
        CheckResult(result, {8, 9, 10});

        // Equal to 99 (not present) -> empty
        Literal literal_99(99);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitEqual(literal_99));
        CheckResult(result, {});
    }

    // --- VisitNotEqual ---
    {
        // NotEqual to 3 -> all non-null except row 6
        Literal literal_3(3);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(literal_3));
        CheckResult(result, {0, 1, 3, 4, 7, 8, 9, 10});
    }

    // --- VisitLessThan ---
    {
        // LessThan 3 -> values 1,2 -> rows 0,1,3,4
        Literal literal_3(3);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(literal_3));
        CheckResult(result, {0, 1, 3, 4});

        // LessThan 1 -> empty (no value < 1)
        Literal literal_1(1);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitLessThan(literal_1));
        CheckResult(result, {});
    }

    // --- VisitLessOrEqual ---
    {
        // LessOrEqual 3 -> values 1,2,3 -> rows 0,1,3,4,6
        Literal literal_3(3);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessOrEqual(literal_3));
        CheckResult(result, {0, 1, 3, 4, 6});
    }

    // --- VisitGreaterThan ---
    {
        // GreaterThan 3 -> values 4,5 -> rows 7,8,9,10
        Literal literal_3(3);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(literal_3));
        CheckResult(result, {7, 8, 9, 10});

        // GreaterThan 5 -> empty (no value > 5)
        Literal literal_5(5);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitGreaterThan(literal_5));
        CheckResult(result, {});
    }

    // --- VisitGreaterOrEqual ---
    {
        // GreaterOrEqual 3 -> values 3,4,5 -> rows 6,7,8,9,10
        Literal literal_3(3);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(literal_3));
        CheckResult(result, {6, 7, 8, 9, 10});
    }

    // --- VisitIn ---
    {
        // In {1, 4} -> rows 0,1,7
        std::vector<Literal> in_literals = {Literal(1), Literal(4)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 1, 7});

        // In {99} (not present) -> empty
        std::vector<Literal> in_missing = {Literal(99)};
        ASSERT_OK_AND_ASSIGN(result, reader->VisitIn(in_missing));
        CheckResult(result, {});
    }

    // --- VisitNotIn ---
    {
        // NotIn {1, 5} -> all non-null except rows 0,1,8,9,10 -> rows 3,4,6,7
        std::vector<Literal> not_in_literals = {Literal(1), Literal(5)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {3, 4, 6, 7});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadStringData) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("str_field", arrow::utf8());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("str_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value):
    //   0->"apple", 1->"apricot", 2->null, 3->"banana", 4->"blueberry",
    //   5->null, 6->"cherry", 7->"cherry", 8->"date"
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        ["apple"],
        ["apricot"],
        [null],
        ["banana"],
        ["blueberry"],
        [null],
        ["cherry"],
        ["cherry"],
        ["date"]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,1,3,4,6,7,8}, Null rows: {2,5}

    // --- VisitIsNull / VisitIsNotNull ---
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {2, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 1, 3, 4, 6, 7, 8});
    }

    // --- VisitEqual ---
    {
        Literal lit_cherry(FieldType::STRING, "cherry", 6);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_cherry));
        CheckResult(result, {6, 7});

        Literal lit_missing(FieldType::STRING, "fig", 3);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitEqual(lit_missing));
        CheckResult(result, {});
    }

    // --- VisitNotEqual ---
    {
        Literal lit_banana(FieldType::STRING, "banana", 6);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_banana));
        CheckResult(result, {0, 1, 4, 6, 7, 8});
    }

    // --- VisitLessThan ---
    {
        // LessThan "cherry" -> "apple","apricot","banana","blueberry" -> rows 0,1,3,4
        Literal lit_cherry(FieldType::STRING, "cherry", 6);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_cherry));
        CheckResult(result, {0, 1, 3, 4});
    }

    // --- VisitLessOrEqual ---
    {
        Literal lit_banana(FieldType::STRING, "banana", 6);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessOrEqual(lit_banana));
        CheckResult(result, {0, 1, 3});
    }

    // --- VisitGreaterThan ---
    {
        Literal lit_cherry(FieldType::STRING, "cherry", 6);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(lit_cherry));
        CheckResult(result, {8});
    }

    // --- VisitGreaterOrEqual ---
    {
        Literal lit_cherry(FieldType::STRING, "cherry", 6);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_cherry));
        CheckResult(result, {6, 7, 8});
    }

    // --- VisitIn ---
    {
        std::vector<Literal> in_literals = {Literal(FieldType::STRING, "apple", 5),
                                            Literal(FieldType::STRING, "date", 4)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 8});
    }

    // --- VisitNotIn ---
    {
        std::vector<Literal> not_in_literals = {Literal(FieldType::STRING, "apple", 5),
                                                Literal(FieldType::STRING, "cherry", 6)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {1, 3, 4, 8});
    }

    // --- VisitStartsWith ---
    {
        // StartsWith "ap" -> "apple","apricot" -> rows 0,1
        Literal lit_ap(FieldType::STRING, "ap", 2);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitStartsWith(lit_ap));
        CheckResult(result, {0, 1});

        // StartsWith "bl" -> "blueberry" -> row 4
        Literal lit_bl(FieldType::STRING, "bl", 2);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitStartsWith(lit_bl));
        CheckResult(result, {4});

        // StartsWith "z" -> no match
        Literal lit_z(FieldType::STRING, "z", 1);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitStartsWith(lit_z));
        CheckResult(result, {});
    }

    // --- VisitEndsWith (falls back to AllNonNullRows) ---
    {
        Literal lit_ry(FieldType::STRING, "ry", 2);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEndsWith(lit_ry));
        CheckResult(result, {0, 1, 3, 4, 6, 7, 8});
    }

    // --- VisitContains (falls back to AllNonNullRows) ---
    {
        Literal lit_an(FieldType::STRING, "an", 2);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitContains(lit_an));
        CheckResult(result, {0, 1, 3, 4, 6, 7, 8});
    }

    // --- VisitLike ---
    {
        // "ap%" is a prefix pattern -> delegates to VisitStartsWith("ap") -> rows 0,1
        Literal lit_ap_pct(FieldType::STRING, "ap%", 3);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLike(lit_ap_pct));
        CheckResult(result, {0, 1});

        // "%erry" is not a prefix pattern -> falls back to AllNonNullRows
        Literal lit_suffix(FieldType::STRING, "%erry", 5);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitLike(lit_suffix));
        CheckResult(result, {0, 1, 3, 4, 6, 7, 8});

        // "b_nana" contains '_' before '%' -> falls back to AllNonNullRows
        Literal lit_underscore(FieldType::STRING, "b_nana", 6);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitLike(lit_underscore));
        CheckResult(result, {0, 1, 3, 4, 6, 7, 8});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadBigIntData) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("bigint_field", arrow::int64());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("bigint_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value):
    //   0->100, 1->null, 2->200, 3->200, 4->300, 5->null, 6->400
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [100],
        [null],
        [200],
        [200],
        [300],
        [null],
        [400]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,2,3,4,6}, Null rows: {1,5}

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {1, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 2, 3, 4, 6});
    }
    {
        Literal lit_200(static_cast<int64_t>(200));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_200));
        CheckResult(result, {2, 3});
    }
    {
        Literal lit_200(static_cast<int64_t>(200));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_200));
        CheckResult(result, {0, 4, 6});
    }
    {
        Literal lit_300(static_cast<int64_t>(300));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_300));
        CheckResult(result, {0, 2, 3});
    }
    {
        Literal lit_300(static_cast<int64_t>(300));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessOrEqual(lit_300));
        CheckResult(result, {0, 2, 3, 4});
    }
    {
        Literal lit_200(static_cast<int64_t>(200));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(lit_200));
        CheckResult(result, {4, 6});
    }
    {
        Literal lit_200(static_cast<int64_t>(200));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_200));
        CheckResult(result, {2, 3, 4, 6});
    }
    {
        std::vector<Literal> in_literals = {Literal(static_cast<int64_t>(100)),
                                            Literal(static_cast<int64_t>(400))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 6});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(static_cast<int64_t>(200))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 4, 6});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadFloatData) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("float_field", arrow::float32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("float_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value):
    //   0->1.0, 1->null, 2->2.5, 3->2.5, 4->3.0, 5->null, 6->4.5
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [1.0],
        [null],
        [2.5],
        [2.5],
        [3.0],
        [null],
        [4.5]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,2,3,4,6}, Null rows: {1,5}

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {1, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 2, 3, 4, 6});
    }
    {
        Literal lit_2_5(2.5f);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_2_5));
        CheckResult(result, {2, 3});
    }
    {
        Literal lit_2_5(2.5f);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_2_5));
        CheckResult(result, {0, 4, 6});
    }
    {
        Literal lit_3_0(3.0f);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_3_0));
        CheckResult(result, {0, 2, 3});
    }
    {
        Literal lit_3_0(3.0f);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessOrEqual(lit_3_0));
        CheckResult(result, {0, 2, 3, 4});
    }
    {
        Literal lit_2_5(2.5f);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(lit_2_5));
        CheckResult(result, {4, 6});
    }
    {
        Literal lit_2_5(2.5f);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_2_5));
        CheckResult(result, {2, 3, 4, 6});
    }
    {
        std::vector<Literal> in_literals = {Literal(1.0f), Literal(4.5f)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 6});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(2.5f)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 4, 6});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadDoubleData) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("double_field", arrow::float64());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("double_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value):
    //   0->1.1, 1->null, 2->2.2, 3->2.2, 4->3.3, 5->null, 6->4.4
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [1.1],
        [null],
        [2.2],
        [2.2],
        [3.3],
        [null],
        [4.4]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,2,3,4,6}, Null rows: {1,5}

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {1, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 2, 3, 4, 6});
    }
    {
        Literal lit_2_2(2.2);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_2_2));
        CheckResult(result, {2, 3});
    }
    {
        Literal lit_2_2(2.2);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_2_2));
        CheckResult(result, {0, 4, 6});
    }
    {
        Literal lit_3_3(3.3);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_3_3));
        CheckResult(result, {0, 2, 3});
    }
    {
        Literal lit_3_3(3.3);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessOrEqual(lit_3_3));
        CheckResult(result, {0, 2, 3, 4});
    }
    {
        Literal lit_2_2(2.2);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(lit_2_2));
        CheckResult(result, {4, 6});
    }
    {
        Literal lit_2_2(2.2);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_2_2));
        CheckResult(result, {2, 3, 4, 6});
    }
    {
        std::vector<Literal> in_literals = {Literal(1.1), Literal(4.4)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 6});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(2.2)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 4, 6});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadAllNonNull) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_));
    // All values are non-null
    // Data layout (row_id -> value): 0->10, 1->20, 2->20, 3->30, 4->40, 5->50
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [10],
        [20],
        [20],
        [30],
        [40],
        [50]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // All rows: {0,1,2,3,4,5}, No null rows

    // --- VisitIsNull -> empty ---
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {});
    }

    // --- VisitIsNotNull -> all rows ---
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 1, 2, 3, 4, 5});
    }

    // --- VisitEqual ---
    {
        Literal lit_20(20);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_20));
        CheckResult(result, {1, 2});
    }

    // --- VisitNotEqual ---
    {
        Literal lit_20(20);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_20));
        CheckResult(result, {0, 3, 4, 5});
    }

    // --- VisitLessThan ---
    {
        Literal lit_30(30);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_30));
        CheckResult(result, {0, 1, 2});
    }

    // --- VisitLessOrEqual ---
    {
        Literal lit_30(30);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessOrEqual(lit_30));
        CheckResult(result, {0, 1, 2, 3});
    }

    // --- VisitGreaterThan ---
    {
        Literal lit_30(30);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(lit_30));
        CheckResult(result, {4, 5});
    }

    // --- VisitGreaterOrEqual ---
    {
        Literal lit_30(30);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_30));
        CheckResult(result, {3, 4, 5});
    }

    // --- VisitIn ---
    {
        std::vector<Literal> in_literals = {Literal(10), Literal(50)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 5});
    }

    // --- VisitNotIn ---
    {
        std::vector<Literal> not_in_literals = {Literal(10), Literal(20)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {3, 4, 5});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadBoolData) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("bool_field", arrow::boolean());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("bool_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value), sorted by key (false < true):
    //   0->false, 1->false, 2->null, 3->false, 4->true, 5->null, 6->true, 7->true
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [false],
        [false],
        [null],
        [false],
        [true],
        [null],
        [true],
        [true]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,1,3,4,6,7}, Null rows: {2,5}

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {2, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 1, 3, 4, 6, 7});
    }
    {
        Literal lit_true(true);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_true));
        CheckResult(result, {4, 6, 7});
    }
    {
        Literal lit_false(false);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_false));
        CheckResult(result, {0, 1, 3});
    }
    {
        Literal lit_true(true);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_true));
        CheckResult(result, {0, 1, 3});
    }
    {
        std::vector<Literal> in_literals = {Literal(true)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {4, 6, 7});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(true)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 1, 3});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadTinyIntData) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("tinyint_field", arrow::int8());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(
        auto writer, indexer->CreateWriter("tinyint_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value):
    //   0->-10, 1->null, 2->0, 3->10, 4->10, 5->null, 6->20
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [-10],
        [null],
        [0],
        [10],
        [10],
        [null],
        [20]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,2,3,4,6}, Null rows: {1,5}

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {1, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 2, 3, 4, 6});
    }
    {
        Literal lit_10(static_cast<int8_t>(10));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_10));
        CheckResult(result, {3, 4});
    }
    {
        Literal lit_10(static_cast<int8_t>(10));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_10));
        CheckResult(result, {0, 2, 6});
    }
    {
        Literal lit_10(static_cast<int8_t>(10));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_10));
        CheckResult(result, {0, 2});
    }
    {
        Literal lit_10(static_cast<int8_t>(10));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_10));
        CheckResult(result, {3, 4, 6});
    }
    {
        std::vector<Literal> in_literals = {Literal(static_cast<int8_t>(-10)),
                                            Literal(static_cast<int8_t>(20))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 6});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(static_cast<int8_t>(10))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 2, 6});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadSmallIntData) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("smallint_field", arrow::int16());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(
        auto writer, indexer->CreateWriter("smallint_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value):
    //   0->-100, 1->null, 2->0, 3->100, 4->100, 5->null, 6->200
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [-100],
        [null],
        [0],
        [100],
        [100],
        [null],
        [200]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,2,3,4,6}, Null rows: {1,5}

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {1, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 2, 3, 4, 6});
    }
    {
        Literal lit_100(static_cast<int16_t>(100));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_100));
        CheckResult(result, {3, 4});
    }
    {
        Literal lit_100(static_cast<int16_t>(100));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_100));
        CheckResult(result, {0, 2, 6});
    }
    {
        Literal lit_100(static_cast<int16_t>(100));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_100));
        CheckResult(result, {0, 2});
    }
    {
        Literal lit_100(static_cast<int16_t>(100));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_100));
        CheckResult(result, {3, 4, 6});
    }
    {
        std::vector<Literal> in_literals = {Literal(static_cast<int16_t>(-100)),
                                            Literal(static_cast<int16_t>(200))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 6});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(static_cast<int16_t>(100))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 2, 6});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadTimestampCompactData) {
    // Compact timestamp: precision <= 3 (millisecond)
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("ts_field", arrow::timestamp(arrow::TimeUnit::MILLI));
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("ts_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value in millis):
    //   0->1000, 1->null, 2->2000, 3->2000, 4->3000, 5->null, 6->4000
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [1000],
        [null],
        [2000],
        [2000],
        [3000],
        [null],
        [4000]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,2,3,4,6}, Null rows: {1,5}

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {1, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 2, 3, 4, 6});
    }
    {
        Literal lit_2000(Timestamp::FromEpochMillis(2000));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_2000));
        CheckResult(result, {2, 3});
    }
    {
        Literal lit_2000(Timestamp::FromEpochMillis(2000));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_2000));
        CheckResult(result, {0, 4, 6});
    }
    {
        Literal lit_3000(Timestamp::FromEpochMillis(3000));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_3000));
        CheckResult(result, {0, 2, 3});
    }
    {
        Literal lit_2000(Timestamp::FromEpochMillis(2000));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_2000));
        CheckResult(result, {2, 3, 4, 6});
    }
    {
        std::vector<Literal> in_literals = {Literal(Timestamp::FromEpochMillis(1000)),
                                            Literal(Timestamp::FromEpochMillis(4000))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 6});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(Timestamp::FromEpochMillis(2000))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 4, 6});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadTimestampNonCompactData) {
    // Non-compact timestamp: precision > 3 (microsecond, precision=6)
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("ts_field", arrow::timestamp(arrow::TimeUnit::MICRO));
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("ts_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value in micros):
    //   0->1000000 (1s), 1->null, 2->2000123 (2s+123us), 3->2000123, 4->3000456, 5->null,
    //   6->4000789
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [1000000],
        [null],
        [2000123],
        [2000123],
        [3000456],
        [null],
        [4000789]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,2,3,4,6}, Null rows: {1,5}
    // micros: 1000000 -> millis=1000, nanos_of_millis=0
    //         2000123 -> millis=2000, nanos_of_millis=123000
    //         3000456 -> millis=3000, nanos_of_millis=456000
    //         4000789 -> millis=4000, nanos_of_millis=789000

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {1, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 2, 3, 4, 6});
    }
    {
        Literal lit_ts(Timestamp(2000, 123000));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_ts));
        CheckResult(result, {2, 3});
    }
    {
        Literal lit_ts(Timestamp(2000, 123000));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_ts));
        CheckResult(result, {0, 4, 6});
    }
    {
        Literal lit_ts(Timestamp(3000, 456000));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_ts));
        CheckResult(result, {0, 2, 3});
    }
    {
        Literal lit_ts(Timestamp(2000, 123000));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_ts));
        CheckResult(result, {2, 3, 4, 6});
    }
    {
        std::vector<Literal> in_literals = {Literal(Timestamp(1000, 0)),
                                            Literal(Timestamp(4000, 789000))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 6});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(Timestamp(2000, 123000))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 4, 6});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadDecimalCompactData) {
    // Compact decimal: precision <= 18
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("decimal_field", arrow::decimal128(10, 2));
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(
        auto writer, indexer->CreateWriter("decimal_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> value, stored as unscaled int with scale=2):
    //   0->1.00, 1->null, 2->2.50, 3->2.50, 4->3.00,
    //   5->null, 6->4.50
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        ["1.00"],
        [null],
        ["2.50"],
        ["2.50"],
        ["3.00"],
        [null],
        ["4.50"]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,2,3,4,6}, Null rows: {1,5}

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {1, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 2, 3, 4, 6});
    }
    {
        Literal lit_250(Decimal::FromUnscaledLong(250, 10, 2));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_250));
        CheckResult(result, {2, 3});
    }
    {
        Literal lit_250(Decimal::FromUnscaledLong(250, 10, 2));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_250));
        CheckResult(result, {0, 4, 6});
    }
    {
        Literal lit_300(Decimal::FromUnscaledLong(300, 10, 2));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_300));
        CheckResult(result, {0, 2, 3});
    }
    {
        Literal lit_250(Decimal::FromUnscaledLong(250, 10, 2));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_250));
        CheckResult(result, {2, 3, 4, 6});
    }
    {
        std::vector<Literal> in_literals = {Literal(Decimal::FromUnscaledLong(100, 10, 2)),
                                            Literal(Decimal::FromUnscaledLong(450, 10, 2))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 6});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(Decimal::FromUnscaledLong(250, 10, 2))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 4, 6});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadDecimalNonCompactData) {
    // Non-compact decimal: precision > 18
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("decimal_field", arrow::decimal128(25, 3));
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(
        auto writer, indexer->CreateWriter("decimal_field", c_schema.get(), file_writer, pool_));
    // Data layout (row_id -> unscaled value with scale=3):
    //   0->1.000, 1->null, 2->2.500, 3->2.500,
    //   4->3.000, 5->null, 6->4.500
    // For non-compact decimal (precision=25), Arrow JSON uses string representation of unscaled
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        ["1.000"],
        [null],
        ["2.500"],
        ["2.500"],
        ["3.000"],
        [null],
        ["4.500"]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // Non-null rows: {0,2,3,4,6}, Null rows: {1,5}

    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {1, 5});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 2, 3, 4, 6});
    }
    {
        Literal lit_2500(Decimal(25, 3, 2500));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_2500));
        CheckResult(result, {2, 3});
    }
    {
        Literal lit_2500(Decimal(25, 3, 2500));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_2500));
        CheckResult(result, {0, 4, 6});
    }
    {
        Literal lit_3000(Decimal(25, 3, 3000));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_3000));
        CheckResult(result, {0, 2, 3});
    }
    {
        Literal lit_2500(Decimal(25, 3, 2500));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_2500));
        CheckResult(result, {2, 3, 4, 6});
    }
    {
        std::vector<Literal> in_literals = {Literal(Decimal(25, 3, 1000)),
                                            Literal(Decimal(25, 3, 4500))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {0, 6});
    }
    {
        std::vector<Literal> not_in_literals = {Literal(Decimal(25, 3, 2500))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {0, 4, 6});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadAllNull) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_));
    // All values are null
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [null],
        [null],
        [null],
        [null]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

    // All rows are null: {0,1,2,3}, No non-null rows

    // --- VisitIsNull -> all rows ---
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {0, 1, 2, 3});
    }

    // --- VisitIsNotNull -> empty ---
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {});
    }

    // --- VisitEqual -> empty ---
    {
        Literal lit_1(1);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_1));
        CheckResult(result, {});
    }

    // --- VisitNotEqual -> empty (no non-null rows) ---
    {
        Literal lit_1(1);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_1));
        CheckResult(result, {});
    }

    // --- VisitLessThan -> empty ---
    {
        Literal lit_1(1);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_1));
        CheckResult(result, {});
    }

    // --- VisitIn -> empty ---
    {
        std::vector<Literal> in_literals = {Literal(1), Literal(2)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        CheckResult(result, {});
    }

    // --- VisitNotIn -> empty ---
    {
        std::vector<Literal> not_in_literals = {Literal(1)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(not_in_literals));
        CheckResult(result, {});
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadLargeDataWithSmallBlocks) {
    // Use very small block size and cache size to force multiple block evictions
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {
        {BtreeDefs::kBtreeIndexBlockSize, "256"},
        {BtreeDefs::kBtreeIndexCacheSize, "1024"},
    };
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_));
    // Generate 50000 sorted int values with some nulls and duplicates.
    // Pattern: every 100th row is null, values increase by 1 every 3 rows (duplicates).
    // Data is written in multiple batches of 1000 rows each.
    constexpr int32_t total_rows = 50000;
    constexpr int32_t batch_size = 1000;

    std::vector<int64_t> null_row_ids;
    std::vector<int64_t> non_null_row_ids;
    int32_t current_value = 0;

    for (int32_t batch_start = 0; batch_start < total_rows; batch_start += batch_size) {
        int32_t batch_end = std::min(batch_start + batch_size, total_rows);
        int32_t batch_len = batch_end - batch_start;

        arrow::Int32Builder value_builder;
        ASSERT_TRUE(value_builder.Reserve(batch_len).ok());

        std::vector<int64_t> batch_row_ids;
        batch_row_ids.reserve(batch_len);

        for (int32_t i = batch_start; i < batch_end; ++i) {
            batch_row_ids.push_back(i);
            if (i % 100 == 99) {
                ASSERT_TRUE(value_builder.AppendNull().ok());
                null_row_ids.push_back(i);
            } else {
                ASSERT_TRUE(value_builder.Append(current_value).ok());
                non_null_row_ids.push_back(i);
                if (i % 3 == 2) {
                    ++current_value;
                }
            }
        }

        std::shared_ptr<arrow::Array> value_array;
        ASSERT_TRUE(value_builder.Finish(&value_array).ok());
        auto struct_array = arrow::StructArray::Make({value_array}, {field}).ValueOrDie();

        ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*struct_array, &c_array).ok());
        ASSERT_OK(writer->AddBatch(&c_array, std::move(batch_row_ids)));
    }

    ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1);

    // Helper lambda: given a predicate on value, collect matching row ids
    auto collect_rows = [total_rows = total_rows](std::function<bool(int32_t)> predicate) {
        std::vector<int64_t> result;
        int32_t val = 0;
        for (int32_t i = 0; i < total_rows; ++i) {
            if (i % 100 == 99) {
                continue;
            }
            if (predicate(val)) {
                result.push_back(i);
            }
            if (i % 3 == 2) {
                ++val;
            }
        }
        return result;
    };

    // Read back with different read-buffer-size configurations:
    //   ""     -> no buffer (original path)
    //   "128"  -> very small buffer (smaller than block size 256)
    //   "512"  -> moderate buffer (larger than block size)
    //   "64KB" -> large buffer
    //   "1MB"  -> very large buffer (likely covers entire file)
    std::vector<std::string> buffer_sizes = {"", "128", "512", "64KB", "1MB"};

    for (const auto& buf_size : buffer_sizes) {
        std::map<std::string, std::string> read_options = {
            {BtreeDefs::kBtreeIndexBlockSize, "256"}, {BtreeDefs::kBtreeIndexCacheSize, "1024"}};
        if (!buf_size.empty()) {
            read_options[BtreeDefs::kBtreeIndexReadBufferSize] = buf_size;
        }
        ASSERT_OK_AND_ASSIGN(auto read_indexer, BTreeGlobalIndexer::Create(read_options));

        auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
        c_schema = CreateArrowSchema(field);
        ASSERT_OK_AND_ASSIGN(auto reader,
                             read_indexer->CreateReader(c_schema.get(), file_reader, metas, pool_));

        // check input stream and read buffer size
        {
            auto lazy_reader = std::dynamic_pointer_cast<LazyFilteredBTreeReader>(reader);
            ASSERT_TRUE(lazy_reader);
            ASSERT_OK_AND_ASSIGN(auto tmp_reader, lazy_reader->GetOrCreateReader(metas[0]));
            auto btree_reader = std::dynamic_pointer_cast<BTreeGlobalIndexReader>(tmp_reader);
            ASSERT_TRUE(btree_reader);
            auto input_stream = btree_reader->sst_file_reader_->block_cache_->in_;
            auto buffered_stream = std::dynamic_pointer_cast<BufferedInputStream>(input_stream);
            if (!buf_size.empty()) {
                ASSERT_TRUE(buffered_stream);
                ASSERT_OK_AND_ASSIGN(int32_t expected_buffer_size,
                                     MemorySize::ParseBytes(buf_size));
                ASSERT_EQ(buffered_stream->buffer_size_, expected_buffer_size);
            } else {
                ASSERT_FALSE(buffered_stream);
            }
        }

        // --- VisitIsNull ---
        {
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
            CheckResult(result, null_row_ids);
        }

        // --- VisitIsNotNull ---
        {
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
            CheckResult(result, non_null_row_ids);
        }

        // --- VisitEqual for value 0 ---
        {
            Literal lit_0(0);
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_0));
            CheckResult(result, collect_rows([](int32_t v) { return v == 0; }));
        }

        // --- VisitEqual for a value in the middle ---
        {
            Literal lit_100(100);
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_100));
            CheckResult(result, collect_rows([](int32_t v) { return v == 100; }));
        }

        // --- VisitEqual for non-existent value ---
        {
            Literal lit_neg(static_cast<int32_t>(-1));
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(lit_neg));
            CheckResult(result, {});
        }

        // --- VisitLessThan for value 5 ---
        {
            Literal lit_5(5);
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(lit_5));
            CheckResult(result, collect_rows([](int32_t v) { return v < 5; }));
        }

        // --- VisitGreaterOrEqual for a high value near max ---
        {
            int32_t max_val =
                static_cast<int32_t>(collect_rows([](int32_t) { return true; }).size()) / 3;
            int32_t threshold = max_val - 2;
            Literal lit_threshold(threshold);
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(lit_threshold));
            CheckResult(result, collect_rows([threshold](int32_t v) { return v >= threshold; }));
        }

        // --- VisitIn for scattered values ---
        {
            std::vector<Literal> in_literals = {Literal(0), Literal(500), Literal(10000)};
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
            CheckResult(result,
                        collect_rows([](int32_t v) { return v == 0 || v == 500 || v == 10000; }));
        }

        // --- VisitNotEqual for value 0 ---
        {
            Literal lit_0(0);
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(lit_0));
            CheckResult(result, collect_rows([](int32_t v) { return v != 0; }));
        }
    }
}

TEST_P(BTreeGlobalIndexIntegrationTest, CreateWriterWithNonStructSchema) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));

    // Export a plain int32 type (not struct) as ArrowSchema
    auto plain_type = arrow::int32();
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportType(*plain_type, &c_schema).ok());

    ASSERT_NOK_WITH_MSG(indexer->CreateWriter("int_field", &c_schema, file_writer, pool_),
                        "arrow schema must be struct type");
}

TEST_P(BTreeGlobalIndexIntegrationTest, CreateReaderWithMultiFieldSchema) {
    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);

    // Create a schema with two fields
    auto schema = arrow::schema(
        {arrow::field("field1", arrow::int32()), arrow::field("field2", arrow::int64())});
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));

    GlobalIndexIOMeta meta("fake_path", 100, nullptr);
    std::vector<GlobalIndexIOMeta> metas = {meta};

    ASSERT_NOK_WITH_MSG(indexer->CreateReader(c_schema.get(), file_reader, metas, pool_),
                        "supposed to have single field");
}

TEST_P(BTreeGlobalIndexIntegrationTest, CreateWriterWithMissingField) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto type = arrow::struct_({arrow::field("existing_field", arrow::int32())});
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(type);
    ASSERT_TRUE(struct_type);
    ASSERT_NOK_WITH_MSG(
        BTreeGlobalIndexWriter::Create("nonexistent_field", struct_type, file_writer, 4096,
                                       compression_factory_, pool_),
        "not in arrow_array when Create BTreeGlobalIndexWriter");
}

TEST_P(BTreeGlobalIndexIntegrationTest, AddBatchWithNullArray) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_));
    std::vector<int64_t> row_ids = {0, 1, 2};
    ASSERT_NOK_WITH_MSG(writer->AddBatch(nullptr, std::move(row_ids)),
                        "CheckRelativeRowIds failed: null c_arrow_array");
}

TEST_P(BTreeGlobalIndexIntegrationTest, AddBatchWithMismatchedRowIds) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_));
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [1],
        [2],
        [3]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

    // Provide wrong number of row_ids (2 instead of 3)
    std::vector<int64_t> row_ids = {0, 1};
    ASSERT_NOK_WITH_MSG(
        writer->AddBatch(&c_array, std::move(row_ids)),
        "relative_row_ids length 2 mismatch arrow_array length 3 in CheckRelativeRowIds");
}

TEST_P(BTreeGlobalIndexIntegrationTest, AddBatchWithNonMonotonicKeys) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_));
    // Write decreasing keys: 3, 2, 1
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
        [3],
        [2],
        [1]
    ])")
                     .ValueOrDie();

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids = {0, 1, 2};
    ASSERT_NOK_WITH_MSG(writer->AddBatch(&c_array, std::move(row_ids)),
                        "Users must keep written keys monotonically incremental");
}

TEST_P(BTreeGlobalIndexIntegrationTest, FinishWithEmptyData) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "128"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_));
    // Finish without adding any data
    ASSERT_NOK_WITH_MSG(writer->Finish(), "Should never write an empty btree index file");
}

TEST_P(BTreeGlobalIndexIntegrationTest, InvalidReadOptions) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());
    auto c_schema = CreateArrowSchema(field);

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "4096"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()},
                                                  {BtreeDefs::kBtreeIndexReadBufferSize, "4GB"}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));

    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    ASSERT_NOK_WITH_MSG(indexer->CreateReader(c_schema.get(), file_reader, /*files=*/{}, pool_),
                        "In BTreeGlobalIndexer::CreateReader: option btree-index.read-buffer-size "
                        "is 4GB, exceed INT_MAX or less than 0");
}

TEST_P(BTreeGlobalIndexIntegrationTest, TestIOException) {
    bool run_complete = false;
    auto io_hook = paimon::IOHook::GetInstance();
    for (size_t i = 0; i < 200; i++) {
        auto test_dir = UniqueTestDirectory::Create("local");
        ASSERT_TRUE(test_dir);
        auto local_fs = test_dir->GetFileSystem();
        auto local_base = test_dir->Str();
        paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, paimon::IOHook::Mode::RETURN_ERROR);

        auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(local_fs, local_base);
        auto field = arrow::field("int_field", arrow::int32());
        auto c_schema = CreateArrowSchema(field);

        std::map<std::string, std::string> options = {
            {BtreeDefs::kBtreeIndexBlockSize, "128"},
            {BtreeDefs::kBtreeIndexCompression, GetParam()}};
        ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));

        // write
        auto writer_result = indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_);
        CHECK_HOOK_STATUS(writer_result.status(), i);
        auto writer = std::move(writer_result).value();

        auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
            [1], [2], [null], [3], [4], [5]
        ])")
                         .ValueOrDie();
        ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        std::vector<int64_t> row_ids = {0, 1, 2, 3, 4, 5};

        CHECK_HOOK_STATUS(writer->AddBatch(&c_array, std::move(row_ids)), i);
        auto finish_result = writer->Finish();
        CHECK_HOOK_STATUS(finish_result.status(), i);
        auto metas = std::move(finish_result).value();

        // read
        auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(local_fs, local_base);
        c_schema = CreateArrowSchema(field);
        auto reader_result = indexer->CreateReader(c_schema.get(), file_reader, metas, pool_);
        CHECK_HOOK_STATUS(reader_result.status(), i);
        auto reader = std::move(reader_result).value();

        auto equal_result = reader->VisitEqual(Literal(3));
        CHECK_HOOK_STATUS(equal_result.status(), i);

        auto typed_result =
            std::dynamic_pointer_cast<BitmapGlobalIndexResult>(equal_result.value());
        ASSERT_TRUE(typed_result);
        auto bitmap_result = typed_result->GetBitmap();
        CHECK_HOOK_STATUS(bitmap_result.status(), i);
        ASSERT_TRUE(bitmap_result.value());
        ASSERT_EQ(*bitmap_result.value(), RoaringBitmap64::From({3}));

        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

// Multiple files with BTreeFileMetaSelector filtering
TEST_P(BTreeGlobalIndexIntegrationTest, WriteAndReadMultiFilesWithMetaSelector) {
    auto file_writer = std::make_shared<FakeGlobalIndexFileWriter>(fs_, base_path_);
    auto field = arrow::field("int_field", arrow::int32());

    std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "4096"},
                                                  {BtreeDefs::kBtreeIndexCompression, GetParam()}};
    ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));

    // Write 3 separate btree index files with non-overlapping key ranges:
    //
    // file0: keys [1, 2, null, 5], row_ids [0, 1, 2, 3]
    //        range [1, 5], has_null=true
    //
    // file1: keys [10, 12, 15], row_ids [4, 5, 6]
    //        range [10, 15], has_null=false
    //
    // file2: keys [20, null, 25], row_ids [7, 8, 9]
    //        range [20, 25], has_null=true
    std::vector<GlobalIndexIOMeta> all_metas;

    auto write_file = [&](const std::string& json_data, const std::vector<int64_t>& row_ids) {
        auto c_schema = CreateArrowSchema(field);
        ASSERT_OK_AND_ASSIGN(
            auto writer, indexer->CreateWriter("int_field", c_schema.get(), file_writer, pool_));
        auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), json_data)
                         .ValueOrDie();
        ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        ASSERT_OK(writer->AddBatch(&c_array, std::vector<int64_t>(row_ids)));
        ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
        ASSERT_EQ(metas.size(), 1);
        all_metas.push_back(metas[0]);
    };

    write_file(R"([[1],[2],[null],[5]])", {0, 1, 2, 3});
    write_file(R"([[10],[12],[15]])", {4, 5, 6});
    write_file(R"([[20],[null],[25]])", {7, 8, 9});

    ASSERT_EQ(all_metas.size(), 3);

    // Create reader over all 3 files (internally uses LazyFilteredBTreeReader +
    // BTreeFileMetaSelector)
    auto file_reader = std::make_shared<FakeGlobalIndexFileReader>(fs_, base_path_);
    auto c_schema = CreateArrowSchema(field);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         indexer->CreateReader(c_schema.get(), file_reader, all_metas, pool_));

    // --- VisitEqual: key=12 -> only file1 is selected by meta selector -> row 5
    {
        Literal literal_12(12);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_12));
        CheckResult(result, {5});
    }

    // --- VisitEqual: key=1 -> only file0 selected -> row 0
    {
        Literal literal_1(1);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_1));
        CheckResult(result, {0});
    }

    // --- VisitEqual: key=25 -> only file2 selected -> row 9
    {
        Literal literal_25(25);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_25));
        CheckResult(result, {9});
    }

    // --- VisitEqual: key=8 -> between file0 and file1 ranges, no file matches -> empty
    {
        Literal literal_8(8);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_8));
        CheckResult(result, {});
    }

    // --- VisitLessThan: key < 10 -> only file0 selected -> non-null rows 0,1,3
    {
        Literal literal_10(10);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(literal_10));
        CheckResult(result, {0, 1, 3});
    }

    // --- VisitGreaterThan: key > 15 -> only file2 selected -> non-null rows 7,9
    {
        Literal literal_15(15);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(literal_15));
        CheckResult(result, {7, 9});
    }

    // --- VisitGreaterOrEqual: key >= 5 -> file0 (key=5) + file1 + file2 -> rows 3,4,5,6,7,9
    {
        Literal literal_5(5);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(literal_5));
        CheckResult(result, {3, 4, 5, 6, 7, 9});
    }

    // --- VisitLessOrEqual: key <= 2 -> only file0 selected -> rows 0,1
    {
        Literal literal_2(2);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessOrEqual(literal_2));
        CheckResult(result, {0, 1});
    }

    // --- VisitIn: IN(2, 15, 25) -> file0 (key=2) + file1 (key=15) + file2 (key=25) -> rows 1,6,9
    {
        std::vector<Literal> literals = {Literal(2), Literal(15), Literal(25)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(literals));
        CheckResult(result, {1, 6, 9});
    }

    // --- VisitIn: IN(8, 17) -> no file contains these keys -> empty
    {
        std::vector<Literal> literals = {Literal(8), Literal(17)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(literals));
        CheckResult(result, {});
    }

    // --- VisitNotEqual: key != 10 -> all files selected, all non-null rows except key=10
    //     non-null: file0 {0,1,3} + file1 {4,5,6} + file2 {7,9}
    //     minus key=10 (row 4) -> {0,1,3,5,6,7,9}
    {
        Literal literal_10(10);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(literal_10));
        CheckResult(result, {0, 1, 3, 5, 6, 7, 9});
    }

    // --- VisitIsNull: file0 has null (row 2), file2 has null (row 8) -> {2, 8}
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        CheckResult(result, {2, 8});
    }

    // --- VisitIsNotNull: all non-null rows -> {0,1,3,4,5,6,7,9}
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        CheckResult(result, {0, 1, 3, 4, 5, 6, 7, 9});
    }

    // --- VisitNotIn: NOT IN(1, 12, 20) -> all files selected
    //     non-null rows: {0,1,3,4,5,6,7,9}
    //     minus key=1 (row 0), key=12 (row 5), key=20 (row 7) -> {1,3,4,6,9}
    {
        std::vector<Literal> literals = {Literal(1), Literal(12), Literal(20)};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(literals));
        CheckResult(result, {1, 3, 4, 6, 9});
    }
}

INSTANTIATE_TEST_SUITE_P(Compression, BTreeGlobalIndexIntegrationTest,
                         ::testing::ValuesIn(std::vector<std::string>({"none", "zstd", "lz4"})));

}  // namespace paimon::test
