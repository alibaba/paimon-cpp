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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/mergetree/external_sort_buffer.h"
#include "paimon/core/mergetree/in_memory_sort_buffer.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/data_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

struct ReaderResult {
    std::string key;
    int32_t sequence_field;
    int32_t value_field;
    int64_t sequence_number;
    const RowKind* row_kind;
};

}  // namespace

class SortBufferTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        test_dir_ = UniqueTestDirectory::Create();
        io_manager_ = std::make_shared<IOManager>(test_dir_->Str(), test_dir_->GetFileSystem());

        std::vector<DataField> value_fields = {DataField(0, arrow::field("key", arrow::utf8())),
                                               DataField(1, arrow::field("seq", arrow::int32())),
                                               DataField(2, arrow::field("val", arrow::int32()))};
        value_type_ = DataField::ConvertDataFieldsToArrowStructType(value_fields);
        value_schema_ = DataField::ConvertDataFieldsToArrowSchema(value_fields);
        primary_keys_ = {"key"};
        sequence_fields_ = {"seq"};

        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, value_schema_, /*partition_keys=*/{},
                                primary_keys_, /*options=*/{{Options::BUCKET, "1"}}));
        data_generator_ = std::make_shared<DataGenerator>(table_schema, pool_);

        ASSERT_OK_AND_ASSIGN(key_comparator_, FieldsComparator::Create(
                                                  value_fields, {0}, /*is_ascending_order=*/true));
        ASSERT_OK_AND_ASSIGN(
            sequence_comparator_,
            FieldsComparator::Create(value_fields, {1}, /*is_ascending_order=*/true));
    }

 protected:
    void CheckResult(std::vector<std::unique_ptr<paimon::KeyValueRecordReader>>&& readers,
                     const std::vector<std::vector<ReaderResult>>& expected, bool need_sort) const {
        ASSERT_EQ(readers.size(), expected.size());
        std::vector<std::vector<ReaderResult>> actual;
        for (size_t i = 0; i < expected.size(); ++i) {
            ASSERT_OK_AND_ASSIGN(auto rows, CollectRows(readers[i].get()));
            actual.push_back(std::move(rows));
        }
        if (need_sort) {
            std::sort(actual.begin(), actual.end(),
                      [](const auto& a, const auto& b) { return a.size() < b.size(); });
        }
        for (size_t i = 0; i < expected.size(); ++i) {
            AssertRows(actual[i], expected[i]);
        }
        CloseReaders(readers);
    }

    BinaryRow MakeRow(const RowKind* kind, const std::string& key, int32_t seq, int32_t val) {
        return BinaryRowGenerator::GenerateRow(kind, {key, seq, val}, pool_.get());
    }
    std::unique_ptr<paimon::RecordBatch> MakeBatch(const std::vector<BinaryRow>& input_rows) {
        EXPECT_OK_AND_ASSIGN(auto batches,
                             data_generator_->SplitArrayByPartitionAndBucket(input_rows));
        EXPECT_EQ(1, batches.size());
        return std::move(batches[0]);
    }

    Result<std::vector<ReaderResult>> CollectRows(KeyValueRecordReader* reader) const {
        std::vector<ReaderResult> rows;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<KeyValueRecordReader::Iterator> iterator,
                                   reader->NextBatch());
            if (iterator == nullptr) {
                break;
            }
            while (iterator->HasNext()) {
                PAIMON_ASSIGN_OR_RAISE(KeyValue key_value, iterator->Next());
                rows.push_back(ReaderResult{std::string(key_value.key->GetStringView(0)),
                                            key_value.value->GetInt(1), key_value.value->GetInt(2),
                                            key_value.sequence_number, key_value.value_kind});
            }
        }
        return rows;
    }

    Result<std::unique_ptr<ExternalSortBuffer>> CreateExternalSortBuffer(
        int64_t last_sequence_number, uint64_t write_buffer_size) const {
        PAIMON_ASSIGN_OR_RAISE(
            CoreOptions options,
            CoreOptions::FromMap({{Options::SPILL_COMPRESSION, "uncompressed"}}));
        auto in_memory_buffer = std::make_unique<InMemorySortBuffer>(
            last_sequence_number, value_type_, primary_keys_, sequence_fields_,
            /*sequence_fields_ascending=*/true, key_comparator_, write_buffer_size, pool_);
        return ExternalSortBuffer::Create(std::move(in_memory_buffer), value_schema_, primary_keys_,
                                          key_comparator_, sequence_comparator_, options,
                                          io_manager_, pool_);
    }

    void AssertRows(const std::vector<ReaderResult>& actual,
                    const std::vector<ReaderResult>& expected) const {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t index = 0; index < expected.size(); ++index) {
            ASSERT_EQ(actual[index].key, expected[index].key);
            ASSERT_EQ(actual[index].sequence_field, expected[index].sequence_field);
            ASSERT_EQ(actual[index].value_field, expected[index].value_field);
            ASSERT_EQ(actual[index].sequence_number, expected[index].sequence_number);
            ASSERT_EQ(actual[index].row_kind, expected[index].row_kind);
        }
    }

    void CloseReaders(const std::vector<std::unique_ptr<KeyValueRecordReader>>& readers) const {
        for (const auto& reader : readers) {
            reader->Close();
        }
    }

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<DataGenerator> data_generator_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<IOManager> io_manager_;
    std::shared_ptr<arrow::DataType> value_type_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::vector<std::string> primary_keys_;
    std::vector<std::string> sequence_fields_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<FieldsComparator> sequence_comparator_;
};

TEST_F(SortBufferTest, TestInMemorySortBufferEstimateMemoryUse) {
    {
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          ["Lucy", 20, 1, 14.1],
          ["Paul", 20, 1, null],
          ["Alice", 10, 0, 13.1]
        ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(int64_t memory_use, InMemorySortBuffer::EstimateMemoryUse(array));
        int64_t expected_memory_use =
            1 + (13 + 3 * 4 + 1) + (3 * 4 + 1) + (3 * 4 + 1) + (3 * 8 + 1);
        ASSERT_EQ(memory_use, expected_memory_use);
    }
    {
        arrow::FieldVector fields = {arrow::field("v0", arrow::boolean()),
                                     arrow::field("v1", arrow::int8()),
                                     arrow::field("v2", arrow::int16()),
                                     arrow::field("v3", arrow::int32()),
                                     arrow::field("v4", arrow::int64()),
                                     arrow::field("v5", arrow::float32()),
                                     arrow::field("v6", arrow::float64()),
                                     arrow::field("v7", arrow::date32()),
                                     arrow::field("v8", arrow::timestamp(arrow::TimeUnit::NANO)),
                                     arrow::field("v9", arrow::decimal128(30, 20)),
                                     arrow::field("v10", arrow::utf8()),
                                     arrow::field("v11", arrow::binary())};

        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [true, 10, 200, 65536, 123456789, 0.0, 0.0, 2000, -86399999999500, "2134.48690000000000000009", "difference", "Alice"],
        [false, -128, -32768, -2147483648, -9223372036854775808, -3.4028235E38, -1.7976931348623157E308, -719528, -9223372036854775808, "-999999999999999999.99999999999999999999", "Alice", "Two"],
        [true, 127, 32767, 2147483647, 9223372036854775807, 3.4028235E38, 1.7976931348623157E308, 2932896, 9223372036854775807, "999999999999999999.99999999999999999999", "Alice", "made"],
        [true, 0, 0, 0, 0, 1.4E-45, 4.9E-324, 0, 0, "0.00000000000000000000", "Alice", "wood"]
])")
                .ValueOrDie());
        ASSERT_OK_AND_ASSIGN(int64_t memory_use, InMemorySortBuffer::EstimateMemoryUse(array));
        int64_t expected_memory_use = 1 + (4 + 1) + (4 + 1) + (2 * 4 + 1) + (4 * 4 + 1) +
                                      (8 * 4 + 1) + (4 * 4 + 1) + (8 * 4 + 1) + (4 * 4 + 1) +
                                      (8 * 4 + 1) + (4 * 16 + 1) + (25 + 4 * 4 + 1) +
                                      (16 + 4 * 4 + 1);
        ASSERT_EQ(memory_use, expected_memory_use);
    }
    {
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::list(arrow::int32())),
            arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
            arrow::field("f2", arrow::struct_({arrow::field("sub1", arrow::int64()),
                                               arrow::field("sub2", arrow::float64()),
                                               arrow::field("sub3", arrow::boolean())})),
        };
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]]
    ])")
                .ValueOrDie());
        ASSERT_OK_AND_ASSIGN(int64_t memory_use, InMemorySortBuffer::EstimateMemoryUse(array));
        int64_t list_mem = 1 + (4 * 6 + 1);
        int64_t map_mem = 1 + (33 + 4 * 7 + 1) + (8 * 7 + 1);
        int64_t struct_mem = 1 + (8 * 3 + 1) + (8 * 3 + 1) + (1 * 3 + 1);
        int64_t expected_memory_use = 1 + list_mem + map_mem + struct_mem;
        ASSERT_EQ(memory_use, expected_memory_use);
    }
}

TEST_F(SortBufferTest, TestInMemorySortBufferSimple) {
    InMemorySortBuffer buffer(/*last_sequence_number=*/9, value_type_, primary_keys_,
                              sequence_fields_, /*sequence_fields_ascending=*/true, key_comparator_,
                              /*write_buffer_size=*/1024 * 1024, pool_);

    std::vector<BinaryRow> input_rows;
    input_rows.push_back(MakeRow(RowKind::Insert(), "b", 2, 200));
    input_rows.push_back(MakeRow(RowKind::Delete(), "a", 3, 300));
    input_rows.push_back(MakeRow(RowKind::UpdateAfter(), "a", 1, 100));
    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota, buffer.Write(MakeBatch(input_rows)));
    ASSERT_TRUE(has_remaining_quota);

    input_rows.clear();
    input_rows.push_back(MakeRow(RowKind::UpdateBefore(), "c", 1, 400));
    input_rows.push_back(MakeRow(RowKind::Insert(), "b", 1, 150));
    ASSERT_OK_AND_ASSIGN(has_remaining_quota, buffer.Write(MakeBatch(input_rows)));
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_TRUE(buffer.HasData());
    ASSERT_GT(buffer.GetMemorySize(), 0);

    ASSERT_OK_AND_ASSIGN(auto readers, buffer.CreateReaders());
    ASSERT_TRUE(buffer.HasData());
    ASSERT_GT(buffer.GetMemorySize(), 0);

    CheckResult(
        std::move(readers),
        {{{"a", 1, 100, 12, RowKind::UpdateAfter()},
          {"a", 3, 300, 11, RowKind::Delete()},
          {"b", 2, 200, 10, RowKind::Insert()}},
         {{"b", 1, 150, 14, RowKind::Insert()}, {"c", 1, 400, 13, RowKind::UpdateBefore()}}},
        /*need_sort=*/false);

    buffer.Clear();
    ASSERT_FALSE(buffer.HasData());
    ASSERT_EQ(buffer.GetMemorySize(), 0);
}

TEST_F(SortBufferTest, TestExternalSortBufferWithInMemoryDataAndNoSpill) {
    ASSERT_OK_AND_ASSIGN(auto buffer, CreateExternalSortBuffer(/*last_sequence_number=*/4,
                                                               /*write_buffer_size=*/1024 * 1024));
    std::vector<BinaryRow> input_rows;
    input_rows.push_back(MakeRow(RowKind::Delete(), "b", 2, 200));
    input_rows.push_back(MakeRow(RowKind::UpdateAfter(), "a", 3, 300));
    input_rows.push_back(MakeRow(RowKind::Insert(), "a", 1, 100));
    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota, buffer->Write(MakeBatch(input_rows)));
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_TRUE(buffer->HasData());
    ASSERT_GT(buffer->GetMemorySize(), 0);

    ASSERT_OK_AND_ASSIGN(auto readers, buffer->CreateReaders());
    ASSERT_TRUE(buffer->HasData());
    ASSERT_GT(buffer->GetMemorySize(), 0);

    CheckResult(std::move(readers),
                {{{"a", 1, 100, 7, RowKind::Insert()},
                  {"a", 3, 300, 6, RowKind::UpdateAfter()},
                  {"b", 2, 200, 5, RowKind::Delete()}}},
                /*need_sort=*/false);

    buffer->Clear();
    ASSERT_FALSE(buffer->HasData());
    ASSERT_EQ(buffer->GetMemorySize(), 0);
}

TEST_F(SortBufferTest, TestExternalSortBufferWithSpilledDataAndInMemoryData) {
    // the write buffer size limit 35 bytes is larger than 2 rows but smaller than 3 rows.
    ASSERT_OK_AND_ASSIGN(auto buffer, CreateExternalSortBuffer(/*last_sequence_number=*/19,
                                                               /*write_buffer_size=*/35));

    // in memory data
    std::vector<BinaryRow> input_rows;
    input_rows.push_back(MakeRow(RowKind::Insert(), "b", 1, 200));
    input_rows.push_back(MakeRow(RowKind::Delete(), "b", 2, 200));
    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota, buffer->Write(MakeBatch(input_rows)));
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_TRUE(buffer->HasData());
    ASSERT_GT(buffer->GetMemorySize(), 0);

    // spill file 1 (with above in memory data)
    input_rows.clear();
    input_rows.push_back(MakeRow(RowKind::UpdateAfter(), "a", 3, 300));
    ASSERT_OK_AND_ASSIGN(has_remaining_quota, buffer->Write(MakeBatch(input_rows)));
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_TRUE(buffer->HasData());
    ASSERT_EQ(buffer->GetMemorySize(), 0);

    // spill file 2
    input_rows.clear();
    input_rows.push_back(MakeRow(RowKind::Insert(), "c", 5, 500));
    input_rows.push_back(MakeRow(RowKind::Insert(), "c", 4, 400));
    input_rows.push_back(MakeRow(RowKind::UpdateBefore(), "a", 1, 100));
    input_rows.push_back(MakeRow(RowKind::Insert(), "b", 1, 150));
    ASSERT_OK_AND_ASSIGN(has_remaining_quota, buffer->Write(MakeBatch(input_rows)));
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_TRUE(buffer->HasData());
    ASSERT_EQ(buffer->GetMemorySize(), 0);

    // in memory data
    input_rows.clear();
    input_rows.push_back(MakeRow(RowKind::Insert(), "c", 4, 400));
    input_rows.push_back(MakeRow(RowKind::UpdateBefore(), "a", 1, 100));
    ASSERT_OK_AND_ASSIGN(has_remaining_quota, buffer->Write(MakeBatch(input_rows)));
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_TRUE(buffer->HasData());
    ASSERT_GT(buffer->GetMemorySize(), 0);

    ASSERT_OK_AND_ASSIGN(auto readers, buffer->CreateReaders());
    ASSERT_TRUE(buffer->HasData());
    ASSERT_GT(buffer->GetMemorySize(), 0);

    CheckResult(std::move(readers),
                {{{"a", 1, 100, 28, RowKind::UpdateBefore()}, {"c", 4, 400, 27, RowKind::Insert()}},
                 {{"a", 3, 300, 22, RowKind::UpdateAfter()},
                  {"b", 1, 200, 20, RowKind::Insert()},
                  {"b", 2, 200, 21, RowKind::Delete()}},
                 {{"a", 1, 100, 25, RowKind::UpdateBefore()},
                  {"b", 1, 150, 26, RowKind::Insert()},
                  {"c", 4, 400, 24, RowKind::Insert()},
                  {"c", 5, 500, 23, RowKind::Insert()}}},
                /*need_sort=*/true);

    buffer->Clear();
    ASSERT_FALSE(buffer->HasData());
    ASSERT_EQ(buffer->GetMemorySize(), 0);
}

}  // namespace paimon::test
