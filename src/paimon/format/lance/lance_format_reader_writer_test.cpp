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

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/format/lance/lance_file_batch_reader.h"
#include "paimon/format/lance/lance_format_writer.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::lance::test {

class LanceFileReaderWriterTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    void WriteFile(const std::string& file_path,
                   const std::shared_ptr<arrow::ChunkedArray>& src_chunk_array,
                   const std::shared_ptr<arrow::Schema>& schema) const {
        ASSERT_OK_AND_ASSIGN(auto writer, LanceFormatWriter::Create(file_path, schema));
        for (auto& array : src_chunk_array->chunks()) {
            ArrowArray c_array;
            ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
            ASSERT_OK(writer->AddBatch(&c_array));
        }
        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
        auto fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(auto file_status, fs->GetFileStatus(file_path));
        ASSERT_GT(file_status->GetLen(), 0);
    }

    void CheckResultWithProjectionAndBitmap(
        const std::shared_ptr<arrow::ChunkedArray>& src_chunk_array,
        const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<int32_t>& read_field_indices,
        const std::vector<int32_t>& read_row_indices) {
        auto dir = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        std::string file_path = dir->Str() + "/test.lance";
        WriteFile(file_path, src_chunk_array, schema);

        // read with projection and bitmap
        auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::Concatenate(src_chunk_array->chunks()).ValueOrDie());
        ASSERT_TRUE(src_array);

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<LanceFileBatchReader> reader,
            LanceFileBatchReader::Create(file_path, /*batch_size=*/2, /*batch_readahead=*/2));

        // bitmap
        auto selection_bitmap = RoaringBitmap32::From(read_row_indices);
        // projection
        arrow::FieldVector read_fields;
        arrow::ArrayVector read_array;
        for (auto idx : read_field_indices) {
            read_fields.push_back(schema->field(idx));
            read_array.push_back(src_array->field(idx));
        }
        auto read_schema = arrow::schema(read_fields);
        ArrowSchema c_read_schema;
        ASSERT_TRUE(arrow::ExportSchema(*read_schema, &c_read_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_read_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/selection_bitmap));
        // generate expected array
        auto all_src_array = arrow::StructArray::Make(read_array, read_fields).ValueOrDie();
        arrow::ArrayVector expected_array_vec;
        for (auto index : read_row_indices) {
            expected_array_vec.push_back(all_src_array->Slice(index, 1));
        }
        auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array_vec);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             paimon::test::ReadResultCollector::CollectResult(reader.get()));
        ASSERT_TRUE(result->Equals(expected_chunk_array)) << result->ToString();
    }

    void CheckResult(const std::shared_ptr<arrow::ChunkedArray>& src_chunk_array,
                     const std::shared_ptr<arrow::Schema>& schema, bool enable_tz = false) const {
        auto dir = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        std::string file_path = dir->Str() + "/test.lance";
        WriteFile(file_path, src_chunk_array, schema);

        std::string timezone_str = enable_tz ? "Asia/Tokyo" : "Asia/Shanghai";
        paimon::test::TimezoneGuard tz_guard(timezone_str);

        // read
        for (int32_t batch_size : {1, 2, 4, 8, 1024}) {
            ASSERT_OK_AND_ASSIGN(
                std::unique_ptr<LanceFileBatchReader> reader,
                LanceFileBatchReader::Create(file_path, batch_size, /*batch_readahead=*/2));
            ASSERT_OK_AND_ASSIGN(auto c_schema, reader->GetFileSchema());
            auto file_schema = arrow::ImportSchema(c_schema.get()).ValueOrDie();
            ASSERT_TRUE(file_schema->Equals(schema));
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                                 paimon::test::ReadResultCollector::CollectResult(reader.get()));
            ASSERT_TRUE(result->Equals(src_chunk_array)) << result->ToString();
            ASSERT_OK_AND_ASSIGN(uint64_t num_rows, reader->GetNumberOfRows());
            ASSERT_EQ(num_rows, src_chunk_array->length());
        }
    }
};

TEST_F(LanceFileReaderWriterTest, TestSimple) {
    arrow::FieldVector fields = {arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::utf8())};
    auto schema = arrow::schema(fields);
    auto array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [1, "Hello"],
        [2, "World"],
        [3, "apple"]
    ])")
            .ValueOrDie());
    auto array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [4, "Alice"],
        [5, "Bob"],
        [6, "Lucy"]
    ])")
            .ValueOrDie());
    auto src_chunk_array =
        std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector({array1, array2}));
    CheckResult(src_chunk_array, schema);

    std::vector<std::vector<int32_t>> field_ids_vec = {{0, 1}, {0}, {1}};
    std::vector<std::vector<int32_t>> row_ids_vec = {{1, 3}, {1, 2, 3, 4}, {0, 2},
                                                     {4, 5}, {0, 2, 4},    {0, 1, 2, 3, 4, 5}};
    for (const auto& field_ids : field_ids_vec) {
        for (const auto& row_ids : row_ids_vec) {
            CheckResultWithProjectionAndBitmap(src_chunk_array, schema, field_ids, row_ids);
        }
    }
}
TEST_F(LanceFileReaderWriterTest, TestPrimitive) {
    arrow::FieldVector fields = {arrow::field("f1", arrow::int8()),
                                 arrow::field("f2", arrow::int16()),
                                 arrow::field("f3", arrow::int32()),
                                 arrow::field("f4", arrow::int64()),
                                 arrow::field("f5", arrow::float32()),
                                 arrow::field("f6", arrow::float64()),
                                 arrow::field("f7", arrow::utf8()),
                                 arrow::field("f8", arrow::binary()),
                                 arrow::field("f9", arrow::date32()),
                                 arrow::field("f10", arrow::timestamp(arrow::TimeUnit::NANO)),
                                 arrow::field("f11", arrow::decimal128(5, 2)),
                                 arrow::field("f12", arrow::boolean())};
    auto schema = arrow::schema(fields);
    auto array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [1, 11, 111, 1111, 1.1, 1.11, "Hello", "你好", 1234, "2033-05-18 03:33:20.0", "1.22", true],
        [2, 22, 222, 2222, 2.2, 2.22, "World", "世界", -1234, "1899-01-01 00:59:20.001001001", "2.22", false],
        [null, null, 0, null, null, 0, null, null, null, null, null, null]
    ])")
            .ValueOrDie());
    auto array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [3, 33, 333, 3333, 3.3, 3.33, "Hello", "你好", 3234, "2033-05-28 03:33:20.0", "3.22", true],
        [4, 44, 444, 4444, 4.4, 4.44, "Pineapple", "好吃", -1434, "2025-01-01 00:59:40.001001001", "4.44", false]
    ])")
            .ValueOrDie());
    auto src_chunk_array =
        std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector({array1, array2}));
    CheckResult(src_chunk_array, schema);

    std::vector<std::vector<int32_t>> field_ids_vec = {{2},
                                                       {0, 1, 3, 5, 8, 11},
                                                       {3, 4, 5, 7, 9},
                                                       {2, 4, 6, 8, 10},
                                                       {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}};
    std::vector<std::vector<int32_t>> row_ids_vec = {{1, 3}, {1, 2, 3, 4}, {0, 2},
                                                     {4},    {0, 2, 4},    {0, 1, 2, 3, 4}};
    for (const auto& field_ids : field_ids_vec) {
        for (const auto& row_ids : row_ids_vec) {
            CheckResultWithProjectionAndBitmap(src_chunk_array, schema, field_ids, row_ids);
        }
    }
}

TEST_F(LanceFileReaderWriterTest, TestNestedType) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::float32())),
        arrow::field("f1", arrow::struct_({arrow::field("sub_f0", arrow::boolean()),
                                           arrow::field("sub_f1", arrow::int64())}))};
    auto schema = arrow::schema(fields);
    auto array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [null, [true, 2]],
        [[0.1, 0.3], [true, 1]],
        [[1.1, 1.2], null]
    ])")
            .ValueOrDie());
    // V2.0 not support [[1.1, 1.2], null], null in struct
    auto array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [[1.1, 1.2], [false, 2222]],
        [[2.2, null], [true, 2]],
        [[2.2, 3.2], [null, 2]]
    ])")
            .ValueOrDie());
    auto src_chunk_array =
        std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector({array1, array2}));
    CheckResult(src_chunk_array, schema);

    std::vector<std::vector<int32_t>> field_ids_vec = {{0}, {1}, {0, 1}};
    std::vector<std::vector<int32_t>> row_ids_vec = {{1, 3}, {1, 2, 3, 4}, {0, 2},
                                                     {4, 5}, {0, 2, 4},    {0, 1, 2, 3, 4, 5}};
    for (const auto& field_ids : field_ids_vec) {
        for (const auto& row_ids : row_ids_vec) {
            CheckResultWithProjectionAndBitmap(src_chunk_array, schema, field_ids, row_ids);
        }
    }
}

TEST_F(LanceFileReaderWriterTest, TestBulkData) {
    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    arrow::FieldVector fields = {arrow::field("f0", arrow::boolean()),
                                 arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::int64()),
                                 arrow::field("f3", arrow::float64()),
                                 arrow::field("f4", arrow::utf8()),
                                 arrow::field("f5", arrow::timestamp(arrow::TimeUnit::NANO))};
    auto schema = arrow::schema(fields);
    int32_t write_batch_size = 1024;
    // TODO(xinyu.lxy): when turn_count reach 1000, read will return error: Failed to next batch,
    // Encountered internal error. Please file a bug report at
    // https://github.com/lancedb/lance/issues. Error decoding batch: LanceError(IO): output offsets
    // buffer too small for FSST decoder
    int32_t turn_count = 10;
    arrow::ArrayVector src_array_vec;
    for (int32_t turn = 0; turn < turn_count; turn++) {
        std::string data_str = "[";
        for (int32_t i = 0; i < write_batch_size; i++) {
            data_str.append("[");
            int32_t base_value = turn * write_batch_size + i;
            if (base_value % 3 == 0) {
                data_str.append("null,");
            } else if (base_value % 3 == 1) {
                data_str.append("true,");
            } else {
                data_str.append("false,");
            }
            data_str.append(std::to_string(base_value) + ",");
            data_str.append(std::to_string(base_value * 10) + ",");
            data_str.append(std::to_string(static_cast<double>(base_value) * 0.1) + ",");
            data_str.append("\"str_" + std::to_string(base_value) + "\",");
            Timestamp ts(base_value, i);
            data_str.append("\"" + ts.ToString() + "\"],");
        }
        data_str.pop_back();
        data_str.append("]");
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), data_str)
                .ValueOrDie());
        src_array_vec.push_back(array);
    }
    auto src_chunk_array = std::make_shared<arrow::ChunkedArray>(src_array_vec);
    CheckResult(src_chunk_array, schema);
    // test read with projection and bitmap
    auto generate_random_visits = [](int32_t count) -> std::vector<int32_t> {
        std::vector<int32_t> ret;
        // force add one item to ret
        ret.push_back(0);
        for (int32_t i = 1; i < count; i++) {
            int32_t visit = paimon::test::RandomNumber(0, 1);
            if (visit == 1) {
                ret.push_back(i);
            }
        }
        return ret;
    };
    auto field_ids = generate_random_visits(fields.size());
    auto row_ids = generate_random_visits(turn_count * write_batch_size);
    CheckResultWithProjectionAndBitmap(src_chunk_array, schema, field_ids, row_ids);
}

TEST_F(LanceFileReaderWriterTest, TestDictionary) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    int32_t dictionary_size = 300;
    std::vector<std::string> dictionary;
    for (int32_t i = 0; i < dictionary_size; i++) {
        dictionary.push_back(std::to_string(paimon::test::RandomNumber(0, 10000)) + "_" +
                             std::to_string(i));
    }
    int32_t write_batch_size = 4000;
    auto schema = arrow::schema(fields);
    std::string data_str = "[";
    for (int32_t i = 0; i < write_batch_size; i++) {
        data_str.append("[");
        data_str.append("\"" + dictionary[i % dictionary.size()] + "\"");
        data_str.append("],");
    }
    data_str.pop_back();
    data_str.append("]");
    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), data_str).ValueOrDie());
    arrow::ArrayVector src_array_vec = {array};
    auto src_chunk_array = std::make_shared<arrow::ChunkedArray>(src_array_vec);
    CheckResult(src_chunk_array, schema);
}

TEST_F(LanceFileReaderWriterTest, TestReachTargetSize) {
    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int64()), arrow::field("f3", arrow::float64()),
        arrow::field("f5", arrow::timestamp(arrow::TimeUnit::NANO))};
    auto schema = arrow::schema(fields);
    int32_t write_batch_size = 1024;
    int32_t turn_count = 2000;
    arrow::ArrayVector src_array_vec;
    for (int32_t turn = 0; turn < turn_count; turn++) {
        std::string data_str = "[";
        for (int32_t i = 0; i < write_batch_size; i++) {
            data_str.append("[");
            int32_t base_value = paimon::test::RandomNumber(0, 10000);
            if (base_value % 3 == 0) {
                data_str.append("null,");
            } else if (base_value % 3 == 1) {
                data_str.append("true,");
            } else {
                data_str.append("false,");
            }
            data_str.append(std::to_string(base_value) + ",");
            data_str.append(std::to_string(static_cast<int64_t>(base_value) * 10) + ",");
            data_str.append(std::to_string(static_cast<double>(base_value) * 0.1) + ",");
            Timestamp ts(base_value, i);
            data_str.append("\"" + ts.ToString() + "\"],");
        }
        data_str.pop_back();
        data_str.append("]");
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), data_str)
                .ValueOrDie());
        src_array_vec.push_back(array);
    }
    auto src_chunk_array = std::make_shared<arrow::ChunkedArray>(src_array_vec);
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string file_path = dir->Str() + "/test.lance";

    ASSERT_OK_AND_ASSIGN(auto writer, LanceFormatWriter::Create(file_path, schema));
    bool reach_targe_size = false;
    for (auto& array : src_chunk_array->chunks()) {
        ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        ASSERT_OK(writer->AddBatch(&c_array));
        ASSERT_OK_AND_ASSIGN(reach_targe_size, writer->ReachTargetSize(/*suggested_check=*/true,
                                                                       /*target_size=*/4096));
    }
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());
    // test reach targe size
    ASSERT_TRUE(reach_targe_size);
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto file_status, fs->GetFileStatus(file_path));
    ASSERT_GT(file_status->GetLen(), 0);
}

TEST_F(LanceFileReaderWriterTest, TestTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("f2", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("f3", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f5", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("f6", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("f7", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("f8", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
["1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
["1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])")
            .ValueOrDie());
    auto src_chunk_array = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector({array}));
    CheckResult(src_chunk_array, schema, /*enable_tz=*/true);
    CheckResult(src_chunk_array, schema, /*enable_tz=*/false);
}

}  // namespace paimon::lance::test
