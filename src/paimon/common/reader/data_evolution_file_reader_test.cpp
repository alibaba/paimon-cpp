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

#include "paimon/common/reader/data_evolution_file_reader.h"

#include <map>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "arrow/util/range.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {

class DataEvolutionFileReaderTest : public ::testing::Test,
                                    public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
    }

    void TearDown() override {
        pool_.reset();
    }

    void CheckResult(const arrow::ArrayVector& src_array_vec,
                     const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::vector<int32_t>& reader_offsets,
                     const std::vector<int32_t>& field_offsets,
                     const std::shared_ptr<arrow::Array>& expected_array,
                     const std::optional<RoaringBitmap32>& selection_bitmap = std::nullopt) const {
        for (auto batch_size : arrow::internal::Iota(1, 10)) {
            int32_t total_row_count = 0;
            std::vector<std::unique_ptr<BatchReader>> readers;
            for (const auto& array : src_array_vec) {
                if (array == nullptr) {
                    // simulate no fields read from current reader
                    readers.push_back(nullptr);
                    continue;
                }
                total_row_count += array->length();
                std::unique_ptr<MockFileBatchReader> file_batch_reader;
                if (selection_bitmap) {
                    file_batch_reader = std::make_unique<MockFileBatchReader>(
                        array, array->type(), selection_bitmap.value(), batch_size);
                } else {
                    file_batch_reader =
                        std::make_unique<MockFileBatchReader>(array, array->type(), batch_size);
                }
                auto enable_randomize_batch_size = GetParam();
                file_batch_reader->EnableRandomizeBatchSize(enable_randomize_batch_size);
                readers.push_back(std::move(file_batch_reader));
            }
            ASSERT_OK_AND_ASSIGN(
                auto data_evolution_file_reader,
                DataEvolutionFileReader::Create(std::move(readers), read_schema, batch_size,
                                                reader_offsets, field_offsets, arrow_pool_));
            ASSERT_EQ(arrow_pool_.get(), data_evolution_file_reader->arrow_pool_.get());
            // check metrics, data_evolution_file_reader collects all row of each
            // MockFileBatchReader
            auto metrics = data_evolution_file_reader->GetReaderMetrics();
            ASSERT_EQ(metrics->ToString(),
                      "{\"mock.number.of.rows\":" + std::to_string(total_row_count) + "}");

            // check result array
            ASSERT_OK_AND_ASSIGN(
                auto result_array,
                paimon::test::ReadResultCollector::CollectResult(data_evolution_file_reader.get()));
            data_evolution_file_reader->Close();
            auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
            ASSERT_TRUE(result_array->Equals(expected_chunk_array));
        }
    }

    void CheckNextBatchForSingleReader(int32_t inner_batch_size, int32_t read_batch_size,
                                       const std::shared_ptr<arrow::Array>& src_array,
                                       const std::optional<RoaringBitmap32>& selection_bitmap,
                                       const std::shared_ptr<arrow::Array>& expected_array) const {
        std::unique_ptr<MockFileBatchReader> file_batch_reader;
        if (selection_bitmap) {
            file_batch_reader = std::make_unique<MockFileBatchReader>(
                src_array, src_array->type(), selection_bitmap.value(), inner_batch_size);
        } else {
            file_batch_reader = std::make_unique<MockFileBatchReader>(src_array, src_array->type(),
                                                                      inner_batch_size);
        }
        auto enable_randomize_batch_size = GetParam();
        file_batch_reader->EnableRandomizeBatchSize(enable_randomize_batch_size);
        std::vector<std::unique_ptr<BatchReader>> readers;
        readers.push_back(std::move(file_batch_reader));
        DataEvolutionFileReader fake_data_evolution_reader(
            std::move(readers), /*read_schema=*/arrow::schema({}), read_batch_size,
            /*reader_offsets=*/{}, /*field_offsets=*/{}, arrow_pool_);
        ASSERT_EQ(arrow_pool_.get(), fake_data_evolution_reader.arrow_pool_.get());
        arrow::ArrayVector result_array_vec;
        while (true) {
            ASSERT_OK_AND_ASSIGN(auto result_array,
                                 fake_data_evolution_reader.NextBatchForSingleReader(0));
            if (result_array == nullptr) {
                break;
            }
            result_array_vec.push_back(result_array);
        }
        ASSERT_EQ(result_array_vec.size(),
                  std::ceil(static_cast<double>(expected_array->length()) / read_batch_size));
        // except for last batch, the length each array is expected to be aligned to read_batch_size
        for (size_t i = 0; i < result_array_vec.size() - 1; i++) {
            ASSERT_EQ(result_array_vec[i]->length(), read_batch_size);
        }
        if (expected_array->length() % read_batch_size == 0) {
            ASSERT_EQ(result_array_vec.back()->length(), read_batch_size);
        } else {
            ASSERT_EQ(result_array_vec.back()->length(),
                      expected_array->length() % read_batch_size);
        }
        auto result_chunk_array = std::make_shared<arrow::ChunkedArray>(result_array_vec);
        auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
        ASSERT_TRUE(result_chunk_array->Equals(expected_chunk_array));
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

TEST_F(DataEvolutionFileReaderTest, TestInvalid) {
    {
        arrow::FieldVector read_fields;
        auto read_schema = arrow::schema(read_fields);
        ASSERT_NOK_WITH_MSG(DataEvolutionFileReader::Create({}, read_schema, /*read_batch_size=*/10,
                                                            {}, {}, arrow_pool_),
                            "read schema must not be empty");
    }
    {
        arrow::FieldVector read_fields = {
            arrow::field("f0", arrow::int32()),
            arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::utf8()),
            arrow::field("f3", arrow::int32()),
        };
        auto read_schema = arrow::schema(read_fields);
        std::vector<int32_t> reader_offsets = {0, 0, 1};
        std::vector<int32_t> field_offsets = {0, 1, 0};
        ASSERT_NOK_WITH_MSG(
            DataEvolutionFileReader::Create({}, read_schema, /*read_batch_size=*/10, reader_offsets,
                                            field_offsets, arrow_pool_),
            "read schema, row offsets and field offsets must have the same size");
    }
    {
        std::vector<std::unique_ptr<BatchReader>> readers;
        readers.push_back(nullptr);

        arrow::FieldVector read_fields = {
            arrow::field("f0", arrow::int32()),
            arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::utf8()),
            arrow::field("f3", arrow::int32()),
        };
        auto read_schema = arrow::schema(read_fields);
        std::vector<int32_t> reader_offsets = {0, 0, 1, 1};
        std::vector<int32_t> field_offsets = {0, 1, 1, 0};
        ASSERT_NOK_WITH_MSG(
            DataEvolutionFileReader::Create(std::move(readers), read_schema, /*read_batch_size=*/10,
                                            reader_offsets, field_offsets, arrow_pool_),
            "readers size is supposed to be more than 1");
    }
}

TEST_P(DataEvolutionFileReaderTest, TestNextBatchForSingleReader) {
    auto prepare_array = [](int64_t array_length) -> std::shared_ptr<arrow::Array> {
        auto array_builder = std::make_shared<arrow::Int32Builder>();
        for (int32_t i = 0; i < array_length; ++i) {
            EXPECT_TRUE(array_builder->Append(i).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(array_builder->Finish(&array).ok());
        return array;
    };
    auto prepare_array_with_bitmap =
        [](const RoaringBitmap32& bitmap) -> std::shared_ptr<arrow::Array> {
        auto array_builder = std::make_shared<arrow::Int32Builder>();
        for (auto iter = bitmap.Begin(); iter != bitmap.End(); ++iter) {
            EXPECT_TRUE(array_builder->Append(*iter).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(array_builder->Finish(&array).ok());
        return array;
    };
    {
        // src array length = 10, read batch size = 10
        auto src_array = prepare_array(10);
        for (int32_t inner_batch_size : arrow::internal::Iota(1, 10)) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/10, src_array,
                                          /*selection_bitmap=*/std::nullopt,
                                          /*expected_array=*/src_array);
        }
    }
    {
        // src array length = 10, read batch size = 6
        auto src_array = prepare_array(10);
        for (int32_t inner_batch_size : arrow::internal::Iota(1, 6)) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/6, src_array,
                                          /*selection_bitmap=*/std::nullopt,
                                          /*expected_array=*/src_array);
        }
    }
    {
        // src array length = 10, read batch size = 15
        auto src_array = prepare_array(10);
        for (int32_t inner_batch_size : arrow::internal::Iota(1, 15)) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/15, src_array,
                                          /*selection_bitmap=*/std::nullopt,
                                          /*expected_array=*/src_array);
        }
    }
    {
        // test bulk data, src array length = 10000, read batch size = 1024
        auto src_array = prepare_array(10000);
        for (int32_t inner_batch_size : {1, 2, 8, 16, 20, 100, 1024}) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/1024, src_array,
                                          /*selection_bitmap=*/std::nullopt,
                                          /*expected_array=*/src_array);
        }
    }
    {
        // src array length = 10, selection bitmap = {1, 3, 5}
        auto src_array = prepare_array(10);
        RoaringBitmap32 selected_bitmap = RoaringBitmap32::From({1, 3, 5});
        auto expected_array = prepare_array_with_bitmap(selected_bitmap);
        for (int32_t inner_batch_size : arrow::internal::Iota(1, 15)) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/15, src_array,
                                          selected_bitmap, expected_array);
        }
    }
    {
        // src array length = 10, selection all
        auto src_array = prepare_array(10);
        RoaringBitmap32 selected_bitmap = RoaringBitmap32::From({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        for (int32_t inner_batch_size : arrow::internal::Iota(1, 15)) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/15, src_array,
                                          selected_bitmap, /*expected_array=*/src_array);
        }
    }
    {
        // src array length = 10, selection first
        auto src_array = prepare_array(10);
        RoaringBitmap32 selected_bitmap = RoaringBitmap32::From({0});
        auto expected_array = prepare_array_with_bitmap(selected_bitmap);
        for (int32_t inner_batch_size : arrow::internal::Iota(1, 15)) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/15, src_array,
                                          selected_bitmap, expected_array);
        }
    }
    {
        // src array length = 10, selection first
        auto src_array = prepare_array(10);
        RoaringBitmap32 selected_bitmap = RoaringBitmap32::From({9});
        auto expected_array = prepare_array_with_bitmap(selected_bitmap);
        for (int32_t inner_batch_size : arrow::internal::Iota(1, 15)) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/15, src_array,
                                          selected_bitmap, expected_array);
        }
    }
    {
        // src array length = 10, selection consecutive positions
        auto src_array = prepare_array(10);
        RoaringBitmap32 selected_bitmap = RoaringBitmap32::From({2, 3, 4, 5});
        auto expected_array = prepare_array_with_bitmap(selected_bitmap);
        for (int32_t inner_batch_size : arrow::internal::Iota(1, 15)) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/15, src_array,
                                          selected_bitmap, expected_array);
        }
    }
    {
        auto src_array = prepare_array(10);
        RoaringBitmap32 selected_bitmap = RoaringBitmap32::From({0, 1, 2, 3, 4, 5, 7, 8, 9});
        auto expected_array = prepare_array_with_bitmap(selected_bitmap);
        // inner batch: [0, 1, 2, 3] | [4, 5] [7, 8] | [9]
        CheckNextBatchForSingleReader(/*inner_batch_size=*/4, /*read_batch_size=*/5, src_array,
                                      selected_bitmap, expected_array);
    }
    {
        // test bulk data, src array length = 10000, read batch size = 1024
        auto src_array = prepare_array(10000);
        RoaringBitmap32 selected_bitmap =
            RoaringBitmap32::From({0, 10, 1000, 2333, 4566, 7838, 8787, 9999});
        auto expected_array = prepare_array_with_bitmap(selected_bitmap);
        for (int32_t inner_batch_size : {1, 2, 8, 16, 20, 100, 1024}) {
            CheckNextBatchForSingleReader(inner_batch_size, /*read_batch_size=*/1024, src_array,
                                          selected_bitmap, expected_array);
        }
    }
}

TEST_P(DataEvolutionFileReaderTest, TestSimple) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),  arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::utf8()),  arrow::field("f5", arrow::int32()),
    };
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 2, 0, 1, 2, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[5], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114],
        [15, 115]
])")
                      .ValueOrDie();
    auto array2 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[4]}), R"([
        [20, "20"],
        [21, "21"],
        [22, "22"],
        [23, "23"],
        [24, "24"],
        [25, "25"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [0, 20, "00", 110, "20", 10],
        [1, 21, "01", 111, "21", 11],
        [2, 22, "02", 112, "22", 12],
        [3, 23, "03", 113, "23", 13],
        [4, 24, "04", 114, "24", 14],
        [5, 25, "05", 115, "25", 15]
])")
            .ValueOrDie();
    CheckResult({array0, array1, array2}, read_schema, reader_offsets, field_offsets,
                expected_array);
}

TEST_P(DataEvolutionFileReaderTest, TestWithNonExistField) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()),        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),         arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::utf8()),         arrow::field("f5", arrow::int32()),
        arrow::field("non-field", arrow::int32()),
    };
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 2, 0, 1, 2, 1, -1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0, -1};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[5], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114],
        [15, 115]
])")
                      .ValueOrDie();
    auto array2 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[4]}), R"([
        [20, "20"],
        [21, "21"],
        [22, "22"],
        [23, "23"],
        [24, "24"],
        [25, "25"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [0, 20, "00", 110, "20", 10, null],
        [1, 21, "01", 111, "21", 11, null],
        [2, 22, "02", 112, "22", 12, null],
        [3, 23, "03", 113, "23", 13, null],
        [4, 24, "04", 114, "24", 14, null],
        [5, 25, "05", 115, "25", 15, null]
])")
            .ValueOrDie();
    CheckResult({array0, array1, array2}, read_schema, reader_offsets, field_offsets,
                expected_array);
}

TEST_P(DataEvolutionFileReaderTest, TestReadFromPartialReaders) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),  arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::utf8()),  arrow::field("f5", arrow::int32()),
    };
    auto read_schema = arrow::schema(read_fields);
    // simulate reader2 has no field to read
    std::vector<int32_t> reader_offsets = {0, 3, 0, 1, 3, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[5], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114],
        [15, 115]
])")
                      .ValueOrDie();
    auto array3 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[4]}), R"([
        [20, "20"],
        [21, "21"],
        [22, "22"],
        [23, "23"],
        [24, "24"],
        [25, "25"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [0, 20, "00", 110, "20", 10],
        [1, 21, "01", 111, "21", 11],
        [2, 22, "02", 112, "22", 12],
        [3, 23, "03", 113, "23", 13],
        [4, 24, "04", 114, "24", 14],
        [5, 25, "05", 115, "25", 15]
])")
            .ValueOrDie();

    CheckResult({array0, array1, nullptr, array3}, read_schema, reader_offsets, field_offsets,
                expected_array);
}

TEST_P(DataEvolutionFileReaderTest, TestNestedType) {
    arrow::FieldVector read_fields = {
        arrow::field("f1", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f2", arrow::list(arrow::float32())),
        arrow::field("f3", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                           arrow::field("f1", arrow::int64())})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f5", arrow::date32()),
        arrow::field("f6", arrow::decimal128(2, 2))};
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 1, 0, 1, 0, 1};
    std::vector<int32_t> field_offsets = {2, 0, 1, 1, 0, 2};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[4], read_fields[2], read_fields[0]}), R"([
        [2456, [true, 2], [[0, 0]]],
        [24, [true, 1], [[0, 1]]],
        [2456, [false, 12], [[10, 10]]],
        [245, [false, 2222], [[127, 32767], [-128, -32768]]],
        [24, [true, 2], [[1, 64], [2, 32]]],
        [24, [true, 2], [[11, 64], [12, 32]]]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[3], read_fields[5]}), R"([
        [[0.1, 0.2], "1970-01-01 00:02:03.123123", "0.22"],
        [[0.1, 0.3], "1970-01-01 00:02:03.999999", "0.28"],
        [[1.1, 1.2], "1970-01-01 00:02:03.123123", "0.22"],
        [[1.1, 1.2], "1970-01-01 00:02:03.123123", "0.12"],
        [[2.2, 3.2], "1970-01-01 00:00:00.0", "0.78"],
        [[2.2, 3.2], "1970-01-01 00:00:00.123123", "0.78"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999999", 24, "0.28"],
        [[[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123", 245, "0.12"],
        [[[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [[[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123123", 24, "0.78"]
])")
            .ValueOrDie();
    CheckResult({array0, array1}, read_schema, reader_offsets, field_offsets, expected_array);
}

TEST_P(DataEvolutionFileReaderTest, TestWithBitmap) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()),       arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::utf8()),        arrow::field("f5", arrow::int32()),
        arrow::field("non-exist", arrow::int32())};
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 2, 0, 1, 2, 1, -1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0, -1};

    RoaringBitmap32 selection_bitmap = RoaringBitmap32::From({1, 3, 5});
    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[5], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114],
        [15, 115]
])")
                      .ValueOrDie();
    auto array2 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[4]}), R"([
        [20, "20"],
        [21, "21"],
        [22, "22"],
        [23, "23"],
        [24, "24"],
        [25, "25"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [1, 21, "01", 111, "21", 11, null],
        [3, 23, "03", 113, "23", 13, null],
        [5, 25, "05", 115, "25", 15, null]
])")
            .ValueOrDie();
    CheckResult({array0, array1, array2}, read_schema, reader_offsets, field_offsets,
                expected_array, selection_bitmap);
}

TEST_P(DataEvolutionFileReaderTest, TestSingleReaderRowCountMismatch) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()), arrow::field("f3", arrow::int32())};
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 1, 0, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114]
])")
                      .ValueOrDie();
    std::vector<std::unique_ptr<BatchReader>> readers;
    for (const auto& array : {array0, array1}) {
        auto file_batch_reader =
            std::make_unique<MockFileBatchReader>(array, array->type(), /*read_batch_size=*/10);
        auto enable_randomize_batch_size = GetParam();
        file_batch_reader->EnableRandomizeBatchSize(enable_randomize_batch_size);
        readers.push_back(std::move(file_batch_reader));
    }
    ASSERT_OK_AND_ASSIGN(
        auto data_evolution_file_reader,
        DataEvolutionFileReader::Create(std::move(readers), read_schema, /*read_batch_size=*/10,
                                        reader_offsets, field_offsets, arrow_pool_));
    ASSERT_EQ(arrow_pool_.get(), data_evolution_file_reader->arrow_pool_.get());
    // array0 has 6 rows but array1 only has 5 rows
    ASSERT_NOK_WITH_MSG(
        paimon::test::ReadResultCollector::CollectResult(data_evolution_file_reader.get()),
        "array for single reader length mismatch others");
}

INSTANTIATE_TEST_SUITE_P(EnableRandomizeBatchSize, DataEvolutionFileReaderTest,
                         ::testing::ValuesIn({true, false}));

}  // namespace paimon::test
