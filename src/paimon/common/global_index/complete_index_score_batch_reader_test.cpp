/*
 * Copyright 2025-present Alibaba Inc.
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

#include "paimon/common/global_index/complete_index_score_batch_reader.h"

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class CompleteIndexScoreBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {
        pool_.reset();
    }

    std::unique_ptr<BatchReader> PrepareCompleteIndexScoreBatchReader(
        const std::shared_ptr<arrow::Array>& src_array, const RoaringBitmap32& selected_bitmap,
        const std::vector<float>& scores, int32_t batch_size) const {
        auto file_batch_reader = std::make_unique<MockFileBatchReader>(src_array, src_array->type(),
                                                                       selected_bitmap, batch_size);
        return std::make_unique<CompleteIndexScoreBatchReader>(std::move(file_batch_reader), scores,
                                                               pool_);
    }

    std::unique_ptr<BatchReader> PrepareCompleteIndexScoreBatchReader(
        const std::shared_ptr<arrow::Array>& src_array, const std::vector<float>& scores,
        int32_t batch_size) const {
        auto file_batch_reader =
            std::make_unique<MockFileBatchReader>(src_array, src_array->type(), batch_size);
        return std::make_unique<CompleteIndexScoreBatchReader>(std::move(file_batch_reader), scores,
                                                               pool_);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(CompleteIndexScoreBatchReaderTest, TestSimple) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_INDEX_SCORE", arrow::float32()),
        arrow::field("_ROW_ID", arrow::int64()),
    };

    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        ["Alice", 10, null, 0],
        ["Bob", 11, null, 1],
        ["Cathy", 12, null, 2]
    ])")
                         .ValueOrDie();

    std::vector<float> scores = {1.23f, 2.34f, 100.10f};
    auto reader = PrepareCompleteIndexScoreBatchReader(src_array, scores,
                                                       /*batch_size=*/1);

    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields), {R"([
        ["Alice", 10, 1.23, 0],
        ["Bob", 11, 2.34, 1],
        ["Cathy", 12, 100.10, 2]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(expected_array->ApproxEquals(*result_array));
    reader->Close();
}

TEST_F(CompleteIndexScoreBatchReaderTest, TestWithBitmap) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_INDEX_SCORE", arrow::float32()),
        arrow::field("_ROW_ID", arrow::int64()),
    };

    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        ["Alice", 10, null, 0],
        ["Bob", 11, null, 1],
        ["Cathy", 12, null, 2],
        ["David", 13, null, 4]
    ])")
                         .ValueOrDie();

    std::vector<float> scores = {1.23f, -19.12f};
    auto selected_bitmap = RoaringBitmap32::From({0, 3});
    auto reader = PrepareCompleteIndexScoreBatchReader(src_array, selected_bitmap, scores,
                                                       /*batch_size=*/2);

    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields), {R"([
        ["Alice", 10, 1.23, 0],
        ["David", 13, -19.12, 4]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(expected_array->ApproxEquals(*result_array));
    reader->Close();
}

TEST_F(CompleteIndexScoreBatchReaderTest, TestReadWithNullScores) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_INDEX_SCORE", arrow::float32()),
        arrow::field("_ROW_ID", arrow::int64()),
    };

    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        ["Alice", 10, null, 0],
        ["Bob", 11, null, 1],
        ["Cathy", 12, null, 2]
    ])")
                         .ValueOrDie();

    // scores is empty, indicates all null score
    auto reader = PrepareCompleteIndexScoreBatchReader(src_array, /*scores=*/{},
                                                       /*batch_size=*/1);

    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(reader.get()));

    auto expected_array = std::make_shared<arrow::ChunkedArray>(src_array);
    ASSERT_TRUE(expected_array->Equals(*result_array));
    reader->Close();
}

}  // namespace paimon::test
