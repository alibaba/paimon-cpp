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

#include "paimon/common/reader/reader_utils.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {
TEST(ReaderUtilsTest, TestAddAllValidBitmap) {
    auto check_result = [](const std::string& src_str) {
        if (src_str.empty()) {
            auto batch_with_bitmap = ReaderUtils::AddAllValidBitmap(BatchReader::MakeEofBatch());
            ASSERT_TRUE(BatchReader::IsEofBatch(batch_with_bitmap));
            auto& [_, bitmap] = batch_with_bitmap;
            ASSERT_TRUE(bitmap.IsEmpty());
            return;
        }
        auto array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), src_str).ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto batch, ReadResultCollector::GetReadBatch(array));
        auto batch_with_bitmap = ReaderUtils::AddAllValidBitmap(std::move(batch));
        auto& [c_batch, bitmap] = batch_with_bitmap;
        ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::GetArray(std::move(c_batch)));
        ASSERT_EQ(bitmap.Cardinality(), result_array->length());
        ASSERT_TRUE(result_array->Equals(array));
    };

    check_result("[0, 1, 2, 3, 4]");
    check_result("[10, 20, 30]");
    check_result("");
}
TEST(ReaderUtilsTest, TestApplyBitmapToReadBatch) {
    auto check_result = [](const std::string& src_str, const std::vector<int32_t>& bitmap_vec,
                           const std::string& target_str, const std::string& erro_msg = "") {
        auto bitmap = RoaringBitmap32::From(bitmap_vec);
        if (src_str.empty()) {
            auto batch_with_bitmap = std::make_pair(BatchReader::MakeEofBatch(), std::move(bitmap));
            ASSERT_OK_AND_ASSIGN(auto result_batch,
                                 ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap),
                                                                     arrow::default_memory_pool()));
            ASSERT_TRUE(BatchReader::IsEofBatch(result_batch));
            return;
        }
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), src_str).ValueOrDie();
        auto target_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), target_str).ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto src_batch, ReadResultCollector::GetReadBatch(src_array));
        auto batch_with_bitmap = std::make_pair(std::move(src_batch), std::move(bitmap));
        if (!erro_msg.empty()) {
            ASSERT_NOK_WITH_MSG(ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap),
                                                                    arrow::default_memory_pool()),
                                erro_msg);
            return;
        }
        ASSERT_OK_AND_ASSIGN(auto result_batch,
                             ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap),
                                                                 arrow::default_memory_pool()));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::GetArray(std::move(result_batch)));
        ASSERT_TRUE(result_array->Equals(target_array));
    };
    check_result("[10, 11, 12, 13, 14]", {1}, "[11]");
    check_result("[10, 11, 12, 13, 14]", {0, 1}, "[10, 11]");
    check_result("[10, 11, 12, 13, 14]", {2, 4}, "[12, 14]");
    check_result("[10, 11, 12, 13, 14]", {2, 3}, "[12, 13]");
    check_result("[10, 11, 12, 13, 14]", {0, 1, 3, 4}, "[10, 11, 13, 14]");
    check_result("[10, 11, 12, 13, 14]", {0, 1, 2, 3, 4}, "[10, 11, 12, 13, 14]");
    // eof batch
    check_result("", {}, "");
    // bitmap is empty, invalid
    check_result("[10, 11, 12, 13, 14]", {}, "[]",
                 "NextBatchWithBitmap should always return the result with at least one valid row "
                 "except eof");
}

}  // namespace paimon::test
