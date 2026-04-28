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

#include "paimon/common/global_index/global_index_utils.h"

#include <optional>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class GlobalIndexUtilsTest : public ::testing::Test {
 public:
    /// Helper to create a valid ArrowArray with the given number of int32 elements.
    static ArrowArray CreateInt32Array(const std::vector<int32_t>& values) {
        arrow::Int32Builder builder;
        EXPECT_TRUE(builder.AppendValues(values).ok());
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        return c_array;
    }
};

TEST_F(GlobalIndexUtilsTest, TestNullArrowArray) {
    std::vector<int64_t> row_ids = {0, 1, 2};
    ASSERT_NOK_WITH_MSG(GlobalIndexUtils::CheckRelativeRowIds(nullptr, row_ids, std::nullopt),
                        "CheckRelativeRowIds failed: null c_arrow_array");
}

TEST_F(GlobalIndexUtilsTest, TestEmptyArrayReturnsOk) {
    auto c_array = CreateInt32Array({});
    ASSERT_OK(GlobalIndexUtils::CheckRelativeRowIds(&c_array, {}, std::nullopt));
    // ArrowArray was not released by CheckRelativeRowIds (early return for length==0),
    // so we release it manually.
    if (!ArrowArrayIsReleased(&c_array)) {
        ArrowArrayRelease(&c_array);
    }
}

TEST_F(GlobalIndexUtilsTest, TestEmptyArrayReturnsInvalid) {
    auto c_array = CreateInt32Array({});
    std::vector<int64_t> row_ids = {0, 1, 2};
    ASSERT_NOK_WITH_MSG(
        GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, std::nullopt),
        "relative_row_ids length 3 mismatch arrow_array length 0 in CheckRelativeRowIds");
}

TEST_F(GlobalIndexUtilsTest, TestMatchingRowIdsAndNoExpectedNextRowId) {
    auto c_array = CreateInt32Array({10, 20, 30});
    std::vector<int64_t> row_ids = {0, 1, 2};
    // expected_next_row_id is nullopt, so the first-element check is skipped.
    ASSERT_OK(GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, std::nullopt));
    // On success, guard.Release() is called and ArrowArray is NOT released by
    // CheckRelativeRowIds, so we must release it manually.
    if (!ArrowArrayIsReleased(&c_array)) {
        ArrowArrayRelease(&c_array);
    }
}

TEST_F(GlobalIndexUtilsTest, TestMatchingRowIdsWithCorrectExpectedNextRowId) {
    auto c_array = CreateInt32Array({10, 20, 30});
    std::vector<int64_t> row_ids = {5, 6, 7};
    // expected_next_row_id matches row_ids[0]
    ASSERT_OK(GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, 5));
    // On success, guard.Release() is called and ArrowArray is NOT released by
    // CheckRelativeRowIds, so we must release it manually.
    if (!ArrowArrayIsReleased(&c_array)) {
        ArrowArrayRelease(&c_array);
    }
}

TEST_F(GlobalIndexUtilsTest, TestMismatchedLength) {
    auto c_array = CreateInt32Array({10, 20, 30});
    std::vector<int64_t> row_ids = {0, 1};
    ASSERT_NOK_WITH_MSG(
        GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, std::nullopt),
        "relative_row_ids length 2 mismatch arrow_array length 3 in CheckRelativeRowIds");
}

TEST_F(GlobalIndexUtilsTest, TestMismatchedExpectedNextRowId) {
    auto c_array = CreateInt32Array({10, 20, 30});
    std::vector<int64_t> row_ids = {5, 6, 7};
    // expected_next_row_id is 100, but row_ids[0] is 5
    ASSERT_NOK_WITH_MSG(
        GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, 100),
        "first relative_row_ids 5 mismatch inner expected_next_row_id 100 in CheckRelativeRowIds");
}

}  // namespace paimon::test
