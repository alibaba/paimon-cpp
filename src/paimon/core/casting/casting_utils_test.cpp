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

#include "paimon/core/casting/casting_utils.h"

#include <memory>

#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class CastingUtilsTest : public ::testing::Test {
    std::shared_ptr<arrow::MemoryPool> arrow_pool_ = GetArrowPool(GetDefaultPool());
};

TEST_F(CastingUtilsTest, TestDictionaryToString) {
    auto dict =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["foo", "bar", "bazr"])")
            .ValueOrDie();
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 0, 2, 0]").ValueOrDie();
    std::shared_ptr<arrow::DictionaryArray> dict_array =
        std::make_shared<arrow::DictionaryArray>(dict_type, indices, dict);
    auto string_array = arrow::ipc::internal::json::ArrayFromJSON(
                            arrow::utf8(), R"(["bar", "bazr", "foo", "bazr", "foo"])")
                            .ValueOrDie();

    auto pool = GetArrowPool(GetDefaultPool());
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    ASSERT_OK_AND_ASSIGN(
        auto result_array,
        CastingUtils::Cast(dict_array, /*target_type=*/arrow::utf8(), options, pool.get()));
    ASSERT_TRUE(result_array->Equals(string_array));
}

TEST_F(CastingUtilsTest, TestTimestampToTimestampWithTimezone) {
    // local no tz -> utc tz
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                         arrow::timestamp(arrow::TimeUnit::SECOND), R"(["1970-01-01 00:00:01"])")
                         .ValueOr(nullptr);
    ASSERT_TRUE(src_array);
    auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai");
    auto target_ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(target_type);
    auto target_array =
        arrow::ipc::internal::json::ArrayFromJSON(target_type, R"(["1969-12-31 16:00:01"])")
            .ValueOr(nullptr);
    ASSERT_TRUE(target_array);
    ASSERT_OK_AND_ASSIGN(auto result_array, CastingUtils::TimestampToTimestampWithTimezone(
                                                src_array, target_ts_type, arrow_pool_.get()));
    ASSERT_TRUE(target_array->Equals(result_array));
}

TEST_F(CastingUtilsTest, TestTimestampToTimestampWithTimezoneInvalid) {
    // local no tz -> utc tz
    {
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::timestamp(arrow::TimeUnit::SECOND),
                                                      R"(["1970-01-01 00:00:01"])")
                .ValueOr(nullptr);
        ASSERT_TRUE(src_array);
        auto target_type = arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai");
        auto target_ts_type =
            arrow::internal::checked_pointer_cast<arrow::TimestampType>(target_type);
        ASSERT_NOK_WITH_MSG(CastingUtils::TimestampToTimestampWithTimezone(
                                src_array, target_ts_type, arrow_pool_.get()),
                            "time unit of src and target type mismatch");
    }
    {
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::timestamp(arrow::TimeUnit::SECOND),
                                                      R"(["1970-01-01 00:00:01"])")
                .ValueOr(nullptr);
        ASSERT_TRUE(src_array);
        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND);
        auto target_ts_type =
            arrow::internal::checked_pointer_cast<arrow::TimestampType>(target_type);
        ASSERT_NOK_WITH_MSG(
            CastingUtils::TimestampToTimestampWithTimezone(src_array, target_ts_type,
                                                           arrow_pool_.get()),
            "src value must be local time (no tz), target value must be UTC (with tz)");
    }
    {
        auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                             arrow::timestamp(arrow::TimeUnit::SECOND),
                             R"(["2015-03-29 02:30:00", "2015-03-29 03:30:00"])")
                             .ValueOr(nullptr);
        ASSERT_TRUE(src_array);
        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Europe/Warsaw");
        auto target_ts_type =
            arrow::internal::checked_pointer_cast<arrow::TimestampType>(target_type);
        ASSERT_NOK_WITH_MSG(CastingUtils::TimestampToTimestampWithTimezone(
                                src_array, target_ts_type, arrow_pool_.get()),
                            "Timestamp doesn't exist in timezone 'Europe/Warsaw': 2015-03-29 "
                            "02:30:00 is in a gap between");
    }
}

TEST_F(CastingUtilsTest, TestTimestampWithTimezoneToTimestamp) {
    // utc tz -> local no tz
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                         arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai"),
                         R"(["1970-01-01 00:00:01"])")
                         .ValueOr(nullptr);
    ASSERT_TRUE(src_array);
    auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND);
    auto target_ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(target_type);
    auto target_array =
        arrow::ipc::internal::json::ArrayFromJSON(target_type, R"(["1970-01-01 08:00:01"])")
            .ValueOr(nullptr);
    ASSERT_TRUE(target_array);
    ASSERT_OK_AND_ASSIGN(auto result_array, CastingUtils::TimestampWithTimezoneToTimestamp(
                                                src_array, target_ts_type, arrow_pool_.get()));
    ASSERT_TRUE(target_array->Equals(result_array));
}

TEST_F(CastingUtilsTest, TestTimestampWithTimezoneToTimestampInvalid) {
    // utc tz -> local no tz
    {
        auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                             arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai"),
                             R"(["1970-01-01 00:00:01"])")
                             .ValueOr(nullptr);
        ASSERT_TRUE(src_array);
        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND);
        auto target_ts_type =
            arrow::internal::checked_pointer_cast<arrow::TimestampType>(target_type);
        ASSERT_NOK_WITH_MSG(CastingUtils::TimestampWithTimezoneToTimestamp(
                                src_array, target_ts_type, arrow_pool_.get()),
                            "in timezone converter, time unit of src and target type mismatch");
    }
    {
        auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                             arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai"),
                             R"(["1970-01-01 00:00:01"])")
                             .ValueOr(nullptr);
        ASSERT_TRUE(src_array);
        auto target_type = arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Tokyo");
        auto target_ts_type =
            arrow::internal::checked_pointer_cast<arrow::TimestampType>(target_type);
        ASSERT_NOK_WITH_MSG(CastingUtils::TimestampWithTimezoneToTimestamp(
                                src_array, target_ts_type, arrow_pool_.get()),
                            "target value must be local time (no tz)");
    }
}

}  // namespace paimon::test
