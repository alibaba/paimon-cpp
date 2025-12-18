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

#include "paimon/format/parquet/parquet_timestamp_converter.h"

#include <memory>

#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::parquet::test {

TEST(ParquetTimestampConverterTest, TestNeedCastArrayForTimestamp) {
    {
        // single field need cast
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::timestamp(arrow::TimeUnit::NANO)),
        };
        arrow::FieldVector target_fields = {
            arrow::field("f0", arrow::timestamp(arrow::TimeUnit::NANO, "UTC")),
        };
        ASSERT_OK_AND_ASSIGN(bool need_cast,
                             ParquetTimestampConverter::NeedCastArrayForTimestamp(
                                 arrow::struct_(fields), arrow::struct_(target_fields)));
        ASSERT_TRUE(need_cast);
    }
    {
        // field in list need cast
        arrow::FieldVector fields = {
            arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI))),
        };
        arrow::FieldVector target_fields = {
            arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::SECOND)))};
        ASSERT_OK_AND_ASSIGN(bool need_cast,
                             ParquetTimestampConverter::NeedCastArrayForTimestamp(
                                 arrow::struct_(fields), arrow::struct_(target_fields)));
        ASSERT_TRUE(need_cast);
    }
    {
        // field in map need cast
        arrow::FieldVector fields = {
            arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::MILLI),
                                          arrow::timestamp(arrow::TimeUnit::NANO))),
        };
        arrow::FieldVector target_fields = {
            arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::SECOND),
                                          arrow::timestamp(arrow::TimeUnit::NANO, "UTC")))};
        ASSERT_OK_AND_ASSIGN(bool need_cast,
                             ParquetTimestampConverter::NeedCastArrayForTimestamp(
                                 arrow::struct_(fields), arrow::struct_(target_fields)));
        ASSERT_TRUE(need_cast);
    }
    {
        // field in struct need cast
        arrow::FieldVector fields = {
            arrow::field("f3", arrow::struct_(
                                   {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI)),
                                    arrow::field("f1", arrow::timestamp(arrow::TimeUnit::NANO))})),
        };
        arrow::FieldVector target_fields = {
            arrow::field("f3",
                         arrow::struct_(
                             {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI)),
                              arrow::field("f1", arrow::timestamp(arrow::TimeUnit::NANO, "UTC"))})),
        };
        ASSERT_OK_AND_ASSIGN(bool need_cast,
                             ParquetTimestampConverter::NeedCastArrayForTimestamp(
                                 arrow::struct_(fields), arrow::struct_(target_fields)));
        ASSERT_TRUE(need_cast);
    }
}

TEST(ParquetTimestampConverterTest, TestCastArrayForTimestamp) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::MILLI),
                                      arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))),
        arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI))),
        arrow::field("f3", arrow::struct_(
                               {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
                                arrow::field("f1", arrow::timestamp(arrow::TimeUnit::NANO))})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
    };

    arrow::FieldVector target_fields = {
        arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::SECOND),
                                      arrow::timestamp(arrow::TimeUnit::MICRO, timezone))),
        arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::SECOND))),
        arrow::field("f3",
                     arrow::struct_(
                         {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
                          arrow::field("f1", arrow::timestamp(arrow::TimeUnit::NANO, timezone))})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };

    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
[[["1970-01-01 00:00:01", "1970-01-01 00:00:00.000001"]], ["1970-01-01 00:00:02"], ["1970-01-01 00:00:02", "1970-01-01 00:00:00.000000002"], "1970-01-01 00:00:00.000000002"],
[[["1970-01-01 00:00:03", "1970-01-01 00:00:00.000003"]], ["1970-01-01 00:00:04"], ["1970-01-01 00:00:04", "1970-01-01 00:00:00.000000004"], "1970-01-01 00:00:00.000000004"],
[null, null, null, "1970-01-01 00:00:00.000000004"]
    ])")
            .ValueOrDie());

    std::shared_ptr<arrow::MemoryPool> pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> result_array,
                         ParquetTimestampConverter::CastArrayForTimestamp(
                             array, arrow::struct_(target_fields), pool));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(target_fields), R"([
[[["1970-01-01 00:00:01", "1970-01-01 00:00:00.000001"]], ["1970-01-01 00:00:02"], ["1970-01-01 00:00:02", "1970-01-01 00:00:00.000000002"], "1970-01-01 00:00:00.000000002"],
[[["1970-01-01 00:00:03", "1970-01-01 00:00:00.000003"]], ["1970-01-01 00:00:04"], ["1970-01-01 00:00:04", "1970-01-01 00:00:00.000000004"], "1970-01-01 00:00:00.000000004"],
[null, null, null, "1970-01-01 00:00:00.000000004"]
    ])")
            .ValueOrDie());
    ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
}

TEST(ParquetTimestampConverterTest, TestAdjustTimezone) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::MILLI),
                                      arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))),
        arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"))),
        arrow::field(
            "f3",
            arrow::struct_({arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI)),
                            arrow::field("f1", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
    };

    arrow::FieldVector target_fields = {
        arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::MILLI),
                                      arrow::timestamp(arrow::TimeUnit::MICRO, timezone))),
        arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI, timezone))),
        arrow::field("f3",
                     arrow::struct_(
                         {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI)),
                          arrow::field("f1", arrow::timestamp(arrow::TimeUnit::MICRO, timezone))})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
    };

    ASSERT_OK_AND_ASSIGN(auto result_type,
                         ParquetTimestampConverter::AdjustTimezone(arrow::struct_(fields)));
    ASSERT_TRUE(result_type->Equals(arrow::struct_(target_fields)));
}
}  // namespace paimon::parquet::test
