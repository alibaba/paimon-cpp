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

#include "paimon/common/data/internal_row.h"

#include <cstddef>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(InternalRowTest, TestCreateFieldGetter) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::binary()),
        arrow::field("f9", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("f10", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("f11", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("f12", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f13", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("f14", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("f15", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("f16", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
        arrow::field("f17", arrow::decimal128(30, 5)),
        arrow::field("f18", arrow::date32()),
        arrow::field("f19", arrow::list(arrow::int32())),
        arrow::field("f20", arrow::map(arrow::boolean(), arrow::int64())),
        arrow::field("f21",
                     arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
    };

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [true, 1, 2, 3, 4, 5.1, 6.12, "abc", "def", "1970-01-02 00:00:01", "1970-01-02 00:00:00.001",
        "1970-01-02 00:00:00.000001", "1970-01-02 00:00:00.000000001", "1970-01-02 00:00:02", "1970-01-02 00:00:00.002",
        "1970-01-02 00:00:00.000002", "1970-01-02 00:00:00.000000002", "-123456789987654321.45678", 12345,
        [1, 2, 3], [[true, 3], [false, 4]], [10, 10.1, false]]
    ])")
            .ValueOrDie());
    auto pool = GetDefaultPool();
    ColumnarRow row(src_array->fields(), pool, /*row_id=*/0);

    std::vector<InternalRow::FieldGetterFunc> getters;
    for (size_t i = 0; i < fields.size(); i++) {
        ASSERT_OK_AND_ASSIGN(
            InternalRow::FieldGetterFunc getter,
            InternalRow::CreateFieldGetter(i, fields[i]->type(), /*use_view=*/true));
        getters.push_back(getter);
    }

    ASSERT_EQ(getters[0](row), VariantType(true));
    ASSERT_EQ(getters[1](row), VariantType(static_cast<char>(1)));
    ASSERT_EQ(getters[2](row), VariantType(static_cast<int16_t>(2)));
    ASSERT_EQ(getters[3](row), VariantType(static_cast<int32_t>(3)));
    ASSERT_EQ(getters[4](row), VariantType(static_cast<int64_t>(4)));
    ASSERT_EQ(getters[5](row), VariantType(static_cast<float>(5.1)));
    ASSERT_EQ(getters[6](row), VariantType(static_cast<double>(6.12)));
    auto string_view7 = DataDefine::GetVariantValue<std::string_view>(getters[7](row));
    ASSERT_EQ(std::string(string_view7), "abc");
    auto string_view8 = DataDefine::GetVariantValue<std::string_view>(getters[8](row));
    ASSERT_EQ(std::string(string_view8), "def");
    ASSERT_EQ(getters[9](row), VariantType(Timestamp(1 * DateTimeUtils::MILLIS_PER_DAY + 1000, 0)));
    ASSERT_EQ(getters[10](row), VariantType(Timestamp(1 * DateTimeUtils::MILLIS_PER_DAY + 1, 0)));
    ASSERT_EQ(getters[11](row), VariantType(Timestamp(1 * DateTimeUtils::MILLIS_PER_DAY, 1000)));
    ASSERT_EQ(getters[12](row), VariantType(Timestamp(1 * DateTimeUtils::MILLIS_PER_DAY, 1)));
    ASSERT_EQ(getters[13](row),
              VariantType(Timestamp(1 * DateTimeUtils::MILLIS_PER_DAY + 2000, 0)));
    ASSERT_EQ(getters[14](row), VariantType(Timestamp(1 * DateTimeUtils::MILLIS_PER_DAY + 2, 0)));
    ASSERT_EQ(getters[15](row), VariantType(Timestamp(1 * DateTimeUtils::MILLIS_PER_DAY, 2000)));
    ASSERT_EQ(getters[16](row), VariantType(Timestamp(1 * DateTimeUtils::MILLIS_PER_DAY, 2)));

    ASSERT_EQ(
        getters[17](row),
        VariantType(Decimal(30, 5, DecimalUtils::StrToInt128("-12345678998765432145678").value())));
    ASSERT_EQ(getters[18](row), VariantType(12345));
    ASSERT_EQ(DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(getters[19](row))
                  ->ToIntArray()
                  .value(),
              std::vector<int32_t>({1, 2, 3}));
    ASSERT_EQ(DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(getters[20](row))
                  ->KeyArray()
                  ->ToBooleanArray()
                  .value(),
              std::vector<char>({true, false}));
    ASSERT_EQ(DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(getters[20](row))
                  ->ValueArray()
                  ->ToLongArray()
                  .value(),
              std::vector<int64_t>({3l, 4l}));
    auto inner_row = DataDefine::GetVariantValue<std::shared_ptr<InternalRow>>(getters[21](row));
    ASSERT_EQ(inner_row->GetLong(0), 10l);
    ASSERT_EQ(inner_row->GetDouble(1), 10.1);
    ASSERT_EQ(inner_row->GetBoolean(2), false);
}

TEST(InternalRowTest, TestCreateFieldGetterWithNull) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::boolean()),
                                 arrow::field("f1", arrow::int8())};

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [true, null]
    ])")
            .ValueOrDie());
    auto pool = GetDefaultPool();
    ColumnarRow row(src_array->fields(), pool, /*row_id=*/0);

    std::vector<InternalRow::FieldGetterFunc> getters;
    for (size_t i = 0; i < fields.size(); i++) {
        ASSERT_OK_AND_ASSIGN(
            InternalRow::FieldGetterFunc getter,
            InternalRow::CreateFieldGetter(i, fields[i]->type(), /*use_view=*/true));
        getters.push_back(getter);
    }

    ASSERT_EQ(getters[0](row), VariantType(true));
    ASSERT_TRUE(DataDefine::IsVariantNull(getters[1](row)));
}

TEST(InternalRowTest, TestCreateFieldGetterWithInvalidType) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::large_utf8())};

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        ["hello"]
    ])")
            .ValueOrDie());
    auto pool = GetDefaultPool();
    ColumnarRow row(src_array->fields(), pool, /*row_id=*/0);

    ASSERT_NOK_WITH_MSG(InternalRow::CreateFieldGetter(0, fields[0]->type(), /*use_view=*/true),
                        "not support in data getter");
}

}  // namespace paimon::test
