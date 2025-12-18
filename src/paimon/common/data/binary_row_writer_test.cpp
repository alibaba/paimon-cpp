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

#include "paimon/common/data/binary_row_writer.h"

#include <string>
#include <string_view>
#include <variant>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(BinaryRowWriterTest, TestFieldSetter) {
    int32_t arity = 24;
    BinaryRow row(arity);
    BinaryRowWriter writer(&row, /*initial_size=*/0, GetDefaultPool().get());
    writer.Reset();

    auto pool = GetDefaultPool();
    // set process
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter0,
                         BinaryRowWriter::CreateFieldSetter(0, arrow::boolean()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter1,
                         BinaryRowWriter::CreateFieldSetter(1, arrow::int8()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter2,
                         BinaryRowWriter::CreateFieldSetter(2, arrow::int16()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter3,
                         BinaryRowWriter::CreateFieldSetter(3, arrow::int32()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter4,
                         BinaryRowWriter::CreateFieldSetter(4, arrow::int64()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter5,
                         BinaryRowWriter::CreateFieldSetter(5, arrow::float32()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter6,
                         BinaryRowWriter::CreateFieldSetter(6, arrow::float64()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter7,
                         BinaryRowWriter::CreateFieldSetter(7, arrow::utf8()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter8,
                         BinaryRowWriter::CreateFieldSetter(8, arrow::binary()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter9,
                         BinaryRowWriter::CreateFieldSetter(9, arrow::date32()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter10,
                         BinaryRowWriter::CreateFieldSetter(10, arrow::decimal128(5, 2)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter11,
        BinaryRowWriter::CreateFieldSetter(11, arrow::timestamp(arrow::TimeUnit::NANO)));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter12,
                         BinaryRowWriter::CreateFieldSetter(12, arrow::utf8()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter13,
                         BinaryRowWriter::CreateFieldSetter(13, arrow::binary()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter14,
                         BinaryRowWriter::CreateFieldSetter(14, arrow::decimal128(37, 2)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter15,
        BinaryRowWriter::CreateFieldSetter(15, arrow::timestamp(arrow::TimeUnit::NANO)));
    // for timestamp type
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter16,
        BinaryRowWriter::CreateFieldSetter(16, arrow::timestamp(arrow::TimeUnit::SECOND)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter17,
        BinaryRowWriter::CreateFieldSetter(17, arrow::timestamp(arrow::TimeUnit::MILLI)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter18,
        BinaryRowWriter::CreateFieldSetter(18, arrow::timestamp(arrow::TimeUnit::MICRO)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter19,
        BinaryRowWriter::CreateFieldSetter(19, arrow::timestamp(arrow::TimeUnit::NANO)));
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter20,
                         BinaryRowWriter::CreateFieldSetter(
                             20, arrow::timestamp(arrow::TimeUnit::SECOND, timezone)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter21,
        BinaryRowWriter::CreateFieldSetter(21, arrow::timestamp(arrow::TimeUnit::MILLI, timezone)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter22,
        BinaryRowWriter::CreateFieldSetter(22, arrow::timestamp(arrow::TimeUnit::MICRO, timezone)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter23,
        BinaryRowWriter::CreateFieldSetter(23, arrow::timestamp(arrow::TimeUnit::NANO, timezone)));

    setter0(true, &writer);
    setter1(static_cast<char>(1), &writer);
    setter2(static_cast<int16_t>(2), &writer);
    setter3(static_cast<int32_t>(3), &writer);
    setter4(static_cast<int64_t>(4), &writer);
    setter5(static_cast<float>(5.5), &writer);
    setter6(6.66, &writer);
    std::string data = "abc";
    setter7(std::string_view(data.data(), data.size()), &writer);
    setter8(std::string_view(data.data(), data.size()), &writer);
    setter9(9, &writer);
    setter10(Decimal(5, 2, 123), &writer);
    setter11(Timestamp(11, 12), &writer);
    setter12(BinaryString::FromString("hello", pool.get()), &writer);
    auto bytes = std::make_shared<Bytes>("world", pool.get());
    setter13(bytes, &writer);
    setter14(NullType(), &writer);
    setter15(NullType(), &writer);

    setter16(Timestamp(1758030901000l, 0), &writer);
    setter17(Timestamp(1758030901001l, 0), &writer);
    setter18(NullType(), &writer);
    setter19(Timestamp(1758030901001l, 1001), &writer);

    setter20(NullType(), &writer);
    setter21(Timestamp(1758030902002l, 0), &writer);
    setter22(Timestamp(1758030902002l, 2000), &writer);
    setter23(Timestamp(1758030902002l, 2002), &writer);

    writer.Complete();

    // get process
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter0,
                         InternalRow::CreateFieldGetter(0, arrow::boolean(), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter1,
                         InternalRow::CreateFieldGetter(1, arrow::int8(), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter2,
                         InternalRow::CreateFieldGetter(2, arrow::int16(), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter3,
                         InternalRow::CreateFieldGetter(3, arrow::int32(), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter4,
                         InternalRow::CreateFieldGetter(4, arrow::int64(), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter5,
                         InternalRow::CreateFieldGetter(5, arrow::float32(), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter6,
                         InternalRow::CreateFieldGetter(6, arrow::float64(), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter7,
                         InternalRow::CreateFieldGetter(7, arrow::utf8(), /*use_view=*/false));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter8,
                         InternalRow::CreateFieldGetter(8, arrow::binary(),
                                                        /*use_view=*/false));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter9,
                         InternalRow::CreateFieldGetter(9, arrow::date32(), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(
        InternalRow::FieldGetterFunc getter10,
        InternalRow::CreateFieldGetter(10, arrow::decimal128(5, 2), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter11,
                         InternalRow::CreateFieldGetter(11, arrow::timestamp(arrow::TimeUnit::NANO),
                                                        /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter12,
                         InternalRow::CreateFieldGetter(12, arrow::utf8(), /*use_view=*/false));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter13,
                         InternalRow::CreateFieldGetter(13, arrow::binary(),
                                                        /*use_view=*/false));
    ASSERT_OK_AND_ASSIGN(
        InternalRow::FieldGetterFunc getter14,
        InternalRow::CreateFieldGetter(14, arrow::decimal128(37, 2), /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter15,
                         InternalRow::CreateFieldGetter(15, arrow::timestamp(arrow::TimeUnit::NANO),
                                                        /*use_view=*/true));
    // for timestamp type
    ASSERT_OK_AND_ASSIGN(
        InternalRow::FieldGetterFunc getter16,
        InternalRow::CreateFieldGetter(16, arrow::timestamp(arrow::TimeUnit::SECOND),
                                       /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(
        InternalRow::FieldGetterFunc getter17,
        InternalRow::CreateFieldGetter(17, arrow::timestamp(arrow::TimeUnit::MILLI),
                                       /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(
        InternalRow::FieldGetterFunc getter18,
        InternalRow::CreateFieldGetter(18, arrow::timestamp(arrow::TimeUnit::MICRO),
                                       /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter19,
                         InternalRow::CreateFieldGetter(19, arrow::timestamp(arrow::TimeUnit::NANO),
                                                        /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(
        InternalRow::FieldGetterFunc getter20,
        InternalRow::CreateFieldGetter(20, arrow::timestamp(arrow::TimeUnit::SECOND),
                                       /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(
        InternalRow::FieldGetterFunc getter21,
        InternalRow::CreateFieldGetter(21, arrow::timestamp(arrow::TimeUnit::MILLI),
                                       /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(
        InternalRow::FieldGetterFunc getter22,
        InternalRow::CreateFieldGetter(22, arrow::timestamp(arrow::TimeUnit::MICRO),
                                       /*use_view=*/true));
    ASSERT_OK_AND_ASSIGN(InternalRow::FieldGetterFunc getter23,
                         InternalRow::CreateFieldGetter(23, arrow::timestamp(arrow::TimeUnit::NANO),
                                                        /*use_view=*/true));

    ASSERT_EQ(DataDefine::GetVariantValue<bool>(getter0(row)), true);
    ASSERT_EQ(DataDefine::GetVariantValue<char>(getter1(row)), static_cast<char>(1));
    ASSERT_EQ(DataDefine::GetVariantValue<int16_t>(getter2(row)), static_cast<int16_t>(2));
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(getter3(row)), static_cast<int32_t>(3));
    ASSERT_EQ(DataDefine::GetVariantValue<int64_t>(getter4(row)), static_cast<int64_t>(4));
    ASSERT_EQ(DataDefine::GetVariantValue<float>(getter5(row)), static_cast<float>(5.5));
    ASSERT_EQ(DataDefine::GetVariantValue<double>(getter6(row)), static_cast<double>(6.66));

    ASSERT_EQ(DataDefine::GetVariantValue<BinaryString>(getter7(row)).ToString(), data);
    ASSERT_EQ(*DataDefine::GetVariantValue<std::shared_ptr<Bytes>>(getter8(row)),
              Bytes(data, pool.get()));
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(getter9(row)), 9);
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(getter10(row)), Decimal(5, 2, 123));
    ASSERT_EQ(DataDefine::GetVariantValue<Timestamp>(getter11(row)), Timestamp(11, 12));
    ASSERT_EQ(DataDefine::GetVariantValue<BinaryString>(getter12(row)).ToString(), "hello");
    ASSERT_EQ(*DataDefine::GetVariantValue<std::shared_ptr<Bytes>>(getter13(row)),
              Bytes("world", pool.get()));
    ASSERT_TRUE(DataDefine::IsVariantNull(getter14(row)));
    ASSERT_TRUE(DataDefine::IsVariantNull(getter15(row)));

    ASSERT_EQ(DataDefine::GetVariantValue<Timestamp>(getter16(row)), Timestamp(1758030901000l, 0));
    ASSERT_EQ(DataDefine::GetVariantValue<Timestamp>(getter17(row)), Timestamp(1758030901001l, 0));
    ASSERT_TRUE(DataDefine::IsVariantNull(getter18(row)));
    ASSERT_EQ(DataDefine::GetVariantValue<Timestamp>(getter19(row)),
              Timestamp(1758030901001l, 1001));

    ASSERT_TRUE(DataDefine::IsVariantNull(getter20(row)));
    ASSERT_EQ(DataDefine::GetVariantValue<Timestamp>(getter21(row)), Timestamp(1758030902002l, 0));
    ASSERT_EQ(DataDefine::GetVariantValue<Timestamp>(getter22(row)),
              Timestamp(1758030902002l, 2000));
    ASSERT_EQ(DataDefine::GetVariantValue<Timestamp>(getter23(row)),
              Timestamp(1758030902002l, 2002));
}

TEST(BinaryRowWriterTest, TestFieldSetterWithNull) {
    int32_t arity = 5;
    BinaryRow row(arity);
    BinaryRowWriter writer(&row, /*initial_size=*/0, GetDefaultPool().get());
    writer.Reset();
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter0,
                         BinaryRowWriter::CreateFieldSetter(0, arrow::boolean()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter1,
                         BinaryRowWriter::CreateFieldSetter(1, arrow::int8()));
    ASSERT_OK_AND_ASSIGN(BinaryRowWriter::FieldSetterFunc setter2,
                         BinaryRowWriter::CreateFieldSetter(2, arrow::decimal128(5, 2)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter3,
        BinaryRowWriter::CreateFieldSetter(3, arrow::timestamp(arrow::TimeUnit::SECOND)));
    ASSERT_OK_AND_ASSIGN(
        BinaryRowWriter::FieldSetterFunc setter4,
        BinaryRowWriter::CreateFieldSetter(4, arrow::timestamp(arrow::TimeUnit::NANO, timezone)));

    setter0(true, &writer);
    setter1(NullType(), &writer);
    setter2(NullType(), &writer);
    setter3(NullType(), &writer);
    setter4(NullType(), &writer);
    writer.Complete();

    ASSERT_EQ(row.GetBoolean(0), true);
    ASSERT_TRUE(row.IsNullAt(1));
    ASSERT_TRUE(row.IsNullAt(2));
    ASSERT_TRUE(row.IsNullAt(3));
    ASSERT_TRUE(row.IsNullAt(4));
}
}  // namespace paimon::test
