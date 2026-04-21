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

#include "paimon/common/global_index/btree/key_serializer.h"

#include "gtest/gtest.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class KeySerializerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(KeySerializerTest, SerializeAndDeserializeAllTypes) {
    // BOOLEAN
    {
        Literal literal(true);
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, arrow::boolean(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, arrow::boolean(), pool_.get()));
        ASSERT_EQ(result.GetValue<bool>(), true);

        Literal literal_false(false);
        ASSERT_OK_AND_ASSIGN(
            bytes, KeySerializer::SerializeKey(literal_false, arrow::boolean(), pool_.get()));
        slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(result,
                             KeySerializer::DeserializeKey(slice, arrow::boolean(), pool_.get()));
        ASSERT_EQ(result.GetValue<bool>(), false);
    }

    // TINYINT (int8)
    {
        Literal literal(static_cast<int8_t>(-42));
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, arrow::int8(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, arrow::int8(), pool_.get()));
        ASSERT_EQ(result.GetValue<int8_t>(), -42);
    }

    // SMALLINT (int16)
    {
        Literal literal(static_cast<int16_t>(12345));
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, arrow::int16(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, arrow::int16(), pool_.get()));
        ASSERT_EQ(result.GetValue<int16_t>(), 12345);
    }

    // INT (int32)
    {
        Literal literal(42);
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, arrow::int32(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, arrow::int32(), pool_.get()));
        ASSERT_EQ(result.GetValue<int32_t>(), 42);
    }

    // DATE (stored as int32)
    {
        Literal literal(FieldType::DATE, 18000);
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, arrow::date32(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, arrow::date32(), pool_.get()));
        ASSERT_EQ(result.GetType(), FieldType::DATE);
        ASSERT_EQ(result.GetValue<int32_t>(), 18000);
    }

    // BIGINT (int64)
    {
        Literal literal(static_cast<int64_t>(123456789012345LL));
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, arrow::int64(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, arrow::int64(), pool_.get()));
        ASSERT_EQ(result.GetValue<int64_t>(), 123456789012345LL);
    }

    // FLOAT
    {
        Literal literal(3.14f);
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, arrow::float32(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, arrow::float32(), pool_.get()));
        ASSERT_FLOAT_EQ(result.GetValue<float>(), 3.14f);
    }

    // DOUBLE
    {
        Literal literal(2.718281828);
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, arrow::float64(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, arrow::float64(), pool_.get()));
        ASSERT_DOUBLE_EQ(result.GetValue<double>(), 2.718281828);
    }

    // STRING
    {
        Literal literal(FieldType::STRING, "hello world", 11);
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, arrow::utf8(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, arrow::utf8(), pool_.get()));
        ASSERT_EQ(result.GetValue<std::string>(), "hello world");
    }

    // TIMESTAMP (compact, millis precision)
    {
        auto ts_type = arrow::timestamp(arrow::TimeUnit::MILLI);
        Literal literal(Timestamp::FromEpochMillis(1234567890));
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, ts_type, pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, ts_type, pool_.get()));
        ASSERT_EQ(result.GetValue<Timestamp>().GetMillisecond(), 1234567890);
    }

    // TIMESTAMP (non-compact, nano precision)
    {
        auto ts_type = arrow::timestamp(arrow::TimeUnit::NANO);
        Literal literal(Timestamp(5000, 123456));
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, ts_type, pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, ts_type, pool_.get()));
        ASSERT_EQ(result.GetValue<Timestamp>().GetMillisecond(), 5000);
        ASSERT_EQ(result.GetValue<Timestamp>().GetNanoOfMillisecond(), 123456);
    }

    // DECIMAL (compact, precision <= 18)
    {
        auto decimal_type = arrow::decimal128(10, 2);
        Literal literal(Decimal::FromUnscaledLong(12345, 10, 2));
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, decimal_type, pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, decimal_type, pool_.get()));
        ASSERT_EQ(result.GetValue<Decimal>().ToUnscaledLong(), 12345);
    }

    // DECIMAL (non-compact, precision > 18)
    {
        auto decimal_type = arrow::decimal128(25, 3);
        Literal literal(Decimal(25, 3, 9999999));
        ASSERT_OK_AND_ASSIGN(auto bytes,
                             KeySerializer::SerializeKey(literal, decimal_type, pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result,
                             KeySerializer::DeserializeKey(slice, decimal_type, pool_.get()));
        ASSERT_EQ(result.GetValue<Decimal>().ToString(), literal.GetValue<Decimal>().ToString());
    }

    // NULL should fail
    {
        Literal null_literal(FieldType::INT);
        ASSERT_NOK_WITH_MSG(KeySerializer::SerializeKey(null_literal, arrow::int32(), pool_.get()),
                            "cannot serialize null in KeySerializer");
    }

    // unsupported type
    {
        Literal literal(FieldType::BINARY, "ab", 2);
        ASSERT_NOK_WITH_MSG(KeySerializer::SerializeKey(literal, arrow::binary(), pool_.get()),
                            "Not support serialize BINARY type in BTreeGlobalIndex");
    }
}

TEST_F(KeySerializerTest, CreateComparator) {
    // INT comparator
    {
        auto comparator = KeySerializer::CreateComparator(arrow::int32(), pool_);
        Literal literal_1(1);
        Literal literal_2(2);
        Literal literal_3(1);
        ASSERT_OK_AND_ASSIGN(auto bytes_1,
                             KeySerializer::SerializeKey(literal_1, arrow::int32(), pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto bytes_2,
                             KeySerializer::SerializeKey(literal_2, arrow::int32(), pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto bytes_3,
                             KeySerializer::SerializeKey(literal_3, arrow::int32(), pool_.get()));
        auto slice_1 = MemorySlice::Wrap(MemorySegment::Wrap(bytes_1));
        auto slice_2 = MemorySlice::Wrap(MemorySegment::Wrap(bytes_2));
        auto slice_3 = MemorySlice::Wrap(MemorySegment::Wrap(bytes_3));

        ASSERT_OK_AND_ASSIGN(auto cmp_result, comparator(slice_1, slice_2));
        ASSERT_LT(cmp_result, 0);
        ASSERT_OK_AND_ASSIGN(cmp_result, comparator(slice_2, slice_1));
        ASSERT_GT(cmp_result, 0);
        ASSERT_OK_AND_ASSIGN(cmp_result, comparator(slice_1, slice_3));
        ASSERT_EQ(cmp_result, 0);
    }

    // STRING comparator
    {
        auto comparator = KeySerializer::CreateComparator(arrow::utf8(), pool_);
        Literal literal_a(FieldType::STRING, "apple", 5);
        Literal literal_b(FieldType::STRING, "banana", 6);
        Literal literal_c(FieldType::STRING, "apple", 5);
        ASSERT_OK_AND_ASSIGN(auto bytes_a,
                             KeySerializer::SerializeKey(literal_a, arrow::utf8(), pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto bytes_b,
                             KeySerializer::SerializeKey(literal_b, arrow::utf8(), pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto bytes_c,
                             KeySerializer::SerializeKey(literal_c, arrow::utf8(), pool_.get()));
        auto slice_a = MemorySlice::Wrap(MemorySegment::Wrap(bytes_a));
        auto slice_b = MemorySlice::Wrap(MemorySegment::Wrap(bytes_b));
        auto slice_c = MemorySlice::Wrap(MemorySegment::Wrap(bytes_c));

        ASSERT_OK_AND_ASSIGN(auto cmp_result, comparator(slice_a, slice_b));
        ASSERT_LT(cmp_result, 0);
        ASSERT_OK_AND_ASSIGN(cmp_result, comparator(slice_b, slice_a));
        ASSERT_GT(cmp_result, 0);
        ASSERT_OK_AND_ASSIGN(cmp_result, comparator(slice_a, slice_c));
        ASSERT_EQ(cmp_result, 0);
    }
}

}  // namespace paimon::test
