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

#include "paimon/common/data/binary_row.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <optional>
#include <set>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/serializer/binary_row_serializer.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class BinaryRowTest : public testing::Test {
 private:
    void AssertTestWriterRow(const BinaryRow& row) {
        ASSERT_EQ(row.GetString(0).ToString(), "1");
        ASSERT_EQ(row.GetInt(8), 88);
        ASSERT_EQ(row.GetShort(11), static_cast<int16_t>(292));
        ASSERT_EQ(row.GetLong(10), 284);
        ASSERT_EQ(row.GetByte(2), static_cast<char>(99));
        ASSERT_EQ(row.GetDouble(6), static_cast<double>(87.1));
        ASSERT_EQ(row.GetFloat(7), 26.1f);
        ASSERT_TRUE(row.GetBoolean(1));
        ASSERT_EQ(row.GetString(3).ToString(), "1234567");
        ASSERT_EQ(row.GetString(5).ToString(), "12345678");
        ASSERT_EQ(row.GetString(9).ToString(), "啦啦啦啦啦我是快乐的粉刷匠");
        ASSERT_EQ(row.GetString(9).HashCode(),
                  BinaryString::FromString("啦啦啦啦啦我是快乐的粉刷匠", GetDefaultPool().get())
                      .HashCode());
        ASSERT_TRUE(row.IsNullAt(12));
    }
};

TEST_F(BinaryRowTest, TestBasic) {
    // consider header 1 byte.
    ASSERT_EQ(BinaryRow(0).GetFixedLengthPartSize(), 8);
    ASSERT_EQ(BinaryRow(1).GetFixedLengthPartSize(), 16);
    ASSERT_EQ(BinaryRow(65).GetFixedLengthPartSize(), 536);
    ASSERT_EQ(BinaryRow(128).GetFixedLengthPartSize(), 1048);

    auto pool = GetDefaultPool();
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(100, pool.get());
    MemorySegment segment = MemorySegment::Wrap(bytes);
    BinaryRow row(2);
    row.PointTo(segment, 10, 48);
    row.SetInt(0, 5);
    row.SetDouble(1, 5.8);
    ASSERT_EQ(5, row.GetInt(0));
    ASSERT_EQ(static_cast<double>(5.8), row.GetDouble(1));

    row.Clear();
    std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes(100, pool.get());
    MemorySegment segment1 = MemorySegment::Wrap(bytes1);
    row.PointTo(segment1, 0, 20);
    row.SetInt(0, 5);
    ASSERT_EQ(5, row.GetInt(0));
}

TEST_F(BinaryRowTest, TestSetAndGet) {
    auto pool = GetDefaultPool();
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(100, pool.get());
    MemorySegment segment = MemorySegment::Wrap(bytes);
    BinaryRow row(9);
    row.PointTo(segment, 20, 80);
    row.SetNullAt(0);
    row.SetInt(1, 11);
    row.SetLong(2, 22);
    row.SetDouble(3, 33);
    row.SetBoolean(4, true);
    row.SetShort(5, static_cast<int16_t>(55));
    row.SetByte(6, static_cast<char>(66));
    row.SetFloat(7, static_cast<float>(77));

    ASSERT_EQ(row.GetInt(1), 11);
    ASSERT_TRUE(row.IsNullAt(0));
    ASSERT_EQ(row.GetShort(5), static_cast<int16_t>(55));
    ASSERT_EQ(row.GetLong(2), 22L);
    ASSERT_TRUE(row.GetBoolean(4));
    ASSERT_EQ(row.GetByte(6), static_cast<char>(66));
    ASSERT_EQ(row.GetFloat(7), static_cast<float>(77));
    ASSERT_EQ(row.GetDouble(3), static_cast<double>(33));
}

TEST_F(BinaryRowTest, TestHeaderSize) {
    ASSERT_EQ(BinaryRow::CalculateBitSetWidthInBytes(56), 8);
    ASSERT_EQ(BinaryRow::CalculateBitSetWidthInBytes(57), 16);
    ASSERT_EQ(BinaryRow::CalculateBitSetWidthInBytes(120), 16);
    ASSERT_EQ(BinaryRow::CalculateBitSetWidthInBytes(121), 24);
}

TEST_F(BinaryRowTest, TestWriter) {
    auto pool = GetDefaultPool();
    int32_t arity = 13;
    BinaryRow row(arity);
    BinaryRowWriter writer(&row, 20, pool.get());
    writer.WriteString(0, BinaryString::FromString("1", pool.get()));
    writer.WriteString(3, BinaryString::FromString("1234567", pool.get()));
    writer.WriteString(5, BinaryString::FromString("12345678", pool.get()));
    writer.WriteString(9, BinaryString::FromString("啦啦啦啦啦我是快乐的粉刷匠", pool.get()));

    writer.WriteBoolean(1, true);
    writer.WriteByte(2, static_cast<char>(99));
    writer.WriteDouble(6, 87.1);
    writer.WriteFloat(7, 26.1f);
    writer.WriteInt(8, 88);
    writer.WriteLong(10, 284);
    writer.WriteShort(11, static_cast<int16_t>(292));
    writer.SetNullAt(12);

    writer.Complete();

    AssertTestWriterRow(row);
    AssertTestWriterRow(row.Copy(pool.get()));

    // test copy from var segments.
    int32_t sub_size = row.GetFixedLengthPartSize() + 10;

    auto bytes1 = Bytes::AllocateBytes(sub_size, pool.get());
    auto bytes2 = Bytes::AllocateBytes(sub_size, pool.get());
    MemorySegment sub_ms1 = MemorySegment::Wrap(std::move(bytes1));
    MemorySegment sub_ms2 = MemorySegment::Wrap(std::move(bytes2));
    row.GetSegments()[0].CopyTo(0, &sub_ms1, 0, sub_size);
    row.GetSegments()[0].CopyTo(sub_size, &sub_ms2, 0, row.GetSizeInBytes() - sub_size);

    std::vector<MemorySegment> segs = {sub_ms1, sub_ms2};
    BinaryRow to_copy(arity);
    to_copy.PointTo(segs, 0, row.GetSizeInBytes());
    ASSERT_EQ(to_copy.HashCode(), row.HashCode());
    AssertTestWriterRow(to_copy);
    BinaryRow new_row(arity);
    to_copy.Copy(&new_row, pool.get());
    AssertTestWriterRow(new_row);
}

TEST_F(BinaryRowTest, TestWriter2) {
    // test write multi segments to var len parts
    auto pool = GetDefaultPool();
    int32_t arity = 1;
    BinaryRow row(arity);
    BinaryRowWriter writer(&row, 100, pool.get());

    std::string str1 = "Strive not to be a success, ";
    std::string str2 = "but rather to be of value.";
    auto bytes1 = std::make_shared<Bytes>(str1, pool.get());
    auto bytes2 = std::make_shared<Bytes>(str2, pool.get());
    std::vector<MemorySegment> mem_segs({MemorySegment::Wrap(bytes1), MemorySegment::Wrap(bytes2)});
    auto binary_string = BinaryString::FromAddress(mem_segs, /*offset=*/2,
                                                   /*num_bytes=*/str1.length() + str2.length() - 2);
    writer.WriteString(0, binary_string);
    writer.Complete();

    ASSERT_EQ(row.GetString(0).ToString(), "rive not to be a success, but rather to be of value.");
}

TEST_F(BinaryRowTest, TestWriteString) {
    auto pool = GetDefaultPool();
    {
        // little byte[]
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 0, pool.get());
        char chars[2];
        chars[0] = static_cast<char>(0xFFFF);
        chars[1] = 0;
        writer.WriteString(0, BinaryString::FromString(std::string(chars, 2), pool.get()));
        writer.Complete();
        std::string str = row.GetString(0).ToString();
        ASSERT_EQ(2, str.length());
        ASSERT_EQ(str[0], chars[0]);
        ASSERT_EQ(str[1], chars[1]);
    }
    {
        // big byte[]
        std::string str = "啦啦啦啦啦我是快乐的粉刷匠";
        BinaryRow row(2);
        BinaryRowWriter writer(&row, 0, pool.get());
        writer.WriteString(0, BinaryString::FromString(str, pool.get()));
        writer.WriteString(1, BinaryString::FromBytes(Bytes::AllocateBytes(str, pool.get())));
        writer.Complete();

        ASSERT_EQ(row.GetString(0).ToString(), str);
        ASSERT_EQ(row.GetString(1).ToString(), str);
    }
}

TEST_F(BinaryRowTest, TestWriteBytes) {
    auto pool = GetDefaultPool();
    {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 0, pool.get());
        std::string str = "啦啦啦啦啦我是快乐的粉刷匠";
        Bytes bytes(str, pool.get());
        writer.WriteBytes(0, bytes);
        writer.Complete();
        ASSERT_EQ(row.GetString(0).ToString(), str);
    }
    {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 0, pool.get());
        std::string str = "啦";
        Bytes bytes(str, pool.get());
        writer.WriteBytes(0, bytes);
        writer.Complete();
        ASSERT_EQ(row.GetString(0).ToString(), str);
    }
}

TEST_F(BinaryRowTest, TestReuseWriter) {
    auto pool = GetDefaultPool();
    BinaryRow row(2);
    BinaryRowWriter writer(&row, 0, pool.get());
    writer.WriteString(0, BinaryString::FromString("01234567", pool.get()));
    writer.WriteString(1, BinaryString::FromString("012345678", pool.get()));
    writer.Complete();
    ASSERT_EQ(row.GetString(0).ToString(), "01234567");
    ASSERT_EQ(row.GetString(1).ToString(), "012345678");

    writer.Reset();
    writer.WriteString(0, BinaryString::FromString("1", pool.get()));
    writer.WriteString(1, BinaryString::FromString("0123456789", pool.get()));
    writer.Complete();
    ASSERT_EQ(row.GetString(0).ToString(), "1");
    ASSERT_EQ(row.GetString(1).ToString(), "0123456789");
}

TEST_F(BinaryRowTest, TestAnyNull) {
    auto pool = GetDefaultPool();
    {
        BinaryRow row(3);
        BinaryRowWriter writer(&row, 0, pool.get());

        ASSERT_FALSE(row.AnyNull());

        // test header should not compute by anyNull
        row.SetRowKind(RowKind::UpdateBefore());
        ASSERT_FALSE(row.AnyNull());

        writer.SetNullAt(2);
        ASSERT_TRUE(row.AnyNull());

        writer.SetNullAt(0);
        ASSERT_TRUE(row.AnyNull({0, 1, 2}));
        ASSERT_FALSE(row.AnyNull({1}));

        writer.SetNullAt(1);
        ASSERT_TRUE(row.AnyNull());
    }

    int32_t num_fields = 80;
    for (int32_t i = 0; i < num_fields; i++) {
        BinaryRow row(num_fields);
        BinaryRowWriter writer(&row, 0, pool.get());
        row.SetRowKind(RowKind::Delete());
        ASSERT_FALSE(row.AnyNull());
        writer.SetNullAt(i);
        ASSERT_TRUE(row.AnyNull());
    }
}

TEST_F(BinaryRowTest, TestSingleSegmentBinaryRowHashCode) {
    auto pool = GetDefaultPool();
    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    // test hash stabilization
    BinaryRow row(13);
    BinaryRowWriter writer(&row, 0, pool.get());
    for (int32_t i = 0; i < 99; i++) {
        writer.Reset();
        writer.WriteString(
            0, BinaryString::FromString("" + std::to_string(static_cast<int32_t>(std::rand())),
                                        pool.get()));
        writer.WriteString(3, BinaryString::FromString("01234567", pool.get()));
        writer.WriteString(5, BinaryString::FromString("012345678", pool.get()));
        writer.WriteString(9, BinaryString::FromString("啦啦啦啦啦我是快乐的粉刷匠", pool.get()));
        writer.WriteBoolean(1, true);
        writer.WriteByte(2, static_cast<char>(99));
        writer.WriteDouble(6, 87.1);
        writer.WriteFloat(7, 26.1f);
        writer.WriteInt(8, 88);
        writer.WriteLong(10, 284);
        writer.WriteShort(11, static_cast<int16_t>(292));
        writer.SetNullAt(12);
        writer.Complete();
        BinaryRow copy = row.Copy(pool.get());
        ASSERT_EQ(copy.HashCode(), row.HashCode()) << "seed: " << seed << ", idx: " << i;
    }

    // test hash distribution
    int32_t count = 999999;
    std::set<int32_t> hash_codes;
    for (int32_t i = 0; i < count; i++) {
        row.SetInt(8, i);
        hash_codes.insert(row.HashCode());
    }
    ASSERT_EQ(hash_codes.size(), count);
    hash_codes.clear();
    BinaryRow row2(1);
    BinaryRowWriter writer2(&row2, 0, pool.get());
    for (int32_t i = 0; i < count; i++) {
        writer2.Reset();
        writer2.WriteString(0, BinaryString::FromString(
                                   "啦啦啦啦啦我是快乐的粉刷匠" + std::to_string(i), pool.get()));
        writer2.Complete();
        hash_codes.insert(row2.HashCode());
    }
    ASSERT_GT(hash_codes.size(), static_cast<size_t>(count * 0.997));
}

TEST_F(BinaryRowTest, TestHeader) {
    auto pool = GetDefaultPool();
    BinaryRow row(2);
    BinaryRowWriter writer(&row, 0, pool.get());

    writer.WriteInt(0, 10);
    writer.SetNullAt(1);
    writer.WriteRowKind(RowKind::UpdateBefore());
    writer.Complete();

    BinaryRow new_row = row.Copy(pool.get());
    ASSERT_EQ(new_row, row);
    ASSERT_EQ(new_row.GetRowKind().value(), RowKind::UpdateBefore());

    new_row.SetRowKind(RowKind::Delete());
    ASSERT_EQ(new_row.GetRowKind().value(), RowKind::Delete());
}

TEST_F(BinaryRowTest, TestDefaultRowKind) {
    auto pool = GetDefaultPool();
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool.get());
    writer.WriteInt(0, 10);
    writer.Complete();
    ASSERT_EQ(row.GetRowKind().value(), RowKind::Insert());
}

TEST_F(BinaryRowTest, TestBinary) {
    auto pool = GetDefaultPool();
    BinaryRow row(2);
    BinaryRowWriter writer(&row, 0, pool.get());
    char chars1[3] = {1, -1, 5};
    char chars2[8] = {1, -1, 5, 5, 1, 5, 1, 5};
    std::string str1(chars1, 3);
    std::string str2(chars2, 8);
    Bytes bytes1(str1, pool.get());
    Bytes bytes2(str2, pool.get());

    writer.WriteBinary(0, bytes1);
    writer.WriteBinary(1, bytes2);
    writer.Complete();

    ASSERT_EQ(*row.GetBinary(0), bytes1);
    ASSERT_EQ(*row.GetBinary(1), bytes2);
}

TEST_F(BinaryRowTest, TestCompatibleWithJava) {
    auto pool = GetDefaultPool();
    {
        int32_t arity = 1;
        BinaryRow row(arity);
        BinaryRowWriter writer(&row, 0, pool.get());
        writer.WriteString(0, BinaryString::FromString("Alice2", pool.get()));
        writer.Complete();

        ASSERT_EQ(row.GetString(0).ToString(), "Alice2");
        auto bytes = SerializationUtils::SerializeBinaryRow(row, pool.get());

        std::string bytes_view(bytes->data(), bytes->size());
        std::vector<uint8_t> expect_bytes = {0, 0, 0,  1,   0,   0,  0,   0,  0, 0,
                                             0, 0, 65, 108, 105, 99, 101, 50, 0, 134};
        std::string expect_view(reinterpret_cast<char*>(expect_bytes.data()), expect_bytes.size());
        ASSERT_EQ(bytes_view, expect_view);

        ASSERT_OK_AND_ASSIGN(auto de_row, SerializationUtils::DeserializeBinaryRow(bytes));
        ASSERT_EQ(1, de_row.GetFieldCount());
        ASSERT_EQ(de_row.GetString(0).ToString(), "Alice2");
    }
    {
        int32_t arity = 1;
        BinaryRow row(arity);
        BinaryRowWriter writer(&row, 0, pool.get());
        writer.WriteInt(0, 18);
        writer.Complete();

        ASSERT_EQ(row.GetInt(0), 18);

        auto bytes = SerializationUtils::SerializeBinaryRow(row, pool.get());
        std::string bytes_view(bytes->data(), bytes->size());
        std::vector<uint8_t> expect_bytes = {0, 0, 0,  1, 0, 0, 0, 0, 0, 0,
                                             0, 0, 18, 0, 0, 0, 0, 0, 0, 0};
        std::string expect_view(reinterpret_cast<char*>(expect_bytes.data()), expect_bytes.size());
        ASSERT_EQ(bytes_view, expect_view);
        ASSERT_OK_AND_ASSIGN(auto de_row, SerializationUtils::DeserializeBinaryRow(bytes));
        ASSERT_EQ(1, de_row.GetFieldCount());
        ASSERT_EQ(de_row.GetInt(0), 18);
    }
}

TEST_F(BinaryRowTest, TestZeroOutPaddingString) {
    srand(static_cast<unsigned>(time(nullptr)));
    auto pool = GetDefaultPool();
    int32_t bytes_size = 1024;
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(1024, pool.get());

    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool.get());

    writer.Reset();
    for (int32_t i = 0; i < bytes_size; i++) {
        (*bytes)[i] = paimon::test::RandomNumber(0, 255);
    }
    writer.WriteBinary(0, *bytes);
    writer.Reset();
    writer.WriteString(0, BinaryString::FromString("wahahah", pool.get()));
    writer.Complete();
    int hash1 = row.HashCode();

    writer.Reset();
    for (int32_t i = 0; i < bytes_size; i++) {
        (*bytes)[i] = paimon::test::RandomNumber(0, 255);
    }
    writer.WriteBinary(0, *bytes);
    writer.Reset();
    writer.WriteString(0, BinaryString::FromString("wahahah", pool.get()));
    writer.Complete();
    int hash2 = row.HashCode();

    ASSERT_EQ(hash2, hash1);
}

TEST_F(BinaryRowTest, TestWriteTimestampAndDecimal) {
    auto pool = GetDefaultPool();
    BinaryRow row(7);
    BinaryRowWriter writer(&row, 0, pool.get());
    // timestamp with millis precision, compact
    Timestamp timestamp(1723535714123ll, 0);
    writer.WriteTimestamp(0, timestamp, Timestamp::MILLIS_PRECISION);
    // timestamp with default precision, not compact
    Timestamp timestamp1(1723535713000ll, 1234);
    writer.WriteTimestamp(1, timestamp1, Timestamp::DEFAULT_PRECISION);
    // 1234.56, compact, precision 6, scale 2
    Decimal decimal(6, 2, 123456);
    writer.WriteDecimal(2, decimal, 6);

    // 123456789987654321.45678, not compact, precision 23, scale 5
    Decimal decimal3(23, 5, DecimalUtils::StrToInt128("12345678998765432145678").value());
    writer.WriteDecimal(3, decimal3, 23);

    // 0, not compact, precision 27, scale 4
    Decimal decimal4(27, 4, 0);
    writer.WriteDecimal(4, decimal4, 27);
    // -1234.56, compact, precision 6, scale 2
    Decimal decimal5(6, 2, -123456);
    writer.WriteDecimal(5, decimal5, 6);
    // -123456789987654321.45678, not compact, precision 23, scale 5
    Decimal decimal6(23, 5, DecimalUtils::StrToInt128("-12345678998765432145678").value());
    writer.WriteDecimal(6, decimal6, 23);
    writer.Complete();

    // test serialize
    std::vector<uint8_t> expected = {
        0,   0,  0, 0, 0,   0,   0,   0,  75,  231, 187, 74,  145, 1,   0,   0,   210, 4,  0,   0,
        64,  0,  0, 0, 64,  226, 1,   0,  0,   0,   0,   0,   10,  0,   0,   0,   72,  0,  0,   0,
        1,   0,  0, 0, 88,  0,   0,   0,  192, 29,  254, 255, 255, 255, 255, 255, 10,  0,  0,   0,
        104, 0,  0, 0, 232, 226, 187, 74, 145, 1,   0,   0,   2,   157, 66,  182, 167, 42, 157, 199,
        7,   14, 0, 0, 0,   0,   0,   0,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,   0,
        0,   0,  0, 0, 253, 98,  189, 73, 88,  213, 98,  56,  248, 242, 0,   0,   0,   0,  0,   0};
    auto result_bytes = row.ToBytes(pool.get());
    std::vector<uint8_t> result(result_bytes->size(), 0);
    memcpy(result.data(), result_bytes->data(), result_bytes->size());
    ASSERT_EQ(expected, result);
    // test hash code
    ASSERT_EQ(0x773cf266, row.HashCode());

    ASSERT_EQ(timestamp, row.GetTimestamp(0, Timestamp::MILLIS_PRECISION));
    ASSERT_EQ(timestamp1, row.GetTimestamp(1, Timestamp::DEFAULT_PRECISION));
    ASSERT_EQ(decimal, row.GetDecimal(2, 6, 2));
    ASSERT_EQ(decimal3, row.GetDecimal(3, 23, 5));
    ASSERT_EQ(decimal4, row.GetDecimal(4, 27, 4));
    ASSERT_EQ(decimal5, row.GetDecimal(5, 6, 2));
    ASSERT_EQ(decimal6, row.GetDecimal(6, 23, 5));
}

TEST_F(BinaryRowTest, TestWriteTimestampAndDecimalWithNull) {
    auto pool = GetDefaultPool();
    BinaryRow row(5);
    BinaryRowWriter writer(&row, 0, pool.get());
    writer.Reset();

    // timestamp with max precision, compact
    writer.WriteTimestamp(0, std::nullopt, Timestamp::MAX_PRECISION);
    // timestamp with MAX_PRECISION, compact
    Timestamp timestamp(1723535713567ll, 1234);
    writer.WriteTimestamp(1, timestamp, Timestamp::MAX_PRECISION);

    // decimal precision 27, not compact
    writer.WriteDecimal(2, std::nullopt, 27);
    // -123456789987654321.45678, not compact, precision 23, scale 5
    Decimal decimal(23, 5, DecimalUtils::StrToInt128("-12345678998765432145678").value());
    writer.WriteDecimal(3, decimal, 23);

    writer.SetNullAt(4);
    writer.Complete();

    // test serialize
    std::vector<uint8_t> expected = {
        0, 21, 0, 0, 0,  0, 0, 0, 0,   0,   0,   0,  48,  0,   0,  0,  210, 4,   0, 0, 56, 0, 0, 0,
        0, 0,  0, 0, 64, 0, 0, 0, 10,  0,   0,   0,  80,  0,   0,  0,  0,   0,   0, 0, 0,  0, 0, 0,
        0, 0,  0, 0, 0,  0, 0, 0, 31,  229, 187, 74, 145, 1,   0,  0,  0,   0,   0, 0, 0,  0, 0, 0,
        0, 0,  0, 0, 0,  0, 0, 0, 253, 98,  189, 73, 88,  213, 98, 56, 248, 242, 0, 0, 0,  0, 0, 0};
    auto result_bytes = row.ToBytes(pool.get());
    std::vector<uint8_t> result(result_bytes->size(), 0);
    memcpy(result.data(), result_bytes->data(), result_bytes->size());
    ASSERT_EQ(expected, result);
    // test hash code
    ASSERT_EQ(0xedbc7d65, row.HashCode());

    // test value in binary row
    ASSERT_TRUE(row.IsNullAt(0));
    ASSERT_EQ(timestamp, row.GetTimestamp(1, Timestamp::MAX_PRECISION));

    ASSERT_TRUE(row.IsNullAt(2));
    ASSERT_EQ(decimal, row.GetDecimal(3, 23, 5));
    ASSERT_TRUE(row.IsNullAt(4));
}

TEST_F(BinaryRowTest, TestBinaryRowSerializer) {
    srand(static_cast<unsigned>(time(nullptr)));
    auto pool = GetDefaultPool();
    BinaryRow row(3);
    int32_t bytes_size = 1024;
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(1024, pool.get());
    for (int32_t i = 0; i < bytes_size; i++) {
        (*bytes)[i] = paimon::test::RandomNumber(0, 255);
    }
    int32_t str_size = 1024;
    std::string test_string1, test_string2;
    test_string1.reserve(str_size);
    test_string2.reserve(str_size);
    for (int32_t j = 0; j < str_size; j++) {
        test_string1 += static_cast<char>(paimon::test::RandomNumber(0, 25) + 'a');
        test_string2 += static_cast<char>(paimon::test::RandomNumber(0, 25) + 'a');
    }
    std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes(test_string1, pool.get());
    std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes(test_string2, pool.get());
    std::vector<MemorySegment> segs = {MemorySegment::Wrap(bytes), MemorySegment::Wrap(bytes1),
                                       MemorySegment::Wrap(bytes2)};
    row.PointTo(segs, 0, bytes_size + test_string1.size() + test_string2.size());

    BinaryRowSerializer row_serializer_(3, pool);
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    ASSERT_OK(row_serializer_.Serialize(row, &out));
    PAIMON_UNIQUE_PTR<Bytes> bytes_serialize =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    std::string str_serialize = std::string(bytes_serialize->data(), bytes_serialize->size());
    ASSERT_GE(str_serialize.size(), bytes_size + test_string1.size() + test_string2.size());
    std::string str_serialize1 = str_serialize.substr(
        str_serialize.size() - test_string1.size() - test_string2.size(), test_string1.size());
    std::string str_serialize2 =
        str_serialize.substr(str_serialize.size() - test_string2.size(), test_string2.size());
    ASSERT_EQ(test_string1, str_serialize1);
    ASSERT_EQ(test_string2, str_serialize2);
}

TEST_F(BinaryRowTest, TestWriteWithMultiSegments) {
    auto pool = GetDefaultPool();
    std::string str1 = "Strive not to be a success, ";
    std::string str2 = "but rather to be of value.";
    auto bytes1 = std::make_shared<Bytes>(str1, pool.get());
    auto bytes2 = std::make_shared<Bytes>(str2, pool.get());
    std::vector<MemorySegment> mem_segs({MemorySegment::Wrap(bytes1), MemorySegment::Wrap(bytes2)});
    auto binary_string = BinaryString::FromAddress(mem_segs, /*offset=*/0,
                                                   /*num_bytes=*/str1.length() + str2.length());
    ASSERT_EQ(str1 + str2, binary_string.ToString());

    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool.get());
    writer.WriteString(0, binary_string);
    writer.Complete();
    ASSERT_EQ(str1 + str2, row.GetString(0).ToString());
}

}  // namespace paimon::test
