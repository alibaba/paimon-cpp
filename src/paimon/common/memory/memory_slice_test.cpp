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

#include "paimon/common/memory/memory_slice.h"

#include <cstdint>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/memory/memory_slice_output.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class MemorySliceTest : public ::testing::Test {
 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

// ==================== MemorySliceOutput Tests ====================

TEST_F(MemorySliceTest, TestOutputWriteValueAndToSlice) {
    MemorySliceOutput output(64, pool_.get());
    ASSERT_EQ(0, output.Size());

    output.WriteValue<int32_t>(42);
    output.WriteValue<int64_t>(123456789LL);
    output.WriteValue<int16_t>(static_cast<int16_t>(7));
    output.WriteValue<int8_t>(static_cast<int8_t>(-1));
    output.WriteValue<bool>(true);

    // 4 + 8 + 2 + 1 + 1 = 16
    ASSERT_EQ(16, output.Size());

    MemorySlice slice = output.ToSlice();
    ASSERT_EQ(16, slice.Length());
    ASSERT_EQ(0, slice.Offset());

    ASSERT_EQ(42, slice.ReadInt(0));
    ASSERT_EQ(123456789LL, slice.ReadLong(4));
    ASSERT_EQ(7, slice.ReadShort(12));
    ASSERT_EQ(-1, slice.ReadByte(14));
}

TEST_F(MemorySliceTest, TestOutputWriteBytes) {
    MemorySliceOutput output(16, pool_.get());

    std::string data = "hello world";
    auto bytes = std::make_shared<Bytes>(data, pool_.get());
    output.WriteBytes(bytes);
    ASSERT_EQ(static_cast<int32_t>(data.size()), output.Size());

    MemorySlice slice = output.ToSlice();
    ASSERT_EQ("hello world", slice.ReadStringView());
}

TEST_F(MemorySliceTest, TestOutputWriteBytesSubRange) {
    MemorySliceOutput output(16, pool_.get());

    std::string data = "hello world";
    auto bytes = std::make_shared<Bytes>(data, pool_.get());
    output.WriteBytes(bytes, 6, 5);
    ASSERT_EQ(5, output.Size());

    MemorySlice slice = output.ToSlice();
    ASSERT_EQ("world", slice.ReadStringView());
}

TEST_F(MemorySliceTest, TestOutputVarLenInt) {
    MemorySliceOutput output(32, pool_.get());

    ASSERT_OK(output.WriteVarLenInt(0));
    ASSERT_OK(output.WriteVarLenInt(127));
    ASSERT_OK(output.WriteVarLenInt(128));
    ASSERT_OK(output.WriteVarLenInt(16384));

    MemorySlice slice = output.ToSlice();
    MemorySliceInput input(slice);

    ASSERT_OK_AND_ASSIGN(int32_t v0, input.ReadVarLenInt());
    ASSERT_EQ(0, v0);
    ASSERT_OK_AND_ASSIGN(int32_t v127, input.ReadVarLenInt());
    ASSERT_EQ(127, v127);
    ASSERT_OK_AND_ASSIGN(int32_t v128, input.ReadVarLenInt());
    ASSERT_EQ(128, v128);
    ASSERT_OK_AND_ASSIGN(int32_t v16384, input.ReadVarLenInt());
    ASSERT_EQ(16384, v16384);
}

TEST_F(MemorySliceTest, TestOutputVarLenLong) {
    MemorySliceOutput output(32, pool_.get());

    ASSERT_OK(output.WriteVarLenLong(0));
    ASSERT_OK(output.WriteVarLenLong(127));
    ASSERT_OK(output.WriteVarLenLong(128));
    ASSERT_OK(output.WriteVarLenLong(1234567890123LL));

    MemorySlice slice = output.ToSlice();
    MemorySliceInput input(slice);

    ASSERT_OK_AND_ASSIGN(int64_t v0, input.ReadVarLenLong());
    ASSERT_EQ(0, v0);
    ASSERT_OK_AND_ASSIGN(int64_t v127, input.ReadVarLenLong());
    ASSERT_EQ(127, v127);
    ASSERT_OK_AND_ASSIGN(int64_t v128, input.ReadVarLenLong());
    ASSERT_EQ(128, v128);
    ASSERT_OK_AND_ASSIGN(int64_t v_large, input.ReadVarLenLong());
    ASSERT_EQ(1234567890123LL, v_large);
}

TEST_F(MemorySliceTest, TestOutputVarLenNegativeValue) {
    MemorySliceOutput output(16, pool_.get());

    ASSERT_NOK_WITH_MSG(output.WriteVarLenInt(-1), "negative value: v=-1");
    ASSERT_NOK_WITH_MSG(output.WriteVarLenLong(-2), "negative value: v=-2");
    ASSERT_EQ(0, output.Size());
}

TEST_F(MemorySliceTest, TestOutputReset) {
    MemorySliceOutput output(16, pool_.get());
    output.WriteValue<int32_t>(42);
    ASSERT_EQ(4, output.Size());

    output.Reset();
    ASSERT_EQ(0, output.Size());

    output.WriteValue<int64_t>(99);
    ASSERT_EQ(8, output.Size());
    MemorySlice slice = output.ToSlice();
    ASSERT_EQ(99, slice.ReadLong(0));
}

TEST_F(MemorySliceTest, TestOutputAutoGrow) {
    // Start with tiny buffer, force multiple grows
    MemorySliceOutput output(4, pool_.get());
    for (int32_t i = 0; i < 100; i++) {
        output.WriteValue<int32_t>(i);
    }
    ASSERT_EQ(400, output.Size());

    MemorySlice slice = output.ToSlice();
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(i, slice.ReadInt(i * 4));
    }
}

TEST_F(MemorySliceTest, TestOutputByteOrder) {
    {
        MemorySliceOutput output(16, pool_.get());
        output.SetOrder(ByteOrder::PAIMON_BIG_ENDIAN);
        output.WriteValue<int32_t>(0x01020304);
        MemorySlice slice = output.ToSlice();

        MemorySliceInput input(slice);
        input.SetOrder(ByteOrder::PAIMON_BIG_ENDIAN);
        ASSERT_EQ(0x01020304, input.ReadInt());
    }
    {
        MemorySliceOutput output(16, pool_.get());
        output.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
        output.WriteValue<int32_t>(0x01020304);
        MemorySlice slice = output.ToSlice();

        MemorySliceInput input(slice);
        input.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
        ASSERT_EQ(0x01020304, input.ReadInt());
    }
    {
        MemorySliceOutput output(16, pool_.get());
        output.SetOrder(ByteOrder::PAIMON_BIG_ENDIAN);
        output.WriteValue<int64_t>(123456789123456ll);
        MemorySlice slice = output.ToSlice();

        MemorySliceInput input(slice);
        input.SetOrder(ByteOrder::PAIMON_BIG_ENDIAN);
        ASSERT_EQ(123456789123456ll, input.ReadLong());
    }
    {
        MemorySliceOutput output(16, pool_.get());
        output.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
        output.WriteValue<int64_t>(123456789123456ll);
        MemorySlice slice = output.ToSlice();

        MemorySliceInput input(slice);
        input.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
        ASSERT_EQ(123456789123456ll, input.ReadLong());
    }
}

// ==================== MemorySlice Tests ====================

TEST_F(MemorySliceTest, TestSliceWrapBytes) {
    std::string data = "abcdefghij";
    auto bytes = std::make_shared<Bytes>(data, pool_.get());
    MemorySlice slice = MemorySlice::Wrap(bytes);

    ASSERT_EQ(10, slice.Length());
    ASSERT_EQ(0, slice.Offset());
    ASSERT_EQ("abcdefghij", slice.ReadStringView());
}

TEST_F(MemorySliceTest, TestSliceWrapSegment) {
    auto bytes = std::make_shared<Bytes>("hello", pool_.get());
    MemorySegment segment = MemorySegment::Wrap(bytes);
    MemorySlice slice = MemorySlice::Wrap(segment);

    ASSERT_EQ(5, slice.Length());
    ASSERT_EQ("hello", slice.ReadStringView());
}

TEST_F(MemorySliceTest, TestSliceSubSlice) {
    std::string data = "abcdefghij";
    auto bytes = std::make_shared<Bytes>(data, pool_.get());
    MemorySlice slice = MemorySlice::Wrap(bytes);

    MemorySlice sub = slice.Slice(3, 4);
    ASSERT_EQ(4, sub.Length());
    ASSERT_EQ(3, sub.Offset());
    ASSERT_EQ("defg", sub.ReadStringView());

    // Slice with same range returns equivalent slice
    MemorySlice same = slice.Slice(0, 10);
    ASSERT_EQ(10, same.Length());
    ASSERT_EQ("abcdefghij", same.ReadStringView());
}

TEST_F(MemorySliceTest, TestSliceReadPrimitives) {
    MemorySliceOutput output(32, pool_.get());
    output.WriteValue<int8_t>(static_cast<int8_t>(0x7F));
    output.WriteValue<int16_t>(static_cast<int16_t>(1234));
    output.WriteValue<int32_t>(56789);
    output.WriteValue<int64_t>(9876543210LL);

    MemorySlice slice = output.ToSlice();
    ASSERT_EQ(0x7F, slice.ReadByte(0));
    ASSERT_EQ(static_cast<int16_t>(1234), slice.ReadShort(1));
    ASSERT_EQ(56789, slice.ReadInt(3));
    ASSERT_EQ(9876543210LL, slice.ReadLong(7));
}

TEST_F(MemorySliceTest, TestSliceCopyBytes) {
    std::string data = "copy me";
    auto bytes = std::make_shared<Bytes>(data, pool_.get());

    MemorySlice slice = MemorySlice::Wrap(bytes);
    auto copied = slice.CopyBytes(pool_.get());
    ASSERT_EQ(data.size(), copied->size());
    ASSERT_EQ(data, std::string(copied->data(), copied->size()));
    // Verify it's a true copy (different pointer)
    ASSERT_NE(bytes->data(), copied->data());

    MemorySlice slice2(MemorySegment::Wrap(bytes), /*offset=*/5, /*length=*/2);
    copied = slice2.CopyBytes(pool_.get());
    ASSERT_EQ(2, copied->size());
    ASSERT_EQ("me", std::string(copied->data(), copied->size()));
    ASSERT_NE(bytes->data(), copied->data());
}

TEST_F(MemorySliceTest, TestSliceGetHeapMemory) {
    auto bytes = std::make_shared<Bytes>("test", pool_.get());
    MemorySlice slice = MemorySlice::Wrap(bytes);
    ASSERT_EQ(bytes, slice.GetHeapMemory());
}

TEST_F(MemorySliceTest, TestSliceToInput) {
    MemorySliceOutput output(16, pool_.get());
    output.WriteValue<int32_t>(42);
    output.WriteValue<int64_t>(99);

    MemorySlice slice = output.ToSlice();
    MemorySliceInput input = slice.ToInput();

    ASSERT_EQ(42, input.ReadInt());
    ASSERT_EQ(99, input.ReadLong());
}

// ==================== MemorySliceInput Tests ====================

TEST_F(MemorySliceTest, TestInputPositionAndAvailable) {
    MemorySliceOutput output(16, pool_.get());
    output.WriteValue<int32_t>(1);
    output.WriteValue<int32_t>(2);

    MemorySliceInput input(output.ToSlice());
    ASSERT_EQ(0, input.Position());
    ASSERT_EQ(8, input.Available());
    ASSERT_TRUE(input.IsReadable());

    input.ReadInt();
    ASSERT_EQ(4, input.Position());
    ASSERT_EQ(4, input.Available());
    ASSERT_TRUE(input.IsReadable());

    input.ReadInt();
    ASSERT_EQ(8, input.Position());
    ASSERT_EQ(0, input.Available());
    ASSERT_FALSE(input.IsReadable());
}

TEST_F(MemorySliceTest, TestInputSetPosition) {
    MemorySliceOutput output(16, pool_.get());
    output.WriteValue<int32_t>(111);
    output.WriteValue<int32_t>(222);

    MemorySliceInput input(output.ToSlice());
    input.ReadInt();
    ASSERT_EQ(4, input.Position());

    ASSERT_OK(input.SetPosition(0));
    ASSERT_EQ(0, input.Position());
    ASSERT_EQ(111, input.ReadInt());

    // Invalid positions
    ASSERT_NOK_WITH_MSG(input.SetPosition(-1), "position -1 index out of bounds");
    ASSERT_NOK_WITH_MSG(input.SetPosition(100), "position 100 index out of bounds");
}

TEST_F(MemorySliceTest, TestInputReadByte) {
    MemorySliceOutput output(8, pool_.get());
    output.WriteValue<int8_t>(static_cast<int8_t>(-128));
    output.WriteValue<int8_t>(static_cast<int8_t>(127));
    output.WriteValue<int8_t>(static_cast<int8_t>(0));

    MemorySliceInput input(output.ToSlice());
    ASSERT_EQ(-128, input.ReadByte());
    ASSERT_EQ(127, input.ReadByte());
    ASSERT_EQ(0, input.ReadByte());
}

TEST_F(MemorySliceTest, TestInputReadUnsignedByte) {
    MemorySliceOutput output(8, pool_.get());
    output.WriteValue<int8_t>(static_cast<int8_t>(0xFF));

    MemorySliceInput input(output.ToSlice());
    // ReadUnsignedByte masks with 0xFF
    int8_t value = input.ReadUnsignedByte();
    ASSERT_EQ(static_cast<int8_t>(0xFF & 0xFF), value);
}

TEST_F(MemorySliceTest, TestInputReadIntAndLong) {
    MemorySliceOutput output(32, pool_.get());
    output.WriteValue<int32_t>(0);
    output.WriteValue<int32_t>(INT32_MAX);
    output.WriteValue<int32_t>(INT32_MIN);
    output.WriteValue<int64_t>(0);
    output.WriteValue<int64_t>(INT64_MAX);
    output.WriteValue<int64_t>(INT64_MIN);

    MemorySliceInput input(output.ToSlice());
    ASSERT_EQ(0, input.ReadInt());
    ASSERT_EQ(INT32_MAX, input.ReadInt());
    ASSERT_EQ(INT32_MIN, input.ReadInt());
    ASSERT_EQ(0, input.ReadLong());
    ASSERT_EQ(INT64_MAX, input.ReadLong());
    ASSERT_EQ(INT64_MIN, input.ReadLong());
}

TEST_F(MemorySliceTest, TestInputReadSlice) {
    std::string data = "abcdefghij";
    auto bytes = std::make_shared<Bytes>(data, pool_.get());
    MemorySlice slice = MemorySlice::Wrap(bytes);
    MemorySliceInput input(slice);

    MemorySlice sub = input.ReadSlice(5);
    ASSERT_EQ("abcde", sub.ReadStringView());
    ASSERT_EQ(5, input.Position());

    MemorySlice sub2 = input.ReadSlice(5);
    ASSERT_EQ("fghij", sub2.ReadStringView());
    ASSERT_EQ(10, input.Position());
}

// ==================== Round-trip Output → Input Tests ====================

TEST_F(MemorySliceTest, TestRoundTripMixedTypes) {
    MemorySliceOutput output(64, pool_.get());
    output.WriteValue<int8_t>(static_cast<int8_t>(42));
    output.WriteValue<int16_t>(static_cast<int16_t>(1000));
    output.WriteValue<int32_t>(123456);
    output.WriteValue<int64_t>(9876543210LL);
    ASSERT_OK(output.WriteVarLenInt(300));
    ASSERT_OK(output.WriteVarLenLong(1000000LL));

    auto bytes = std::make_shared<Bytes>("test", pool_.get());
    output.WriteValue<int32_t>(static_cast<int32_t>(bytes->size()));
    output.WriteBytes(bytes);

    MemorySlice slice = output.ToSlice();
    MemorySliceInput input(slice);
    ASSERT_EQ(42, input.ReadByte());
    // int16_t written at position 1, read via slice
    ASSERT_EQ(1000, slice.ReadShort(1));
    ASSERT_OK(input.SetPosition(3));
    ASSERT_EQ(123456, input.ReadInt());
    ASSERT_OK(input.SetPosition(3));
    ASSERT_EQ(123456, input.ReadInt());
    ASSERT_EQ(9876543210LL, input.ReadLong());

    ASSERT_OK_AND_ASSIGN(int32_t var_int, input.ReadVarLenInt());
    ASSERT_EQ(300, var_int);
    ASSERT_OK_AND_ASSIGN(int64_t var_long, input.ReadVarLenLong());
    ASSERT_EQ(1000000LL, var_long);

    int32_t str_len = input.ReadInt();
    ASSERT_EQ(4, str_len);
    MemorySlice str_slice = input.ReadSlice(str_len);
    ASSERT_EQ("test", str_slice.ReadStringView());
}

}  // namespace paimon::test
