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

#include "paimon/utils/roaring_bitmap64.h"

#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(RoaringBitmap64Test, TestSimple) {
    RoaringBitmap64 roaring;
    ASSERT_TRUE(roaring.IsEmpty());
    roaring.Add(4147483647l);
    roaring.Add(614748364721l);
    ASSERT_TRUE(roaring.Contains(4147483647l));
    ASSERT_TRUE(roaring.Contains(614748364721l));
    ASSERT_FALSE(roaring.CheckedAdd(614748364721l));
    ASSERT_TRUE(roaring.CheckedAdd(8147483647210l));
    ASSERT_TRUE(roaring.Contains(614748364721l));
    ASSERT_FALSE(roaring.IsEmpty());
    ASSERT_EQ("{4147483647,614748364721,8147483647210}", roaring.ToString());
    ASSERT_EQ(3, roaring.Cardinality());
    roaring.AddRange(614748364720, 614748364725);
    ASSERT_EQ(
        "{4147483647,614748364720,614748364721,614748364722,614748364723,614748364724,"
        "8147483647210}",
        roaring.ToString());
    ASSERT_EQ(7, roaring.Cardinality());
    roaring.RemoveRange(614748364721l, 614748364723l);
    ASSERT_EQ("{4147483647,614748364720,614748364723,614748364724,8147483647210}",
              roaring.ToString());
    ASSERT_EQ(5, roaring.Cardinality());
}
TEST(RoaringBitmap64Test, TestCompatibleWithJava) {
    auto pool = GetDefaultPool();
    {
        RoaringBitmap64 roaring;
        for (int64_t i = 0; i < 100; i++) {
            roaring.Add(i + 100000000000l);
        }
        for (int64_t i = 0; i < 100; i++) {
            ASSERT_TRUE(roaring.Contains(i + 100000000000l));
        }
        auto bytes = roaring.Serialize(pool.get());
        std::vector<uint8_t> result(bytes->data(), bytes->data() + bytes->size());
        std::vector<uint8_t> expected = {1, 0, 0, 0,   0,  0,  0, 0, 23, 0, 0,   0,  59, 48,
                                         0, 0, 1, 118, 72, 99, 0, 1, 0,  0, 232, 99, 0};
        ASSERT_EQ(result, expected);

        RoaringBitmap64 de_roaring;
        ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
        for (int64_t i = 0; i < 100; i++) {
            ASSERT_TRUE(de_roaring.Contains(i + 100000000000l));
        }
    }
    {
        RoaringBitmap64 roaring;
        for (int64_t i = RoaringBitmap64::MAX_VALUE - 1; i > RoaringBitmap64::MAX_VALUE - 101;
             i--) {
            roaring.Add(i);
        }
        for (int64_t i = RoaringBitmap64::MAX_VALUE - 1; i > RoaringBitmap64::MAX_VALUE - 101;
             i--) {
            ASSERT_TRUE(roaring.Contains(i));
        }
        auto bytes = roaring.Serialize(pool.get());
        std::vector<uint8_t> result(bytes->data(), bytes->data() + bytes->size());
        std::vector<uint8_t> expected = {1, 0, 0, 0,   0,   0,  0, 0, 255, 255, 255, 127, 59, 48,
                                         0, 0, 1, 255, 255, 99, 0, 1, 0,   155, 255, 99,  0};
        ASSERT_EQ(result, expected);

        RoaringBitmap64 de_roaring;
        ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
        for (int64_t i = RoaringBitmap64::MAX_VALUE - 1; i > RoaringBitmap64::MAX_VALUE - 101;
             i--) {
            ASSERT_TRUE(de_roaring.Contains(i));
        }
    }
    {
        RoaringBitmap64 roaring;
        for (int64_t i = 5000; i < 10000; i += 17) {
            roaring.Add(i + 300000000000l);
        }
        for (int64_t i = 5000; i < 10000; i += 17) {
            ASSERT_TRUE(roaring.Contains(i + 300000000000l));
        }
        auto bytes = roaring.Serialize(pool.get());
        std::vector<uint8_t> result(bytes->data(), bytes->data() + bytes->size());
        std::vector<uint8_t> expected = {
            1,   0,   0,   0,   0,   0,   0,   0,   69,  0,   0,   0,   58,  48,  0,   0,   1,
            0,   0,   0,   100, 217, 38,  1,   16,  0,   0,   0,   136, 203, 153, 203, 170, 203,
            187, 203, 204, 203, 221, 203, 238, 203, 255, 203, 16,  204, 33,  204, 50,  204, 67,
            204, 84,  204, 101, 204, 118, 204, 135, 204, 152, 204, 169, 204, 186, 204, 203, 204,
            220, 204, 237, 204, 254, 204, 15,  205, 32,  205, 49,  205, 66,  205, 83,  205, 100,
            205, 117, 205, 134, 205, 151, 205, 168, 205, 185, 205, 202, 205, 219, 205, 236, 205,
            253, 205, 14,  206, 31,  206, 48,  206, 65,  206, 82,  206, 99,  206, 116, 206, 133,
            206, 150, 206, 167, 206, 184, 206, 201, 206, 218, 206, 235, 206, 252, 206, 13,  207,
            30,  207, 47,  207, 64,  207, 81,  207, 98,  207, 115, 207, 132, 207, 149, 207, 166,
            207, 183, 207, 200, 207, 217, 207, 234, 207, 251, 207, 12,  208, 29,  208, 46,  208,
            63,  208, 80,  208, 97,  208, 114, 208, 131, 208, 148, 208, 165, 208, 182, 208, 199,
            208, 216, 208, 233, 208, 250, 208, 11,  209, 28,  209, 45,  209, 62,  209, 79,  209,
            96,  209, 113, 209, 130, 209, 147, 209, 164, 209, 181, 209, 198, 209, 215, 209, 232,
            209, 249, 209, 10,  210, 27,  210, 44,  210, 61,  210, 78,  210, 95,  210, 112, 210,
            129, 210, 146, 210, 163, 210, 180, 210, 197, 210, 214, 210, 231, 210, 248, 210, 9,
            211, 26,  211, 43,  211, 60,  211, 77,  211, 94,  211, 111, 211, 128, 211, 145, 211,
            162, 211, 179, 211, 196, 211, 213, 211, 230, 211, 247, 211, 8,   212, 25,  212, 42,
            212, 59,  212, 76,  212, 93,  212, 110, 212, 127, 212, 144, 212, 161, 212, 178, 212,
            195, 212, 212, 212, 229, 212, 246, 212, 7,   213, 24,  213, 41,  213, 58,  213, 75,
            213, 92,  213, 109, 213, 126, 213, 143, 213, 160, 213, 177, 213, 194, 213, 211, 213,
            228, 213, 245, 213, 6,   214, 23,  214, 40,  214, 57,  214, 74,  214, 91,  214, 108,
            214, 125, 214, 142, 214, 159, 214, 176, 214, 193, 214, 210, 214, 227, 214, 244, 214,
            5,   215, 22,  215, 39,  215, 56,  215, 73,  215, 90,  215, 107, 215, 124, 215, 141,
            215, 158, 215, 175, 215, 192, 215, 209, 215, 226, 215, 243, 215, 4,   216, 21,  216,
            38,  216, 55,  216, 72,  216, 89,  216, 106, 216, 123, 216, 140, 216, 157, 216, 174,
            216, 191, 216, 208, 216, 225, 216, 242, 216, 3,   217, 20,  217, 37,  217, 54,  217,
            71,  217, 88,  217, 105, 217, 122, 217, 139, 217, 156, 217, 173, 217, 190, 217, 207,
            217, 224, 217, 241, 217, 2,   218, 19,  218, 36,  218, 53,  218, 70,  218, 87,  218,
            104, 218, 121, 218, 138, 218, 155, 218, 172, 218, 189, 218, 206, 218, 223, 218, 240,
            218, 1,   219, 18,  219, 35,  219, 52,  219, 69,  219, 86,  219, 103, 219, 120, 219,
            137, 219, 154, 219, 171, 219, 188, 219, 205, 219, 222, 219, 239, 219, 0,   220, 17,
            220, 34,  220, 51,  220, 68,  220, 85,  220, 102, 220, 119, 220, 136, 220, 153, 220,
            170, 220, 187, 220, 204, 220, 221, 220, 238, 220, 255, 220, 16,  221, 33,  221, 50,
            221, 67,  221, 84,  221, 101, 221, 118, 221, 135, 221, 152, 221, 169, 221, 186, 221,
            203, 221, 220, 221, 237, 221, 254, 221, 15,  222, 32,  222, 49,  222, 66,  222, 83,
            222, 100, 222, 117, 222, 134, 222, 151, 222, 168, 222, 185, 222, 202, 222, 219, 222,
            236, 222, 253, 222, 14,  223};
        ASSERT_EQ(result, expected);

        RoaringBitmap64 de_roaring;
        ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
        for (int64_t i = 5000; i < 10000; i += 17) {
            ASSERT_TRUE(de_roaring.Contains(i + 300000000000l));
        }
    }
    // empty
    {
        RoaringBitmap64 roaring;
        auto bytes = roaring.Serialize(pool.get());
        std::vector<uint8_t> result(bytes->data(), bytes->data() + bytes->size());
        std::vector<uint8_t> expected = {0, 0, 0, 0, 0, 0, 0, 0};
        ASSERT_EQ(result, expected);
        RoaringBitmap64 de_roaring;
        ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
        ASSERT_TRUE(de_roaring.IsEmpty());
        ASSERT_FALSE(de_roaring.Contains(58));
    }
}

TEST(RoaringBitmap64Test, TestDeserializeFailed) {
    RoaringBitmap64 roaring;
    std::vector<char> invalid_bytes = {0, 100};
    ASSERT_NOK_WITH_MSG(roaring.Deserialize(invalid_bytes.data(), invalid_bytes.size()),
                        "catch exception in Deserialize() of RoaringBitmap64");
}

TEST(RoaringBitmap64Test, TestAndOr) {
    RoaringBitmap64 roaring1 = RoaringBitmap64::From({100000000000l, 100000000000000l});
    RoaringBitmap64 roaring2 =
        RoaringBitmap64::From({200000000000l, 100000000000000l, 200000000000000l});

    auto and_roaring = RoaringBitmap64::And(roaring1, roaring2);
    ASSERT_EQ(and_roaring, RoaringBitmap64::From({100000000000000l}));

    auto or_roaring = RoaringBitmap64::Or(roaring1, roaring2);
    ASSERT_EQ(or_roaring, RoaringBitmap64::From(
                              {100000000000l, 200000000000l, 100000000000000l, 200000000000000l}));
}

TEST(RoaringBitmap64Test, TestAssignAndMove) {
    RoaringBitmap64 roaring1 = RoaringBitmap64::From({100000000000l, 200000000000000l});
    RoaringBitmap64 roaring2 = roaring1;
    ASSERT_EQ(roaring1, roaring2);
    ASSERT_FALSE(roaring1.IsEmpty());
    ASSERT_FALSE(roaring2.IsEmpty());

    RoaringBitmap64 roaring3 = std::move(roaring1);
    ASSERT_EQ(roaring3, roaring2);
    ASSERT_FALSE(roaring1.roaring_bitmap_);  // NOLINT(bugprone-use-after-move)

    roaring3.Add(20);
    ASSERT_FALSE(roaring3 == roaring2);

    roaring3 = roaring2;
    ASSERT_EQ(roaring3, roaring2);

    roaring2 = std::move(roaring3);
    ASSERT_FALSE(roaring3.roaring_bitmap_);  // NOLINT(bugprone-use-after-move)
    ASSERT_EQ("{100000000000,200000000000000}", roaring2.ToString());

    roaring3 = roaring2;
    ASSERT_EQ("{100000000000,200000000000000}", roaring3.ToString());

    roaring3 = std::move(roaring2);
    ASSERT_EQ("{100000000000,200000000000000}", roaring3.ToString());
    ASSERT_FALSE(roaring2.roaring_bitmap_);  // NOLINT(bugprone-use-after-move)
}

TEST(RoaringBitmap64Test, TestFastUnion) {
    RoaringBitmap64 roaring1 = RoaringBitmap64::From({100000000000l, 200000000000000l});
    RoaringBitmap64 roaring2 = RoaringBitmap64::From(
        {1l, 100000000000l, 150000000000l, 200000000000000l, 300000000000000l});
    RoaringBitmap64 roaring3 = RoaringBitmap64::From({2l, 200000000000000l, 800l});

    RoaringBitmap64 res = RoaringBitmap64::FastUnion({&roaring1, &roaring2, &roaring3});
    ASSERT_EQ(res, RoaringBitmap64::From({1l, 2l, 100000000000l, 150000000000l, 200000000000000l,
                                          300000000000000l, 800l}));

    std::vector<RoaringBitmap64> roarings = {roaring1, roaring2, roaring3};
    RoaringBitmap64 res2 = RoaringBitmap64::FastUnion(roarings);
    ASSERT_EQ(res2, RoaringBitmap64::From({1l, 2l, 100000000000l, 150000000000l, 200000000000000l,
                                           300000000000000l, 800l}));
}

TEST(RoaringBitmap64Test, TestFlip) {
    RoaringBitmap64 roaring =
        RoaringBitmap64::From({1000000000001l, 1000000000002l, 1000000000004l});
    roaring.Flip(1000000000000l, 1000000000005l);
    ASSERT_EQ(roaring, RoaringBitmap64::From({1000000000000l, 1000000000003l}));
}

TEST(RoaringBitmap64Test, TestAndNot) {
    RoaringBitmap64 roaring1 = RoaringBitmap64::From(
        {10000000000010l, 10000000000020l, 100000000000100l, 100000000000200l});
    RoaringBitmap64 roaring2 = RoaringBitmap64::From(
        {1000000000005l, 10000000000020l, 10000000000080l, 100000000000200l, 100000000000250l});

    auto andnot_roaring = RoaringBitmap64::AndNot(roaring1, roaring2);
    ASSERT_EQ(andnot_roaring, RoaringBitmap64::From({10000000000010l, 100000000000100l}));
}

TEST(RoaringBitmap64Test, TestIterator) {
    RoaringBitmap64 roaring = RoaringBitmap64::From({100000000000l, 200000000000l});
    auto iter = roaring.Begin();
    ASSERT_EQ(*iter, 100000000000l);
    auto iter2 = ++iter;
    ASSERT_EQ(*iter, 200000000000l);
    ASSERT_EQ(*iter2, 200000000000l);
    ++iter;
    ASSERT_EQ(iter, roaring.End());

    iter = roaring.EqualOrLarger(5);
    ASSERT_EQ(*iter, 100000000000l);
    iter = roaring.EqualOrLarger(100000000000l);
    ASSERT_EQ(*iter, 100000000000l);
    iter = roaring.EqualOrLarger(150000000000l);
    ASSERT_EQ(*iter, 200000000000l);
    iter = roaring.EqualOrLarger(200000000000l);
    ASSERT_EQ(*iter, 200000000000l);
    iter = roaring.EqualOrLarger(200000000001l);
    ASSERT_EQ(iter, roaring.End());
}

TEST(RoaringBitmap64Test, TestIteratorAssignAndMove) {
    RoaringBitmap64 roaring =
        RoaringBitmap64::From({10000000000010l, 100000000000100l, 100000000000200l});

    auto iter1 = roaring.EqualOrLarger(10000000000010l);
    auto iter2 = iter1;
    ASSERT_EQ(iter1, iter2);
    ASSERT_EQ(10000000000010l, *iter1);
    ASSERT_EQ(10000000000010l, *iter2);

    auto iter3 = std::move(iter1);
    ASSERT_EQ(iter2, iter3);
    ASSERT_FALSE(iter1.iterator_);  // NOLINT(bugprone-use-after-move)

    ++iter3;
    ASSERT_NE(iter3, iter2);
    ASSERT_EQ(100000000000100l, *iter3);

    iter3 = iter2;
    ASSERT_EQ(iter3, iter2);

    iter2 = std::move(iter3);
    ASSERT_FALSE(iter3.iterator_);  // NOLINT(bugprone-use-after-move)
    ASSERT_EQ(10000000000010l, *iter2);

    iter3 = iter2;
    ASSERT_EQ(iter2, iter3);
    ASSERT_EQ(10000000000010l, *iter2);
    ASSERT_EQ(10000000000010l, *iter3);

    iter3 = std::move(iter2);
    ASSERT_EQ(10000000000010l, *iter3);
    ASSERT_FALSE(iter2.iterator_);  // NOLINT(bugprone-use-after-move)
}

TEST(RoaringBitmap64Test, TestDeserializeFromInputStream) {
    RoaringBitmap64 roaring1 =
        RoaringBitmap64::From({10000000000010l, 10000000000020l, 100000000000100l});
    RoaringBitmap64 roaring2 = RoaringBitmap64::From(
        {10000000000010l, 10000000000050l, 100000000000150l, 100000000000200l});
    RoaringBitmap64 roaring3 =
        RoaringBitmap64::From({1000000000002l, 1000000000004l, 1000000000006l, 1000000000001000l});

    size_t len1 = roaring1.GetSizeInBytes();
    size_t len2 = roaring2.GetSizeInBytes();
    size_t len3 = roaring3.GetSizeInBytes();

    auto pool = GetDefaultPool();
    auto bytes1 = roaring1.Serialize(pool.get());
    ASSERT_EQ(bytes1->size(), len1);
    auto bytes2 = roaring2.Serialize(pool.get());
    ASSERT_EQ(bytes2->size(), len2);
    auto bytes3 = roaring3.Serialize(pool.get());
    ASSERT_EQ(bytes3->size(), len3);

    auto concat_bytes = std::make_shared<Bytes>(len1 + len2 + len3, pool.get());
    memcpy(concat_bytes->data(), bytes1->data(), len1);
    memcpy(concat_bytes->data() + len1, bytes2->data(), len2);
    memcpy(concat_bytes->data() + len1 + len2, bytes3->data(), len3);

    ByteArrayInputStream byte_array_input_stream(concat_bytes->data(), concat_bytes->size());
    RoaringBitmap64 de_roaring1;
    ASSERT_OK(de_roaring1.Deserialize(&byte_array_input_stream));
    ASSERT_EQ(de_roaring1, roaring1);
    ASSERT_EQ(byte_array_input_stream.GetPos().value(), len1);

    RoaringBitmap64 de_roaring2;
    ASSERT_OK(de_roaring2.Deserialize(&byte_array_input_stream));
    ASSERT_EQ(de_roaring2, roaring2);
    ASSERT_EQ(byte_array_input_stream.GetPos().value(), len1 + len2);

    RoaringBitmap64 de_roaring3;
    ASSERT_OK(de_roaring3.Deserialize(&byte_array_input_stream));
    ASSERT_EQ(de_roaring3, roaring3);
    ASSERT_EQ(byte_array_input_stream.GetPos().value(), len1 + len2 + len3);
}

TEST(RoaringBitmap64Test, TestHighCardinality) {
    std::srand(time(nullptr));
    auto pool = GetDefaultPool();
    RoaringBitmap64 roaring;
    for (int64_t i = 0; i < 10000; i++) {
        roaring.Add(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + std::rand());
    }
    auto bytes = roaring.Serialize(pool.get());
    RoaringBitmap64 de_roaring;
    ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
    ASSERT_EQ(roaring, de_roaring);
}

TEST(RoaringBitmap64Test, TestInplaceAndOr) {
    RoaringBitmap64 roaring = RoaringBitmap64::From({0l, 1l, 100000000000100l});
    RoaringBitmap64 roaring1 = RoaringBitmap64::From({1l, 2l, 1000000000005l, 100000000000200l});
    roaring |= roaring1;
    ASSERT_EQ(roaring.ToString(), "{0,1,2,1000000000005,100000000000100,100000000000200}");
    RoaringBitmap64 roaring2 =
        RoaringBitmap64::From({1l, 2l, 3l, 1000000000005l, 100000000000100l, 100000000000500l});
    roaring &= roaring2;
    ASSERT_EQ(roaring.ToString(), "{1,2,1000000000005,100000000000100}");
    RoaringBitmap64 roaring3 = RoaringBitmap64::From({2l, 100000000000100l});
    roaring -= roaring3;
    ASSERT_EQ(roaring.ToString(), "{1,1000000000005}");
}

TEST(RoaringBitmap64Test, TestContainsAny) {
    RoaringBitmap64 roaring =
        RoaringBitmap64::From({10000000000010l, 10000000000011l, 10000000000100l});
    ASSERT_TRUE(roaring.ContainsAny(10000000000010l, 10000000000011l));
    ASSERT_TRUE(roaring.ContainsAny(10000000000010l, 10000000000020l));
    ASSERT_TRUE(roaring.ContainsAny(10000000000010l, 10000000000101l));
    ASSERT_TRUE(roaring.ContainsAny(10000000000020l, 10000000000200l));
    ASSERT_TRUE(roaring.ContainsAny(10000000000005l, 10000000000011l));
    ASSERT_FALSE(roaring.ContainsAny(10000000000005l, 10000000000010l));
    ASSERT_FALSE(roaring.ContainsAny(10000000000020l, 10000000000100l));
    ASSERT_FALSE(roaring.ContainsAny(10000000000020l, 10000000000030l));
    ASSERT_FALSE(roaring.ContainsAny(10000000000500l, 10000000000520l));
}

TEST(RoaringBitmap64Test, TestFromRoaringBitmap32) {
    {
        RoaringBitmap32 roaring32 = RoaringBitmap32::From({10, 20, 21});
        RoaringBitmap64 roaring64(roaring32);
        ASSERT_EQ(roaring64.ToString(), "{10,20,21}");
    }
    {
        RoaringBitmap32 roaring32 = RoaringBitmap32::From({10, 20, 21});
        RoaringBitmap64 roaring64;
        roaring64 = roaring32;
        ASSERT_EQ(roaring64.ToString(), "{10,20,21}");
    }
}

TEST(RoaringBitmap64Test, TestIteratorEqualOrLarger) {
    RoaringBitmap64 roaring = RoaringBitmap64::From({1l, 3l, 5l, 100l});
    auto iter = roaring.Begin();
    ASSERT_EQ(*iter, 1l);
    iter.EqualOrLarger(5l);
    ASSERT_EQ(*iter, 5l);
    iter.EqualOrLarger(10l);
    ASSERT_EQ(*iter, 100l);
    iter.EqualOrLarger(200l);
    ASSERT_EQ(iter, roaring.End());
}
}  // namespace paimon::test
