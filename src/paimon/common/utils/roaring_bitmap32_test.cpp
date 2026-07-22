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

#include "paimon/utils/roaring_bitmap32.h"

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
TEST(RoaringBitmap32Test, TestSimple) {
    RoaringBitmap32 roaring;
    ASSERT_TRUE(roaring.IsEmpty());
    roaring.Add(10);
    roaring.Add(100);
    ASSERT_TRUE(roaring.Contains(10));
    ASSERT_TRUE(roaring.Contains(100));
    ASSERT_FALSE(roaring.CheckedAdd(10));
    ASSERT_TRUE(roaring.CheckedAdd(20));
    ASSERT_TRUE(roaring.Contains(20));
    ASSERT_FALSE(roaring.IsEmpty());
    ASSERT_EQ("{10,20,100}", roaring.ToString());
    ASSERT_EQ(3, roaring.Cardinality());
    roaring.AddRange(10, 15);
    ASSERT_EQ("{10,11,12,13,14,20,100}", roaring.ToString());
    ASSERT_EQ(7, roaring.Cardinality());
    roaring.RemoveRange(12, 20);
    ASSERT_EQ("{10,11,20,100}", roaring.ToString());
    ASSERT_EQ(4, roaring.Cardinality());
}
TEST(RoaringBitmap32Test, TestCompatibleWithJava) {
    auto pool = GetDefaultPool();
    {
        RoaringBitmap32 roaring;
        for (int32_t i = 0; i < 100; i++) {
            roaring.Add(i);
        }
        for (int32_t i = 0; i < 100; i++) {
            ASSERT_TRUE(roaring.Contains(i));
        }
        auto bytes = roaring.Serialize(pool.get());
        std::vector<uint8_t> result(bytes->data(), bytes->data() + bytes->size());
        std::vector<uint8_t> expected = {59, 48, 0, 0, 1, 0, 0, 99, 0, 1, 0, 0, 0, 99, 0};
        ASSERT_EQ(result, expected);

        RoaringBitmap32 de_roaring;
        ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
        for (int32_t i = 0; i < 100; i++) {
            ASSERT_TRUE(de_roaring.Contains(i));
        }
    }
    {
        RoaringBitmap32 roaring;
        for (int32_t i = RoaringBitmap32::MAX_VALUE - 1; i > RoaringBitmap32::MAX_VALUE - 101;
             i--) {
            roaring.Add(i);
        }
        for (int32_t i = RoaringBitmap32::MAX_VALUE - 1; i > RoaringBitmap32::MAX_VALUE - 101;
             i--) {
            ASSERT_TRUE(roaring.Contains(i));
        }
        auto bytes = roaring.Serialize(pool.get());
        std::vector<uint8_t> result(bytes->data(), bytes->data() + bytes->size());
        std::vector<uint8_t> expected = {59, 48, 0, 0, 1, 255, 127, 99, 0, 1, 0, 155, 255, 99, 0};
        ASSERT_EQ(result, expected);

        RoaringBitmap32 de_roaring;
        ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
        for (int32_t i = RoaringBitmap32::MAX_VALUE - 1; i > RoaringBitmap32::MAX_VALUE - 101;
             i--) {
            ASSERT_TRUE(de_roaring.Contains(i));
        }
    }
    {
        RoaringBitmap32 roaring;
        for (int32_t i = 5000; i < 10000; i += 17) {
            roaring.Add(i);
        }
        for (int32_t i = 5000; i < 10000; i += 17) {
            ASSERT_TRUE(roaring.Contains(i));
        }
        auto bytes = roaring.Serialize(pool.get());
        std::vector<uint8_t> result(bytes->data(), bytes->data() + bytes->size());
        std::vector<uint8_t> expected = {
            58,  48, 0,   0,  1,   0,  0,   0,  0,   0,  38,  1,  16,  0,  0,   0,  136, 19,
            153, 19, 170, 19, 187, 19, 204, 19, 221, 19, 238, 19, 255, 19, 16,  20, 33,  20,
            50,  20, 67,  20, 84,  20, 101, 20, 118, 20, 135, 20, 152, 20, 169, 20, 186, 20,
            203, 20, 220, 20, 237, 20, 254, 20, 15,  21, 32,  21, 49,  21, 66,  21, 83,  21,
            100, 21, 117, 21, 134, 21, 151, 21, 168, 21, 185, 21, 202, 21, 219, 21, 236, 21,
            253, 21, 14,  22, 31,  22, 48,  22, 65,  22, 82,  22, 99,  22, 116, 22, 133, 22,
            150, 22, 167, 22, 184, 22, 201, 22, 218, 22, 235, 22, 252, 22, 13,  23, 30,  23,
            47,  23, 64,  23, 81,  23, 98,  23, 115, 23, 132, 23, 149, 23, 166, 23, 183, 23,
            200, 23, 217, 23, 234, 23, 251, 23, 12,  24, 29,  24, 46,  24, 63,  24, 80,  24,
            97,  24, 114, 24, 131, 24, 148, 24, 165, 24, 182, 24, 199, 24, 216, 24, 233, 24,
            250, 24, 11,  25, 28,  25, 45,  25, 62,  25, 79,  25, 96,  25, 113, 25, 130, 25,
            147, 25, 164, 25, 181, 25, 198, 25, 215, 25, 232, 25, 249, 25, 10,  26, 27,  26,
            44,  26, 61,  26, 78,  26, 95,  26, 112, 26, 129, 26, 146, 26, 163, 26, 180, 26,
            197, 26, 214, 26, 231, 26, 248, 26, 9,   27, 26,  27, 43,  27, 60,  27, 77,  27,
            94,  27, 111, 27, 128, 27, 145, 27, 162, 27, 179, 27, 196, 27, 213, 27, 230, 27,
            247, 27, 8,   28, 25,  28, 42,  28, 59,  28, 76,  28, 93,  28, 110, 28, 127, 28,
            144, 28, 161, 28, 178, 28, 195, 28, 212, 28, 229, 28, 246, 28, 7,   29, 24,  29,
            41,  29, 58,  29, 75,  29, 92,  29, 109, 29, 126, 29, 143, 29, 160, 29, 177, 29,
            194, 29, 211, 29, 228, 29, 245, 29, 6,   30, 23,  30, 40,  30, 57,  30, 74,  30,
            91,  30, 108, 30, 125, 30, 142, 30, 159, 30, 176, 30, 193, 30, 210, 30, 227, 30,
            244, 30, 5,   31, 22,  31, 39,  31, 56,  31, 73,  31, 90,  31, 107, 31, 124, 31,
            141, 31, 158, 31, 175, 31, 192, 31, 209, 31, 226, 31, 243, 31, 4,   32, 21,  32,
            38,  32, 55,  32, 72,  32, 89,  32, 106, 32, 123, 32, 140, 32, 157, 32, 174, 32,
            191, 32, 208, 32, 225, 32, 242, 32, 3,   33, 20,  33, 37,  33, 54,  33, 71,  33,
            88,  33, 105, 33, 122, 33, 139, 33, 156, 33, 173, 33, 190, 33, 207, 33, 224, 33,
            241, 33, 2,   34, 19,  34, 36,  34, 53,  34, 70,  34, 87,  34, 104, 34, 121, 34,
            138, 34, 155, 34, 172, 34, 189, 34, 206, 34, 223, 34, 240, 34, 1,   35, 18,  35,
            35,  35, 52,  35, 69,  35, 86,  35, 103, 35, 120, 35, 137, 35, 154, 35, 171, 35,
            188, 35, 205, 35, 222, 35, 239, 35, 0,   36, 17,  36, 34,  36, 51,  36, 68,  36,
            85,  36, 102, 36, 119, 36, 136, 36, 153, 36, 170, 36, 187, 36, 204, 36, 221, 36,
            238, 36, 255, 36, 16,  37, 33,  37, 50,  37, 67,  37, 84,  37, 101, 37, 118, 37,
            135, 37, 152, 37, 169, 37, 186, 37, 203, 37, 220, 37, 237, 37, 254, 37, 15,  38,
            32,  38, 49,  38, 66,  38, 83,  38, 100, 38, 117, 38, 134, 38, 151, 38, 168, 38,
            185, 38, 202, 38, 219, 38, 236, 38, 253, 38, 14,  39};
        ASSERT_EQ(result, expected);

        RoaringBitmap32 de_roaring;
        ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
        for (int32_t i = 5000; i < 10000; i += 17) {
            ASSERT_TRUE(de_roaring.Contains(i));
        }
    }
    // empty
    {
        RoaringBitmap32 roaring;
        auto bytes = roaring.Serialize(pool.get());
        std::vector<uint8_t> result(bytes->data(), bytes->data() + bytes->size());
        std::vector<uint8_t> expected = {58, 48, 0, 0, 0, 0, 0, 0};
        ASSERT_EQ(result, expected);
        RoaringBitmap32 de_roaring;
        ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
        ASSERT_TRUE(de_roaring.IsEmpty());
        ASSERT_FALSE(de_roaring.Contains(58));
    }
}

TEST(RoaringBitmap32Test, TestDeserializeFailed) {
    RoaringBitmap32 roaring;
    std::vector<char> invalid_bytes = {0, 100};
    ASSERT_NOK_WITH_MSG(roaring.Deserialize(invalid_bytes.data(), invalid_bytes.size()),
                        "catch exception in Deserialize() of RoaringBitmap32");
}

TEST(RoaringBitmap32Test, TestAndOr) {
    RoaringBitmap32 roaring1 = RoaringBitmap32::From({10, 100});
    RoaringBitmap32 roaring2 = RoaringBitmap32::From({20, 100, 200});

    auto and_roaring = RoaringBitmap32::And(roaring1, roaring2);
    ASSERT_EQ(and_roaring, RoaringBitmap32::From({100}));

    auto or_roaring = RoaringBitmap32::Or(roaring1, roaring2);
    ASSERT_EQ(or_roaring, RoaringBitmap32::From({10, 20, 100, 200}));
}

TEST(RoaringBitmap32Test, TestAssignAndMove) {
    RoaringBitmap32 roaring1 = RoaringBitmap32::From({10, 100});
    RoaringBitmap32 roaring2 = roaring1;
    ASSERT_EQ(roaring1, roaring2);
    ASSERT_FALSE(roaring1.IsEmpty());
    ASSERT_FALSE(roaring2.IsEmpty());

    RoaringBitmap32 roaring3 = std::move(roaring1);
    ASSERT_EQ(roaring3, roaring2);
    ASSERT_FALSE(roaring1.roaring_bitmap_);  // NOLINT(bugprone-use-after-move)

    roaring3.Add(20);
    ASSERT_FALSE(roaring3 == roaring2);

    roaring3 = roaring2;
    ASSERT_EQ(roaring3, roaring2);

    roaring2 = std::move(roaring3);
    ASSERT_FALSE(roaring3.roaring_bitmap_);  // NOLINT(bugprone-use-after-move)
    ASSERT_EQ("{10,100}", roaring2.ToString());

    roaring3 = roaring2;
    ASSERT_EQ("{10,100}", roaring3.ToString());

    roaring3 = std::move(roaring2);
    ASSERT_EQ("{10,100}", roaring3.ToString());
    ASSERT_FALSE(roaring2.roaring_bitmap_);  // NOLINT(bugprone-use-after-move)
}

TEST(RoaringBitmap32Test, TestFastUnion) {
    RoaringBitmap32 roaring1 = RoaringBitmap32::From({10, 100});
    RoaringBitmap32 roaring2 = RoaringBitmap32::From({1, 10, 20, 100, 300});
    RoaringBitmap32 roaring3 = RoaringBitmap32::From({2, 100, 800});

    RoaringBitmap32 res = RoaringBitmap32::FastUnion({&roaring1, &roaring2, &roaring3});
    ASSERT_EQ(res, RoaringBitmap32::From({1, 2, 10, 20, 100, 300, 800}));

    std::vector<RoaringBitmap32> roarings = {roaring1, roaring2, roaring3};
    RoaringBitmap32 res2 = RoaringBitmap32::FastUnion(roarings);
    ASSERT_EQ(res2, RoaringBitmap32::From({1, 2, 10, 20, 100, 300, 800}));
}

TEST(RoaringBitmap32Test, TestFlip) {
    RoaringBitmap32 roaring = RoaringBitmap32::From({1, 2, 4});
    roaring.Flip(0, 5);
    ASSERT_EQ(roaring, RoaringBitmap32::From({0, 3}));
}

TEST(RoaringBitmap32Test, TestAndNot) {
    RoaringBitmap32 roaring1 = RoaringBitmap32::From({10, 20, 100, 200});
    RoaringBitmap32 roaring2 = RoaringBitmap32::From({5, 20, 80, 200, 250});

    auto andnot_roaring = RoaringBitmap32::AndNot(roaring1, roaring2);
    ASSERT_EQ(andnot_roaring, RoaringBitmap32::From({10, 100}));
}

TEST(RoaringBitmap32Test, TestIterator) {
    RoaringBitmap32 roaring = RoaringBitmap32::From({10, 20});
    auto iter = roaring.Begin();
    ASSERT_EQ(*iter, 10);
    auto iter2 = ++iter;
    ASSERT_EQ(*iter, 20);
    ASSERT_EQ(*iter2, 20);
    ++iter;
    ASSERT_EQ(iter, roaring.End());

    iter = roaring.EqualOrLarger(5);
    ASSERT_EQ(*iter, 10);
    iter = roaring.EqualOrLarger(10);
    ASSERT_EQ(*iter, 10);
    iter = roaring.EqualOrLarger(15);
    ASSERT_EQ(*iter, 20);
    iter = roaring.EqualOrLarger(20);
    ASSERT_EQ(*iter, 20);
    iter = roaring.EqualOrLarger(100);
    ASSERT_EQ(iter, roaring.End());
}

TEST(RoaringBitmap32Test, TestNextAndPreviousValue) {
    RoaringBitmap32 roaring = RoaringBitmap32::From({10, 20, 30});

    ASSERT_EQ(roaring.NextValue(5), std::optional<int32_t>(10));
    ASSERT_EQ(roaring.NextValue(10), std::optional<int32_t>(10));
    ASSERT_EQ(roaring.NextValue(25), std::optional<int32_t>(30));
    ASSERT_EQ(roaring.NextValue(31), std::nullopt);

    ASSERT_EQ(roaring.PreviousValue(10), std::nullopt);
    ASSERT_EQ(roaring.PreviousValue(11), std::optional<int32_t>(10));
    ASSERT_EQ(roaring.PreviousValue(20), std::optional<int32_t>(10));
    ASSERT_EQ(roaring.PreviousValue(21), std::optional<int32_t>(20));
    ASSERT_EQ(roaring.PreviousValue(100), std::optional<int32_t>(30));

    RoaringBitmap32 empty;
    ASSERT_EQ(empty.NextValue(0), std::nullopt);
    ASSERT_EQ(empty.PreviousValue(0), std::nullopt);
}

TEST(RoaringBitmap32Test, TestIteratorAssignAndMove) {
    RoaringBitmap32 roaring = RoaringBitmap32::From({10, 100, 200});

    auto iter1 = roaring.EqualOrLarger(10);
    auto iter2 = iter1;
    ASSERT_EQ(iter1, iter2);
    ASSERT_EQ(10, *iter1);
    ASSERT_EQ(10, *iter2);

    auto iter3 = std::move(iter1);
    ASSERT_EQ(iter2, iter3);
    ASSERT_FALSE(iter1.iterator_);  // NOLINT(bugprone-use-after-move)

    ++iter3;
    ASSERT_NE(iter3, iter2);
    ASSERT_EQ(100, *iter3);

    iter3 = iter2;
    ASSERT_EQ(iter3, iter2);

    iter2 = std::move(iter3);
    ASSERT_FALSE(iter3.iterator_);  // NOLINT(bugprone-use-after-move)
    ASSERT_EQ(10, *iter2);

    iter3 = iter2;
    ASSERT_EQ(iter2, iter3);
    ASSERT_EQ(10, *iter2);
    ASSERT_EQ(10, *iter3);

    iter3 = std::move(iter2);
    ASSERT_EQ(10, *iter3);
    ASSERT_FALSE(iter2.iterator_);  // NOLINT(bugprone-use-after-move)
}

TEST(RoaringBitmap32Test, TestDeserializeFromInputStream) {
    RoaringBitmap32 roaring1 = RoaringBitmap32::From({10, 20, 100});
    RoaringBitmap32 roaring2 = RoaringBitmap32::From({10, 50, 150, 200});
    RoaringBitmap32 roaring3 = RoaringBitmap32::From({2, 4, 6, 1000});

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
    RoaringBitmap32 de_roaring1;
    ASSERT_OK(de_roaring1.Deserialize(&byte_array_input_stream));
    ASSERT_EQ(de_roaring1, roaring1);
    ASSERT_EQ(byte_array_input_stream.GetPos().value(), len1);

    RoaringBitmap32 de_roaring2;
    ASSERT_OK(de_roaring2.Deserialize(&byte_array_input_stream));
    ASSERT_EQ(de_roaring2, roaring2);
    ASSERT_EQ(byte_array_input_stream.GetPos().value(), len1 + len2);

    RoaringBitmap32 de_roaring3;
    ASSERT_OK(de_roaring3.Deserialize(&byte_array_input_stream));
    ASSERT_EQ(de_roaring3, roaring3);
    ASSERT_EQ(byte_array_input_stream.GetPos().value(), len1 + len2 + len3);
}

TEST(RoaringBitmap32Test, TestHighCardinality) {
    std::srand(time(nullptr));
    auto pool = GetDefaultPool();
    RoaringBitmap32 roaring;
    for (int32_t i = 0; i < 10000; i++) {
        roaring.Add(std::rand());
    }
    auto bytes = roaring.Serialize(pool.get());
    RoaringBitmap32 de_roaring;
    ASSERT_OK(de_roaring.Deserialize(bytes->data(), bytes->size()));
    ASSERT_EQ(roaring, de_roaring);
}

TEST(RoaringBitmap32Test, TestInplaceAndOr) {
    RoaringBitmap32 roaring = RoaringBitmap32::From({0, 1, 100});
    RoaringBitmap32 roaring1 = RoaringBitmap32::From({1, 2, 5, 200});
    roaring |= roaring1;
    ASSERT_EQ(roaring.ToString(), "{0,1,2,5,100,200}");
    RoaringBitmap32 roaring2 = RoaringBitmap32::From({1, 2, 3, 5, 100, 500});
    roaring &= roaring2;
    ASSERT_EQ(roaring.ToString(), "{1,2,5,100}");
    RoaringBitmap32 roaring3 = RoaringBitmap32::From({2, 100});
    roaring -= roaring3;
    ASSERT_EQ(roaring.ToString(), "{1,5}");
}

TEST(RoaringBitmap32Test, TestContainsAny) {
    RoaringBitmap32 roaring = RoaringBitmap32::From({10, 11, 100});
    ASSERT_TRUE(roaring.ContainsAny(10, 11));
    ASSERT_TRUE(roaring.ContainsAny(10, 20));
    ASSERT_TRUE(roaring.ContainsAny(10, 101));
    ASSERT_TRUE(roaring.ContainsAny(20, 200));
    ASSERT_TRUE(roaring.ContainsAny(5, 11));
    ASSERT_FALSE(roaring.ContainsAny(5, 10));
    ASSERT_FALSE(roaring.ContainsAny(20, 100));
    ASSERT_FALSE(roaring.ContainsAny(20, 30));
    ASSERT_FALSE(roaring.ContainsAny(500, 520));
}

}  // namespace paimon::test
