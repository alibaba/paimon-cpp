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

#include "paimon/common/memory/memory_segment.h"

#include <climits>
#include <cstdlib>
#include <limits>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(MemorySegmentTest, TestByteAccess) {
    auto pool = paimon::GetDefaultPool();
    int32_t page_size = 64 * 1024;
    MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    for (int32_t i = 0; i < page_size; i++) {
        segment.Put(i, static_cast<char>(std::rand()));
    }
    std::srand(seed);
    for (int32_t i = 0; i < page_size; i++) {
        ASSERT_EQ(segment.Get(i), static_cast<char>(std::rand()))
            << "seed: " << seed << ", idx: " << i;
    }

    // test expected correct behavior, random access

    std::srand(seed);
    bool* occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));
    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % page_size;
        if (occupied[pos]) {
            continue;
        } else {
            occupied[pos] = true;
        }
        segment.Put(pos, static_cast<char>(std::rand()));
    }
    delete[] occupied;

    std::srand(seed);
    occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));

    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % page_size;

        if (occupied[pos]) {
            continue;
        } else {
            occupied[pos] = true;
        }

        ASSERT_EQ(segment.Get(pos), static_cast<char>(std::rand()))
            << "seed: " << seed << ", idx: " << pos;
    }
    delete[] occupied;
}

TEST(MemorySegmentTest, TestBooleanAccess) {
    auto pool = paimon::GetDefaultPool();
    int32_t page_size = 64 * 1024;
    MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    bool* occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));
    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % page_size;
        if (occupied[pos]) {
            continue;
        } else {
            occupied[pos] = true;
        }
        segment.PutValue<bool>(pos, static_cast<bool>(std::rand() % 2));
    }
    delete[] occupied;

    std::srand(seed);
    occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));

    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % page_size;
        if (occupied[pos]) {
            continue;
        } else {
            occupied[pos] = true;
        }

        ASSERT_EQ(segment.GetValue<bool>(pos), static_cast<bool>(std::rand() % 2))
            << "seed: " << seed << ", idx: " << pos;
    }
    delete[] occupied;
}

TEST(MemorySegmentTest, TestEqualTo) {
    auto pool = paimon::GetDefaultPool();
    int32_t page_size = 64 * 1024;
    MemorySegment seg1 = MemorySegment::AllocateHeapMemory(page_size, pool.get());
    MemorySegment seg2 = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    Bytes reference_array(page_size, pool.get());
    seg1.Put(0, reference_array);
    seg2.Put(0, reference_array);

    int32_t i = paimon::test::RandomNumber(0, (page_size - 8) - 1);
    seg1.Put(i, static_cast<char>(10));
    ASSERT_FALSE(seg1.EqualTo(seg2, i, i, 9)) << "rand value:" << i;

    seg1.Put(i, static_cast<char>(0));
    ASSERT_TRUE(seg1.EqualTo(seg2, i, i, 9)) << "rand value:" << i;

    seg1.Put(i + 8, static_cast<char>(10));
    ASSERT_FALSE(seg1.EqualTo(seg2, i, i, 9)) << "rand value:" << i;
}

TEST(MemorySegmentTest, TestCompare) {
    auto pool = paimon::GetDefaultPool();
    int32_t page_size = 64 * 1024;
    MemorySegment seg1 = MemorySegment::AllocateHeapMemory(page_size, pool.get());
    MemorySegment seg2 = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    Bytes reference_array(page_size, pool.get());
    seg1.Put(0, reference_array);
    seg2.Put(0, reference_array);

    int32_t i = paimon::test::RandomNumber(0, (page_size - 8) - 1);
    seg1.Put(i, static_cast<char>(10));
    ASSERT_GT(seg1.Compare(seg2, i, i, 9, 9), 0);

    seg1.Put(i, static_cast<char>(0));
    ASSERT_EQ(seg1.Compare(seg2, i, i, 9, 9), 0);

    seg2.Put(i + 8, static_cast<char>(10));
    ASSERT_EQ(seg1.Compare(seg2, i, i, 7, 7), 0);
    ASSERT_LT(seg1.Compare(seg2, i, i, 9, 9), 0);
}

TEST(MemorySegmentTest, TestSwapBytes) {
    auto pool = GetDefaultPool();
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

    MemorySegment seg1 = MemorySegment::Wrap(bytes1);
    MemorySegment seg2 = MemorySegment::Wrap(bytes2);

    std::shared_ptr<Bytes> bytes1_serialize = Bytes::AllocateBytes(test_string2.size(), pool.get());
    std::shared_ptr<Bytes> bytes2_serialize = Bytes::AllocateBytes(test_string1.size(), pool.get());
    seg1.SwapBytes(bytes1_serialize.get(), &seg2, 0, 0, str_size);
    seg1.CopyToUnsafe(0, bytes1_serialize->data(), 0, str_size);
    seg2.CopyToUnsafe(0, bytes2_serialize->data(), 0, str_size);
    ASSERT_EQ(test_string1, std::string(bytes2_serialize->data(), bytes2_serialize->size()));
    ASSERT_EQ(test_string2, std::string(bytes1_serialize->data(), bytes1_serialize->size()));
}

TEST(MemorySegmentTest, TestCharAccess) {
    auto pool = paimon::GetDefaultPool();
    int32_t page_size = 64 * 1024;
    MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);

    for (int32_t i = 0; i <= page_size - 2; i += 2) {
        segment.PutValue<char16_t>(i, static_cast<char>(std::rand() % (CHAR_MAX)));
    }

    std::srand(seed);
    for (int32_t i = 0; i <= page_size - 2; i += 2) {
        ASSERT_EQ(segment.GetValue<char16_t>(i), static_cast<char>(std::rand() % (CHAR_MAX)))
            << "seed: " << seed << ", idx: " << i;
    }

    // test expected correct behavior, random access

    std::srand(seed);
    bool* occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));
    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 1);
        if (occupied[pos] || occupied[pos + 1]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
        }
        segment.PutValue<char16_t>(pos, static_cast<char>(std::rand() % (CHAR_MAX)));
    }
    delete[] occupied;

    std::srand(seed);
    occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));

    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 1);
        if (occupied[pos] || occupied[pos + 1]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
        }

        ASSERT_EQ(segment.GetValue<char16_t>(pos), static_cast<char>(std::rand() % (CHAR_MAX)))
            << "seed: " << seed << ", idx:" << pos;
    }
    delete[] occupied;
}

TEST(MemorySegmentTest, TestShortAccess) {
    auto pool = paimon::GetDefaultPool();
    int32_t page_size = 64 * 1024;
    MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);

    for (int32_t i = 0; i <= page_size - 2; i += 2) {
        segment.PutValue<int16_t>(i, static_cast<int16_t>(std::rand()));
    }

    std::srand(seed);
    for (int32_t i = 0; i <= page_size - 2; i += 2) {
        ASSERT_EQ(segment.GetValue<int16_t>(i), static_cast<int16_t>(std::rand()))
            << "seed: " << seed << ", idx:" << i;
    }

    // test expected correct behavior, random access

    std::srand(seed);
    bool* occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));
    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 1);
        if (occupied[pos] || occupied[pos + 1]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
        }
        segment.PutValue<int16_t>(pos, static_cast<int16_t>(std::rand()));
    }
    delete[] occupied;

    std::srand(seed);
    occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));

    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 1);
        if (occupied[pos] || occupied[pos + 1]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
        }

        ASSERT_EQ(segment.GetValue<int16_t>(pos), static_cast<int16_t>(std::rand()))
            << "seed: " << seed << ", idx:" << pos;
    }
    delete[] occupied;
}

TEST(MemorySegmentTest, TestIntAccess) {
    auto pool = paimon::GetDefaultPool();
    int32_t page_size = 64 * 1024;
    MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);

    for (int32_t i = 0; i <= page_size - 4; i += 4) {
        segment.PutValue<int32_t>(i, static_cast<int32_t>(std::rand()));
    }

    std::srand(seed);
    for (int32_t i = 0; i <= page_size - 4; i += 4) {
        ASSERT_EQ(segment.GetValue<int32_t>(i), static_cast<int32_t>(std::rand()))
            << "seed: " << seed << ", idx:" << i;
    }

    // test expected correct behavior, random access

    std::srand(seed);
    bool* occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));
    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 3);
        if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
            occupied[pos + 2] = true;
            occupied[pos + 3] = true;
        }
        segment.PutValue<int32_t>(pos, static_cast<int32_t>(std::rand()));
    }
    delete[] occupied;

    std::srand(seed);
    occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));

    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 3);
        if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
            occupied[pos + 2] = true;
            occupied[pos + 3] = true;
        }

        ASSERT_EQ(segment.GetValue<int32_t>(pos), static_cast<int32_t>(std::rand()))
            << "seed: " << seed << ", idx:" << pos;
    }
    delete[] occupied;
}

TEST(MemorySegmentTest, TestLongAccess) {
    auto pool = paimon::GetDefaultPool();
    auto lrand = []() -> int64_t {
        return (static_cast<int64_t>(std::rand()) << (sizeof(int32_t) * 8)) | std::rand();
    };
    int32_t page_size = 64 * 1024;
    MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);

    for (int32_t i = 0; i <= page_size - 8; i += 8) {
        segment.PutValue<int64_t>(i, lrand());
    }

    std::srand(seed);
    for (int32_t i = 0; i <= page_size - 8; i += 8) {
        ASSERT_EQ(segment.GetValue<int64_t>(i), lrand()) << "seed: " << seed << ", idx:" << i;
    }

    // test expected correct behavior, random access

    std::srand(seed);
    bool* occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));
    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 7);
        if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3] ||
            occupied[pos + 4] || occupied[pos + 5] || occupied[pos + 6] || occupied[pos + 7]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
            occupied[pos + 2] = true;
            occupied[pos + 3] = true;
            occupied[pos + 4] = true;
            occupied[pos + 5] = true;
            occupied[pos + 6] = true;
            occupied[pos + 7] = true;
        }
        segment.PutValue<int64_t>(pos, lrand());
    }
    delete[] occupied;

    std::srand(seed);
    occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));

    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 7);
        if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3] ||
            occupied[pos + 4] || occupied[pos + 5] || occupied[pos + 6] || occupied[pos + 7]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
            occupied[pos + 2] = true;
            occupied[pos + 3] = true;
            occupied[pos + 4] = true;
            occupied[pos + 5] = true;
            occupied[pos + 6] = true;
            occupied[pos + 7] = true;
        }

        ASSERT_EQ(segment.GetValue<int64_t>(pos), lrand()) << "seed: " << seed << ", idx:" << pos;
    }
    delete[] occupied;
}

TEST(MemorySegmentTest, TestFloatAccess) {
    auto pool = paimon::GetDefaultPool();
    auto frand = []() -> int64_t {
        return (static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX));
    };

    int32_t page_size = 64 * 1024;
    MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);

    for (int32_t i = 0; i <= page_size - 4; i += 4) {
        segment.PutValue<float>(i, frand());
    }

    std::srand(seed);
    for (int32_t i = 0; i <= page_size - 4; i += 4) {
        ASSERT_EQ(segment.GetValue<float>(i), frand()) << "seed: " << seed << ", idx:" << i;
    }

    // test expected correct behavior, random access

    std::srand(seed);
    bool* occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));
    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 3);
        if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
            occupied[pos + 2] = true;
            occupied[pos + 3] = true;
        }
        segment.PutValue<float>(pos, frand());
    }
    delete[] occupied;

    std::srand(seed);
    occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));

    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 3);
        if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
            occupied[pos + 2] = true;
            occupied[pos + 3] = true;
        }

        ASSERT_EQ(segment.GetValue<float>(pos), frand()) << "seed: " << seed << ", idx:" << pos;
    }
    delete[] occupied;
}

TEST(MemorySegmentTest, TestDoubleAccess) {
    auto pool = paimon::GetDefaultPool();
    auto lrand = []() -> int64_t {
        return (static_cast<int64_t>(std::rand()) << (sizeof(int32_t) * 8)) | std::rand();
    };
    auto drand = [&]() -> int64_t {
        return (static_cast<double>(lrand()) /
                static_cast<double>(std::numeric_limits<int64_t>::max()));
    };

    int32_t page_size = 64 * 1024;
    MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);

    for (int32_t i = 0; i <= page_size - 8; i += 8) {
        segment.PutValue<double>(i, drand());
    }

    std::srand(seed);
    for (int32_t i = 0; i <= page_size - 8; i += 8) {
        ASSERT_EQ(segment.GetValue<double>(i), drand()) << "seed: " << seed << ", idx:" << i;
    }

    // test expected correct behavior, random access

    std::srand(seed);
    bool* occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));
    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 7);
        if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3] ||
            occupied[pos + 4] || occupied[pos + 5] || occupied[pos + 6] || occupied[pos + 7]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
            occupied[pos + 2] = true;
            occupied[pos + 3] = true;
            occupied[pos + 4] = true;
            occupied[pos + 5] = true;
            occupied[pos + 6] = true;
            occupied[pos + 7] = true;
        }
        segment.PutValue<double>(pos, drand());
    }
    delete[] occupied;

    std::srand(seed);
    occupied = new bool[page_size];
    std::memset(occupied, 0, page_size * sizeof(bool));

    for (int32_t i = 0; i < 1000; i++) {
        int32_t pos = std::rand() % (page_size - 7);
        if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3] ||
            occupied[pos + 4] || occupied[pos + 5] || occupied[pos + 6] || occupied[pos + 7]) {
            continue;
        } else {
            occupied[pos] = true;
            occupied[pos + 1] = true;
            occupied[pos + 2] = true;
            occupied[pos + 3] = true;
            occupied[pos + 4] = true;
            occupied[pos + 5] = true;
            occupied[pos + 6] = true;
            occupied[pos + 7] = true;
        }

        ASSERT_EQ(segment.GetValue<double>(pos), drand()) << "seed: " << seed << ", idx:" << pos;
    }
    delete[] occupied;
}

// ------------------------------------------------------------------------
//  Bulk Byte Movements
// ------------------------------------------------------------------------

TEST(MemorySegmentTest, TestBulkByteAccess) {
    auto pool = paimon::GetDefaultPool();
    // test expected correct behavior with default offset / length
    auto rand_bytes = [&](int32_t size) -> std::shared_ptr<Bytes> {
        auto bytes = Bytes::AllocateBytes(size, pool.get());
        for (int32_t i = 0; i < static_cast<int32_t>(bytes->size()); i++) {
            (*bytes)[i] = static_cast<char>(std::rand());
        }
        return bytes;
    };
    {
        int32_t page_size = 64 * 1024;
        MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
        for (int32_t i = 0; i < 8; i++) {
            auto src = rand_bytes(page_size / 8);
            segment.Put(i * (page_size / 8), *src);
        }

        std::srand(seed);

        for (int32_t i = 0; i < 8; i++) {
            std::shared_ptr<Bytes> expected = rand_bytes(page_size / 8);
            std::shared_ptr<Bytes> actual = Bytes::AllocateBytes(page_size / 8, pool.get());
            segment.Get(i * (page_size / 8), actual.get());
            ASSERT_EQ(expected->size(), actual->size()) << "seed: " << seed << ", i:" << i;
            ASSERT_EQ(std::memcmp(expected->data(), actual->data(), expected->size()), 0)
                << "seed: " << seed << ", i:" << i;
        }
    }

    // test expected correct behavior with specific offset / length
    {
        int32_t page_size = 64 * 1024;
        MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());

        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
        std::shared_ptr<Bytes> expected = rand_bytes(page_size);

        for (int32_t i = 0; i < 16; i++) {
            segment.Put(i * (page_size / 16), *expected, i * (page_size / 16), page_size / 16);
        }
        auto actual = Bytes::AllocateBytes(page_size, pool.get());
        for (int32_t i = 0; i < 16; i++) {
            segment.Get(i * (page_size / 16), actual.get(), i * (page_size / 16), page_size / 16);
        }
        ASSERT_EQ(expected->size(), actual->size()) << "seed: " << seed;
        ASSERT_EQ(std::memcmp(expected->data(), actual->data(), expected->size()), 0)
            << "seed: " << seed;
    }
    // put segments of various lengths to various positions
    {
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
        int32_t page_size = 64 * 1024;
        MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());
        auto expected = Bytes::AllocateBytes(page_size, pool.get());
        segment.Put(0, *expected, 0, page_size);
        for (int32_t i = 0; i < 200; i++) {
            int32_t num_bytes = std::rand() % (page_size - 10) + 1;
            int32_t pos = std::rand() % (page_size - num_bytes + 1);
            std::shared_ptr<Bytes> data = rand_bytes((std::rand() % 3 + 1) * num_bytes);
            int32_t data_start_pos = std::rand() % (data->size() - num_bytes + 1);

            // copy to the expected
            std::memcpy(expected->data() + pos, data->data() + data_start_pos, num_bytes);

            // put to the memory segment
            segment.Put(pos, *data, data_start_pos, num_bytes);
        }

        auto validation = Bytes::AllocateBytes(page_size, pool.get());
        segment.Get(0, validation.get());
        ASSERT_EQ(std::memcmp(validation->data(), expected->data(), expected->size()), 0)
            << "seed: " << seed;
    }
    // get segments with various contents
    {
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
        int32_t page_size = 64 * 1024;
        MemorySegment segment = MemorySegment::AllocateHeapMemory(page_size, pool.get());
        std::shared_ptr<Bytes> contents = rand_bytes(page_size);
        segment.Put(0, *contents);

        for (int32_t i = 0; i < 200; i++) {
            int32_t num_bytes = std::rand() % (page_size / 8) + 1;
            int32_t pos = std::rand() % (page_size - num_bytes + 1);
            std::shared_ptr<Bytes> data = rand_bytes((std::rand() % 3 + 1) * num_bytes);
            int32_t data_start_pos = std::rand() % (data->size() - num_bytes + 1);

            segment.Get(pos, data.get(), data_start_pos, num_bytes);
            Bytes expected(num_bytes, pool.get());
            std::memcpy(expected.data(), contents->data() + pos, num_bytes);
            Bytes validation(num_bytes, pool.get());
            std::memcpy(validation.data(), data->data() + data_start_pos, num_bytes);
            ASSERT_EQ(std::memcmp(validation.data(), expected.data(), expected.size()), 0)
                << "seed: " << seed << ", i:" << i;
        }
    }
}

}  // namespace paimon::test
