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

#include "paimon/common/memory/memory_segment_utils.h"

#include <cstdlib>
#include <string>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(MemorySegmentUtilsTest, TestSetAndGetValue) {
    auto pool = GetDefaultPool();
    for (auto single_segment_size : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 128}) {
        std::vector<MemorySegment> segments;
        segments.reserve(50);
        for (size_t i = 0; i < 50; i++) {
            segments.push_back(
                MemorySegment::Wrap(Bytes::AllocateBytes(single_segment_size, pool.get())));
        }
        int32_t offset = 0;
        MemorySegmentUtils::SetValue<int32_t>(&segments, offset, 233);
        ASSERT_EQ(233, MemorySegmentUtils::GetValue<int32_t>(segments, offset));

        offset += sizeof(int32_t);
        MemorySegmentUtils::SetValue<int64_t>(&segments, offset, 23333333333);
        ASSERT_EQ(23333333333, MemorySegmentUtils::GetValue<int64_t>(segments, offset));

        offset += sizeof(int64_t);
        MemorySegmentUtils::SetValue<float>(&segments, offset, 233.3);
        ASSERT_NEAR(233.3, MemorySegmentUtils::GetValue<float>(segments, offset), 0.001);

        offset += sizeof(float);
        MemorySegmentUtils::SetValue<double>(&segments, offset, 244.3);
        ASSERT_NEAR(244.3, MemorySegmentUtils::GetValue<double>(segments, offset), 0.001);

        offset += sizeof(double);
        MemorySegmentUtils::SetValue<int16_t>(&segments, offset, 5564);
        ASSERT_EQ(5564, MemorySegmentUtils::GetValue<int16_t>(segments, offset));

        offset += sizeof(int16_t);
        MemorySegmentUtils::SetValue<char>(&segments, offset, 123);
        ASSERT_EQ(123, MemorySegmentUtils::GetValue<char>(segments, offset));

        offset += sizeof(char);
        MemorySegmentUtils::SetValue<bool>(&segments, offset, true);
        ASSERT_EQ(true, MemorySegmentUtils::GetValue<bool>(segments, offset));

        ASSERT_EQ(233, MemorySegmentUtils::GetValue<int32_t>(segments, 0));
    }
}

TEST(MemorySegmentUtilsTest, TestCopyFromBytesAndGetBytes) {
    auto pool = GetDefaultPool();
    int32_t str_size = 1024;
    std::string test_string1, test_string2;
    test_string1.reserve(str_size);
    test_string2.reserve(str_size);
    for (int32_t j = 0; j < str_size; j++) {
        test_string1 += paimon::test::RandomNumber(0, 25) + 'a';
        test_string2 += paimon::test::RandomNumber(0, 25) + 'a';
    }
    test_string2 += test_string1;
    std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes(test_string1, pool.get());
    std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes(test_string2, pool.get());

    std::vector<MemorySegment> segs = {MemorySegment::AllocateHeapMemory(str_size, pool.get())};
    MemorySegmentUtils::CopyFromBytes(&segs, 0, *bytes1, 0, test_string1.size());
    PAIMON_UNIQUE_PTR<Bytes> bytes_serialize =
        MemorySegmentUtils::CopyToBytes(segs, 0, test_string1.size(), pool.get());
    auto bytes_get = MemorySegmentUtils::GetBytes(segs, /*base_offset=*/0,
                                                  /*size_in_bytes=*/str_size, pool.get());
    ASSERT_EQ(*bytes_get, *bytes1);
    std::string str_serialize = std::string(bytes_serialize->data(), bytes_serialize->size());
    ASSERT_EQ(test_string1, str_serialize);

    // test MultiSegments
    std::vector<MemorySegment> segs2 = {MemorySegment::AllocateHeapMemory(str_size, pool.get()),
                                        MemorySegment::AllocateHeapMemory(str_size, pool.get())};
    MemorySegmentUtils::CopyFromBytes(&segs2, 0, *bytes2, 0, test_string2.size());
    PAIMON_UNIQUE_PTR<Bytes> bytes_serialize2 =
        MemorySegmentUtils::CopyToBytes(segs2, 0, test_string2.size(), pool.get());
    std::string str_serialize2 = std::string(bytes_serialize2->data(), bytes_serialize2->size());
    ASSERT_EQ(test_string2, str_serialize2);

    std::shared_ptr<Bytes> bytes_serialize3 = MemorySegmentUtils::GetBytes(
        segs2, test_string2.size() - test_string1.size(), test_string1.size(), pool.get());
    std::string str_serialize3 = std::string(bytes_serialize3->data(), bytes_serialize3->size());
    ASSERT_EQ(test_string1, str_serialize3);
}

TEST(MemorySegmentUtilsTest, TestCopyToUnsafe) {
    auto pool = GetDefaultPool();
    int32_t str_size = 1024;
    std::string test_string1, test_string2;
    test_string1.reserve(str_size);
    test_string2.reserve(str_size);
    for (int32_t j = 0; j < str_size; j++) {
        test_string1 += paimon::test::RandomNumber(0, 25) + 'a';
        test_string2 += paimon::test::RandomNumber(0, 25) + 'a';
    }
    test_string2 += test_string1;
    std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes(test_string1, pool.get());
    std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes(test_string2, pool.get());

    std::vector<MemorySegment> segs = {MemorySegment::AllocateHeapMemory(str_size, pool.get())};
    MemorySegmentUtils::CopyFromBytes(&segs, 0, *bytes1, 0, test_string1.size());
    std::shared_ptr<Bytes> bytes_serialize = Bytes::AllocateBytes(test_string1.size(), pool.get());
    MemorySegmentUtils::CopyToUnsafe(segs, 0, bytes_serialize->data(), test_string1.size());
    ASSERT_EQ(test_string1, std::string(bytes_serialize->data(), bytes_serialize->size()));

    // test MultiSegments
    std::vector<MemorySegment> segs2 = {MemorySegment::AllocateHeapMemory(str_size, pool.get()),
                                        MemorySegment::AllocateHeapMemory(str_size, pool.get())};
    MemorySegmentUtils::CopyFromBytes(&segs2, 0, *bytes2, 0, test_string2.size());
    std::shared_ptr<Bytes> bytes_serialize2 = Bytes::AllocateBytes(test_string1.size(), pool.get());
    MemorySegmentUtils::CopyToUnsafe(segs2, test_string2.size() - test_string1.size(),
                                     bytes_serialize2->data(), test_string1.size());
    ASSERT_EQ(test_string1, std::string(bytes_serialize2->data(), bytes_serialize2->size()));
}

TEST(MemorySegmentUtilsTest, TestSetAndUnSet) {
    auto pool = GetDefaultPool();
    int32_t str_size = 1024;
    std::string test_string1, test_string2;
    test_string1.reserve(str_size);
    test_string2.reserve(str_size);
    for (int32_t j = 0; j < str_size; j++) {
        test_string1 += paimon::test::RandomNumber(0, 25) + 'a';
        test_string2 += paimon::test::RandomNumber(0, 25) + 'a';
    }
    test_string2 += test_string1;
    std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes(test_string2, pool.get());

    std::vector<MemorySegment> segs = {MemorySegment::AllocateHeapMemory(str_size, pool.get()),
                                       MemorySegment::AllocateHeapMemory(str_size, pool.get())};
    MemorySegmentUtils::CopyFromBytes(&segs, 0, *bytes2, 0, test_string2.size());
    int32_t index = paimon::test::RandomNumber(0, str_size - 1);
    MemorySegmentUtils::BitUnSet(&segs, str_size, index);
    ASSERT_FALSE(MemorySegmentUtils::BitGet(segs, str_size, index));
    MemorySegmentUtils::BitSet(&segs, str_size, index);
    ASSERT_TRUE(MemorySegmentUtils::BitGet(segs, str_size, index));
    MemorySegmentUtils::BitSet(&segs[0], /*base_offset=*/0, index);
    ASSERT_TRUE(MemorySegmentUtils::BitGet(segs[0], /*base_offset=*/0, index));
    MemorySegmentUtils::BitUnSet(&segs[0], /*base_offset=*/0, index);
    ASSERT_FALSE(MemorySegmentUtils::BitGet(segs[0], /*base_offset=*/0, index));
}

TEST(MemorySegmentUtilsTest, TestCopyMultiSegmentsFromBytes) {
    auto pool = GetDefaultPool();
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes("abcdef", pool.get());
    int32_t segment_size = 10;
    std::vector<MemorySegment> segs = {MemorySegment::AllocateHeapMemory(segment_size, pool.get()),
                                       MemorySegment::AllocateHeapMemory(segment_size, pool.get())};
    {
        MemorySegmentUtils::CopyMultiSegmentsFromBytes(&segs, /*offset=*/3, *bytes,
                                                       /*bytes_offset=*/0,
                                                       /*num_bytes=*/bytes->size());
        auto result_bytes =
            MemorySegmentUtils::CopyToBytes(segs, /*offset=*/3,
                                            /*num_bytes=*/bytes->size(), pool.get());
        ASSERT_EQ(*bytes, *result_bytes);
    }
    {
        MemorySegmentUtils::CopyMultiSegmentsFromBytes(&segs, /*offset=*/12, *bytes,
                                                       /*bytes_offset=*/0,
                                                       /*num_bytes=*/bytes->size());
        auto result_bytes =
            MemorySegmentUtils::CopyToBytes(segs, /*offset=*/12,
                                            /*num_bytes=*/bytes->size(), pool.get());
        ASSERT_EQ(*bytes, *result_bytes);
    }
}

TEST(MemorySegmentUtilsTest, TestCopyToStream) {
    auto pool = GetDefaultPool();
    int32_t segment_size = 3;
    std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes("abc", pool.get());
    std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes("def", pool.get());
    std::vector<MemorySegment> segs = {MemorySegment::Wrap(bytes1), MemorySegment::Wrap(bytes2)};
    {
        MemorySegmentOutputStream out(/*segment_size=*/segment_size, pool);
        ASSERT_OK(MemorySegmentUtils::CopyToStream(segs, /*offset=*/0,
                                                   /*size_in_bytes=*/segment_size * 2, &out));
        std::shared_ptr<Bytes> expected_bytes = Bytes::AllocateBytes("abcdef", pool.get());
        auto bytes =
            MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
        ASSERT_EQ(*expected_bytes, *bytes);
    }
    {
        MemorySegmentOutputStream out(/*segment_size=*/segment_size, pool);
        ASSERT_OK(MemorySegmentUtils::CopyToStream(segs, /*offset=*/2, /*size_in_bytes=*/4, &out));
        std::shared_ptr<Bytes> expected_bytes = Bytes::AllocateBytes("cdef", pool.get());
        auto bytes =
            MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
        ASSERT_EQ(*expected_bytes, *bytes);
    }
    {
        MemorySegmentOutputStream out(/*segment_size=*/segment_size, pool);
        ASSERT_OK(MemorySegmentUtils::CopyToStream(segs, /*offset=*/4, /*size_in_bytes=*/2, &out));
        std::shared_ptr<Bytes> expected_bytes = Bytes::AllocateBytes("ef", pool.get());
        auto bytes =
            MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
        ASSERT_EQ(*expected_bytes, *bytes);
    }
}

TEST(MemorySegmentUtilsTest, TestFind) {
    auto pool = GetDefaultPool();
    std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes("abc", pool.get());
    std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes("def", pool.get());
    std::shared_ptr<Bytes> bytes3 = Bytes::AllocateBytes("adef", pool.get());
    std::shared_ptr<Bytes> bytes4 = Bytes::AllocateBytes("ghi", pool.get());
    std::vector<MemorySegment> segs1 = {MemorySegment::Wrap(bytes1),
                                        MemorySegment::Wrap(bytes2)};  // abcdef
    std::vector<MemorySegment> segs2 = {MemorySegment::Wrap(bytes3),
                                        MemorySegment::Wrap(bytes4)};  // adefghi
    // find ""
    ASSERT_EQ(1, MemorySegmentUtils::Find(segs1, /*offset1=*/1, /*num_bytes1=*/5, segs2,
                                          /*offset2=*/0, /*num_bytes2=*/0));
    // find "def"
    ASSERT_EQ(3, MemorySegmentUtils::Find(segs1, /*offset1=*/0, /*num_bytes1=*/6, segs2,
                                          /*offset2=*/1, /*num_bytes2=*/3));
    // find "defg"
    ASSERT_EQ(-1, MemorySegmentUtils::Find(segs1, /*offset1=*/0, /*num_bytes1=*/6, segs2,
                                           /*offset2=*/1, /*num_bytes2=*/4));
    // find "de" in "abc" of segs1
    ASSERT_EQ(-1, MemorySegmentUtils::Find(segs1, /*offset1=*/0, /*num_bytes1=*/3, segs2,
                                           /*offset2=*/1, /*num_bytes2=*/2));
    // find "a" in "abc" of segs1
    ASSERT_EQ(0, MemorySegmentUtils::Find(segs1, /*offset1=*/0, /*num_bytes1=*/3, segs2,
                                          /*offset2=*/0, /*num_bytes2=*/1));
}

}  // namespace paimon::test
