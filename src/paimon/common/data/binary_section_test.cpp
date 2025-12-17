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

#include "paimon/common/data/binary_section.h"

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {

class BinarySectionTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();

        segments_ = {MemorySegment::AllocateHeapMemory(1024, pool_.get()),
                     MemorySegment::AllocateHeapMemory(1024, pool_.get())};
        for (int i = 0; i < 1024; ++i) {
            segments_[0].Put(i, static_cast<uint8_t>(i));
            segments_[1].Put(i, static_cast<uint8_t>(i + 1024));
        }
        offset_ = 0;
        size_in_bytes_ = 2048;
        binary_section_ = BinarySection(segments_, offset_, size_in_bytes_);
    }

    std::vector<MemorySegment> segments_;
    int32_t offset_;
    int32_t size_in_bytes_;
    BinarySection binary_section_;
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BinarySectionTest, EqualityOperator) {
    BinarySection other(segments_, offset_, size_in_bytes_);
    EXPECT_TRUE(binary_section_ == other);

    BinarySection different(segments_, offset_, size_in_bytes_ - 1);
    EXPECT_FALSE(binary_section_ == different);
}

TEST_F(BinarySectionTest, ToBytes) {
    auto bytes = binary_section_.ToBytes(pool_.get());
    ASSERT_EQ(bytes->size(), size_in_bytes_);
    for (int i = 0; i < size_in_bytes_; ++i) {
        EXPECT_EQ(static_cast<uint8_t>(bytes->data()[i]), static_cast<uint8_t>(i % 1024));
    }
}

TEST_F(BinarySectionTest, HashCode) {
    int32_t hash_code = binary_section_.HashCode();
    EXPECT_NE(hash_code, 0);
}

TEST_F(BinarySectionTest, ReadBinary) {
    int64_t variable_part_offset_and_len = (static_cast<int64_t>(offset_) << 32) | size_in_bytes_;
    auto bytes =
        BinarySection::ReadBinary(segments_, 0, offset_, variable_part_offset_and_len, pool_.get());
    ASSERT_EQ(bytes->size(), size_in_bytes_);
    for (int i = 0; i < size_in_bytes_; ++i) {
        EXPECT_EQ(static_cast<uint8_t>(bytes->data()[i]), static_cast<uint8_t>(i % 1024));
    }
}

}  // namespace paimon::test
