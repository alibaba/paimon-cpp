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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/sst/block_iterator.h"
#include "paimon/common/sst/block_reader.h"
#include "paimon/common/sst/block_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BlockWriteReadTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        comparator_ = [](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
            std::string_view va = a.ReadStringView();
            std::string_view vb = b.ReadStringView();
            if (va < vb) {
                return -1;
            }
            if (va > vb) {
                return 1;
            }
            return 0;
        };
    }

    std::shared_ptr<Bytes> MakeBytes(const std::string& str) const {
        return std::make_shared<Bytes>(str, pool_.get());
    }

    /// Build a block with the given key-value pairs via BlockWriter.
    MemorySlice BuildBlock(const std::vector<std::pair<std::string, std::string>>& pairs,
                           BlockWriter* writer) const {
        for (const auto& [k, v] : pairs) {
            auto key = MakeBytes(k);
            auto value = MakeBytes(v);
            EXPECT_OK(writer->Write(key, value));
        }
        EXPECT_OK_AND_ASSIGN(auto result, writer->Finish());
        return result;
    }

    std::shared_ptr<MemoryPool> pool_;
    MemorySlice::SliceComparator comparator_;
};

TEST_F(BlockWriteReadTest, AlignedWriteAndRead) {
    BlockWriter writer(1024, pool_);
    ASSERT_EQ(writer.Size(), 0);

    auto key1 = MakeBytes("aaaa");
    auto val1 = MakeBytes("1111");
    ASSERT_OK(writer.Write(key1, val1));
    ASSERT_EQ(writer.Size(), 1);
    ASSERT_GT(writer.Memory(), 0);

    auto key2 = MakeBytes("bbbb");
    auto val2 = MakeBytes("2222");
    auto key3 = MakeBytes("cccc");
    auto val3 = MakeBytes("3333");
    ASSERT_OK(writer.Write(key2, val2));
    ASSERT_OK(writer.Write(key3, val3));
    ASSERT_EQ(writer.Size(), 3);

    auto block = BuildBlock({}, &writer);
    ASSERT_GT(block.Length(), 0);
    // Last byte should be ALIGNED (0) because all kv pairs are same-size
    ASSERT_EQ(block.ReadByte(block.Length() - 1), static_cast<int8_t>(BlockAlignedType::ALIGNED));

    // Read back
    ASSERT_OK_AND_ASSIGN(auto reader, BlockReader::Create(block, comparator_));
    ASSERT_EQ(reader->RecordCount(), 3);
    ASSERT_NE(reader->Comparator(), nullptr);

    // SeekTo returns byte positions, should be increasing
    ASSERT_EQ(reader->SeekTo(0), 0);
    ASSERT_GT(reader->SeekTo(1), 0);
    ASSERT_GT(reader->SeekTo(2), reader->SeekTo(1));

    // BlockInput should be readable
    ASSERT_TRUE(reader->BlockInput().IsReadable());

    // Iterate and verify all entries
    auto iter = reader->Iterator();
    ASSERT_TRUE(iter->HasNext());
    ASSERT_OK_AND_ASSIGN(auto entry1, iter->Next());
    ASSERT_EQ(entry1.key.ReadStringView(), "aaaa");
    ASSERT_EQ(entry1.value.ReadStringView(), "1111");

    ASSERT_OK_AND_ASSIGN(auto entry2, iter->Next());
    ASSERT_EQ(entry2.key.ReadStringView(), "bbbb");
    ASSERT_EQ(entry2.value.ReadStringView(), "2222");

    ASSERT_OK_AND_ASSIGN(auto entry3, iter->Next());
    ASSERT_EQ(entry3.key.ReadStringView(), "cccc");
    ASSERT_EQ(entry3.value.ReadStringView(), "3333");

    ASSERT_FALSE(iter->HasNext());
}

TEST_F(BlockWriteReadTest, UnalignedWriteAndRead) {
    BlockWriter writer(1024, pool_, /*aligned=*/false);
    auto block = BuildBlock({{"a", "short"}, {"bb", "a_longer_value"}}, &writer);
    ASSERT_GT(block.Length(), 0);
    // Last byte should be UNALIGNED (1)
    ASSERT_EQ(block.ReadByte(block.Length() - 1), static_cast<int8_t>(BlockAlignedType::UNALIGNED));

    ASSERT_OK_AND_ASSIGN(auto reader, BlockReader::Create(block, comparator_));
    ASSERT_EQ(reader->RecordCount(), 2);

    auto iter = reader->Iterator();
    ASSERT_OK_AND_ASSIGN(auto entry1, iter->Next());
    ASSERT_EQ(entry1.key.ReadStringView(), "a");
    ASSERT_EQ(entry1.value.ReadStringView(), "short");

    ASSERT_OK_AND_ASSIGN(auto entry2, iter->Next());
    ASSERT_EQ(entry2.key.ReadStringView(), "bb");
    ASSERT_EQ(entry2.value.ReadStringView(), "a_longer_value");

    ASSERT_FALSE(iter->HasNext());
}

TEST_F(BlockWriteReadTest, EmptyBlockWriteAndRead) {
    BlockWriter writer(1024, pool_);

    ASSERT_OK_AND_ASSIGN(auto block, writer.Finish());
    ASSERT_GT(block.Length(), 0);
    // Empty block must be UNALIGNED (1)
    ASSERT_EQ(block.ReadByte(block.Length() - 1), static_cast<int8_t>(BlockAlignedType::UNALIGNED));

    // Reader should see 0 records
    ASSERT_OK_AND_ASSIGN(auto reader, BlockReader::Create(block, comparator_));
    ASSERT_EQ(reader->RecordCount(), 0);

    auto iter = reader->Iterator();
    ASSERT_FALSE(iter->HasNext());
}

TEST_F(BlockWriteReadTest, ResetAndRewrite) {
    BlockWriter writer(1024, pool_);

    auto key = MakeBytes("old_key");
    auto val = MakeBytes("old_val");
    ASSERT_OK(writer.Write(key, val));
    ASSERT_EQ(writer.Size(), 1);

    writer.Reset();
    ASSERT_EQ(writer.Size(), 0);

    // Write new data and read back
    auto block = BuildBlock({{"new_key", "new_val"}}, &writer);
    ASSERT_OK_AND_ASSIGN(auto reader, BlockReader::Create(block, comparator_));
    ASSERT_EQ(reader->RecordCount(), 1);

    auto iter = reader->Iterator();
    ASSERT_OK_AND_ASSIGN(auto entry, iter->Next());
    ASSERT_EQ(entry.key.ReadStringView(), "new_key");
    ASSERT_EQ(entry.value.ReadStringView(), "new_val");
}

TEST_F(BlockWriteReadTest, MemoryAlignedVsUnaligned) {
    BlockWriter aligned_writer(1024, pool_);
    auto k1 = MakeBytes("aaaa");
    auto v1 = MakeBytes("1111");
    auto k2 = MakeBytes("bbbb");
    auto v2 = MakeBytes("2222");
    ASSERT_OK(aligned_writer.Write(k1, v1));
    ASSERT_OK(aligned_writer.Write(k2, v2));

    BlockWriter unaligned_writer(1024, pool_, /*aligned=*/false);
    auto k3 = MakeBytes("aaaa");
    auto v3 = MakeBytes("1111");
    auto k4 = MakeBytes("bbbb");
    auto v4 = MakeBytes("2222");
    ASSERT_OK(unaligned_writer.Write(k3, v3));
    ASSERT_OK(unaligned_writer.Write(k4, v4));

    // Unaligned memory should be larger due to position index overhead
    ASSERT_GT(unaligned_writer.Memory(), aligned_writer.Memory());
}

TEST_F(BlockWriteReadTest, SkipKeyAndReadValue) {
    BlockWriter writer(1024, pool_);
    auto block = BuildBlock({{"key1", "val1"}, {"key2", "val2"}}, &writer);

    ASSERT_OK_AND_ASSIGN(auto reader, BlockReader::Create(block, comparator_));
    auto iter = reader->Iterator();

    ASSERT_TRUE(iter->HasNext());
    ASSERT_OK_AND_ASSIGN(auto value1, iter->SkipKeyAndReadValue());
    ASSERT_EQ(value1.ReadStringView(), "val1");

    ASSERT_TRUE(iter->HasNext());
    ASSERT_OK_AND_ASSIGN(auto value2, iter->SkipKeyAndReadValue());
    ASSERT_EQ(value2.ReadStringView(), "val2");

    ASSERT_FALSE(iter->HasNext());
}

TEST_F(BlockWriteReadTest, IteratorSeekTo) {
    BlockWriter writer(1024, pool_);
    auto block =
        BuildBlock({{"apple", "1"}, {"banana", "2"}, {"cherry", "3"}, {"date", "4"}}, &writer);

    ASSERT_OK_AND_ASSIGN(auto reader, BlockReader::Create(block, comparator_));

    // Exact match
    {
        auto iter = reader->Iterator();
        auto target = MemorySlice::Wrap(MakeBytes("cherry"));
        ASSERT_OK_AND_ASSIGN(bool found, iter->SeekTo(target));
        ASSERT_TRUE(found);
        ASSERT_TRUE(iter->HasNext());
        ASSERT_OK_AND_ASSIGN(auto entry, iter->Next());
        ASSERT_EQ(entry.key.ReadStringView(), "cherry");
        ASSERT_EQ(entry.value.ReadStringView(), "3");
    }

    // No exact match: "blueberry" should land on "cherry" (first key >= target)
    {
        auto iter = reader->Iterator();
        auto target = MemorySlice::Wrap(MakeBytes("blueberry"));
        ASSERT_OK_AND_ASSIGN(bool found, iter->SeekTo(target));
        ASSERT_FALSE(found);
        ASSERT_TRUE(iter->HasNext());
        ASSERT_OK_AND_ASSIGN(auto entry, iter->Next());
        ASSERT_EQ(entry.key.ReadStringView(), "cherry");
    }
}

}  // namespace paimon::test
