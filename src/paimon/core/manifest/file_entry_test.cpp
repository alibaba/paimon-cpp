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

#include "paimon/core/manifest/file_entry.h"

#include <cassert>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class MockFileEntry : public FileEntry {
 public:
    MockFileEntry(FileKind kind, BinaryRow partition, int32_t bucket, int32_t level,
                  const std::string& file_name,
                  const std::optional<std::string>& external_path = std::nullopt)
        : kind_(kind),
          partition_(partition),
          bucket_(bucket),
          level_(level),
          file_name_(file_name),
          external_path_(external_path),
          min_key_(BinaryRow::EmptyRow()),
          max_key_(BinaryRow::EmptyRow()) {}

    const FileKind& Kind() const override {
        return kind_;
    }
    const BinaryRow& Partition() const override {
        return partition_;
    }
    int32_t Bucket() const override {
        return bucket_;
    }
    int32_t Level() const override {
        return level_;
    }
    const std::string& FileName() const override {
        return file_name_;
    }
    const std::optional<std::string>& ExternalPath() const override {
        return external_path_;
    }
    Identifier CreateIdentifier() const override {
        return Identifier(partition_, bucket_, level_, file_name_, external_path_);
    }
    const BinaryRow& MinKey() const override {
        assert(false);
        return min_key_;
    }
    const BinaryRow& MaxKey() const override {
        assert(false);
        return max_key_;
    }

 private:
    FileKind kind_;
    BinaryRow partition_;
    int32_t bucket_;
    int32_t level_;
    std::string file_name_;
    std::optional<std::string> external_path_;
    BinaryRow min_key_;
    BinaryRow max_key_;
};

class FileEntryTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    BinaryRow GetPartition(const std::string& part_str) {
        BinaryRow part(/*arity=*/1);
        BinaryRowWriter writer(&part, /*initial_size=*/20, pool_.get());
        writer.WriteString(0, BinaryString::FromString(part_str, pool_.get()));
        return part;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(FileEntryTest, TestMergeEntriesSimple) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file2");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file3");
    entries.emplace_back(FileKind::Delete(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");

    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file2", merged_entries[0].FileName());
    ASSERT_EQ("file3", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFile) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_NOK_WITH_MSG(FileEntry::MergeEntries(entries, &merged_entries),
                        "which is already added.");
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFileWithDiffPart) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("1"), /*bucket=*/0, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file1", merged_entries[0].FileName());
    ASSERT_EQ("file1", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFileWithDiffBucket) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/1, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file1", merged_entries[0].FileName());
    ASSERT_EQ("file1", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFileWithDiffLevel) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/1, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file1", merged_entries[0].FileName());
    ASSERT_EQ("file1", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFileWithDiffExternalPath) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1",
                         "/tmp/external_path1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1",
                         "/tmp/external_path2");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file1", merged_entries[0].FileName());
    ASSERT_EQ("file1", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddAndDelete) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Delete(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(0u, merged_entries.size());
}

TEST_F(FileEntryTest, TestMergeEntriesDeleteAndAdd) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Delete(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_NOK(FileEntry::MergeEntries(entries, &merged_entries));
}

}  // namespace paimon::test
