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

#include "paimon/core/mergetree/leveled_merger.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class LeveledMergerTest : public ::testing::Test {
 protected:
    FileChannelInfo MakeFile(int32_t id, int64_t size) {
        return FileChannelInfo{FileIOChannel::ID(std::to_string(id)), size};
    }

    LeveledMerger::MergeFn CreateMockMergeFn() {
        return [this](const std::vector<FileChannelInfo>& inputs) -> Result<FileChannelInfo> {
            merge_call_count_++;
            int64_t total_size = 0;
            for (const auto& f : inputs) {
                total_size += f.file_size;
            }
            return MakeFile(next_file_id_++, total_size);
        };
    }

    LeveledMerger::MergeFn CreateFailingMergeFn() {
        return [this](const std::vector<FileChannelInfo>&) -> Result<FileChannelInfo> {
            merge_call_count_++;
            return Status::IOError("simulated write failure");
        };
    }

    int32_t merge_call_count_ = 0;
    int32_t next_file_id_ = 1000;
};

TEST_F(LeveledMergerTest, NoCompactionBelowFanIn) {
    LeveledMerger merger(4);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));
    merger.AddFile(MakeFile(3, 300));

    ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 0);
    ASSERT_EQ(merger.GetAllFiles().size(), 3);
}

TEST_F(LeveledMergerTest, CompactionTriggeredAtFanIn) {
    LeveledMerger merger(3);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));
    merger.AddFile(MakeFile(3, 300));

    ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(files[0].file_size, 600);
}

TEST_F(LeveledMergerTest, MinimalFanInTwo) {
    LeveledMerger merger(2);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));

    ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(files[0].file_size, 300);
}

TEST_F(LeveledMergerTest, MultiLevelCompaction) {
    LeveledMerger merger(2);

    // Adding 4 files with fan_in=2 should trigger multi-level compaction:
    // Add file 1,2 -> compact to level 1 (1 file at level 1)
    // Add file 3,4 -> compact level 0 to level 1 (2 files at level 1) -> compact level 1
    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 100));
    ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);

    merger.AddFile(MakeFile(3, 100));
    merger.AddFile(MakeFile(4, 100));
    ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));
    // level 0 compaction + level 1 compaction
    ASSERT_EQ(merge_call_count_, 3);

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(files[0].file_size, 400);
}

TEST_F(LeveledMergerTest, ManyFilesWithFanInTwo) {
    LeveledMerger merger(2);

    for (int32_t i = 0; i < 8; ++i) {
        merger.AddFile(MakeFile(i, 100));
        ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));
    }

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(files[0].file_size, 800);
}

TEST_F(LeveledMergerTest, FinalCleanupReducesFileCount) {
    LeveledMerger merger(4);

    // Add 5 files (just above fan_in). Level 0 gets compacted once, leaving:
    // level 0: 1 file, level 1: 1 file
    for (int32_t i = 0; i < 5; ++i) {
        merger.AddFile(MakeFile(i, 100));
        ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));
    }

    auto files_before = merger.GetAllFiles();
    ASSERT_EQ(files_before.size(), 2);

    ASSERT_OK(merger.RunFinalCleanupIfNeeded(1, CreateMockMergeFn()));

    auto files_after = merger.GetAllFiles();
    ASSERT_EQ(files_after.size(), 1);
}

TEST_F(LeveledMergerTest, FinalCleanupMergesSmallestFirst) {
    LeveledMerger merger(10);

    merger.AddFile(MakeFile(1, 1000));
    merger.AddFile(MakeFile(2, 10));
    merger.AddFile(MakeFile(3, 20));
    merger.AddFile(MakeFile(4, 500));

    // target=2, need to eliminate 2 files, so merge 3 smallest into 1
    ASSERT_OK(merger.RunFinalCleanupIfNeeded(2, CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 2);

    // The merged file should be 10+20+500=530 (3 smallest merged)
    int64_t total = 0;
    for (const auto& f : files) {
        total += f.file_size;
    }
    ASSERT_EQ(total, 1530);
}

TEST_F(LeveledMergerTest, FinalCleanupNoOpWhenAlreadyBelowTarget) {
    LeveledMerger merger(4);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));

    ASSERT_OK(merger.RunFinalCleanupIfNeeded(3, CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 0);
    ASSERT_EQ(merger.GetAllFiles().size(), 2);
}

TEST_F(LeveledMergerTest, FinalCleanupConvergesToTarget) {
    LeveledMerger merger(3);

    // Add many files without running compaction (fan_in large enough)
    LeveledMerger merger2(100);
    for (int32_t i = 0; i < 20; ++i) {
        merger2.AddFile(MakeFile(i, (i + 1) * 10));
    }
    ASSERT_EQ(merger2.GetAllFiles().size(), 20);

    ASSERT_OK(merger2.RunFinalCleanupIfNeeded(3, CreateMockMergeFn()));
    ASSERT_LE(static_cast<int32_t>(merger2.GetAllFiles().size()), 3);
}

TEST_F(LeveledMergerTest, MergeFnFailurePreservesState) {
    LeveledMerger merger(2);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));

    auto status = merger.RunCompactionIfNeeded(CreateFailingMergeFn());
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(merge_call_count_, 1);

    // Files should still be present since merge failed
    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 2);
}

TEST_F(LeveledMergerTest, ClearRemovesAllFiles) {
    LeveledMerger merger(4);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));
    merger.AddFile(MakeFile(3, 300));

    merger.Clear();
    ASSERT_EQ(merger.GetAllFiles().size(), 0);
}

TEST_F(LeveledMergerTest, SetMaxFanInAffectsCompaction) {
    LeveledMerger merger(4);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));
    merger.AddFile(MakeFile(3, 300));

    // No compaction at fan_in=4
    ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 0);

    // Lower fan_in to 3, now compaction should trigger
    merger.SetMaxFanIn(3);
    ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);
    ASSERT_EQ(merger.GetAllFiles().size(), 1);
}

TEST_F(LeveledMergerTest, CompactionOnlyTakesFanInFilesFromLevel) {
    LeveledMerger merger(3);

    // Add 5 files to level 0 (exceeds fan_in=3)
    for (int32_t i = 0; i < 5; ++i) {
        merger.AddFile(MakeFile(i, 100));
    }

    ASSERT_OK(merger.RunCompactionIfNeeded(CreateMockMergeFn()));

    // First compaction takes 3 from level 0 -> 1 at level 1
    // Remaining: 2 at level 0, 1 at level 1 = 3 total
    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 3);
}

}  // namespace paimon::test
