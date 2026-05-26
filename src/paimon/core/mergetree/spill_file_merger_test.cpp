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

#include "paimon/core/mergetree/spill_file_merger.h"

#include <algorithm>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class SpillFileMergerTest : public ::testing::Test {
 protected:
    FileChannelInfo MakeFile(int32_t id, int64_t size) {
        return FileChannelInfo{FileIOChannel::ID(std::to_string(id)), size};
    }

    SpillFileMerger::MergeFn CreateMockMergeFn() {
        return [this](const std::vector<FileChannelInfo>& inputs) -> Result<FileChannelInfo> {
            merge_call_count_++;
            int64_t total_size = 0;
            for (const auto& file : inputs) {
                total_size += file.file_size;
            }
            return MakeFile(next_file_id_++, total_size);
        };
    }

    SpillFileMerger::MergeFn CreateFailingMergeFn() {
        return [this](const std::vector<FileChannelInfo>&) -> Result<FileChannelInfo> {
            merge_call_count_++;
            return Status::IOError("simulated write failure");
        };
    }

    int32_t merge_call_count_ = 0;
    int32_t next_file_id_ = 1000;
};

TEST_F(SpillFileMergerTest, NoMergeBelowFanIn) {
    SpillFileMerger merger(4);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));
    merger.AddFile(MakeFile(3, 300));

    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 0);
    ASSERT_EQ(merger.GetAllFiles().size(), 3);
}

TEST_F(SpillFileMergerTest, MergeTriggeredAtFanIn) {
    SpillFileMerger merger(3);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));
    merger.AddFile(MakeFile(3, 300));

    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(files[0].file_size, 600);
}

TEST_F(SpillFileMergerTest, MinimalFanInTwo) {
    SpillFileMerger merger(2);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));

    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(files[0].file_size, 300);
}

TEST_F(SpillFileMergerTest, MultiLevelMerge) {
    SpillFileMerger merger(2);

    // Adding 4 files with fan_in=2 should trigger multi-level merge:
    // Add file 1,2 -> merge to level 1 (1 file at level 1)
    // Add file 3,4 -> merge level 0 to level 1 (2 files at level 1) -> merge level 1
    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 100));
    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);

    merger.AddFile(MakeFile(3, 100));
    merger.AddFile(MakeFile(4, 100));
    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    // level 0 merge + level 1 merge
    ASSERT_EQ(merge_call_count_, 3);

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(files[0].file_size, 400);
}

TEST_F(SpillFileMergerTest, ManyFilesWithFanInTwo) {
    SpillFileMerger merger(2);

    for (int32_t i = 0; i < 8; ++i) {
        merger.AddFile(MakeFile(i, 100));
        ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    }

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(files[0].file_size, 800);
}

TEST_F(SpillFileMergerTest, FinalCleanupReducesFileCount) {
    SpillFileMerger merger(4);

    // Add 5 files (just above fan_in). Level 0 gets merged once, leaving:
    // level 0: 1 file, level 1: 1 file
    for (int32_t i = 0; i < 5; ++i) {
        merger.AddFile(MakeFile(i, 100));
        ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    }

    auto files_before = merger.GetAllFiles();
    ASSERT_EQ(files_before.size(), 2);

    ASSERT_OK(merger.RunFinalMergeIfNeeded(1, CreateMockMergeFn()));

    auto files_after = merger.GetAllFiles();
    ASSERT_EQ(files_after.size(), 1);
}

TEST_F(SpillFileMergerTest, FinalCleanupMergesSmallestFirst) {
    SpillFileMerger merger(10);

    merger.AddFile(MakeFile(1, 1000));
    merger.AddFile(MakeFile(2, 10));
    merger.AddFile(MakeFile(3, 20));
    merger.AddFile(MakeFile(4, 500));

    // target=2, need to eliminate 2 files, so merge 3 smallest into 1
    ASSERT_OK(merger.RunFinalMergeIfNeeded(2, CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);

    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 2);

    // The 3 smallest (10, 20, 500) should be merged into one 530-sized file,
    // leaving the largest (1000) untouched.
    std::vector<int64_t> sizes;
    for (const auto& file : files) {
        sizes.push_back(file.file_size);
    }
    std::sort(sizes.begin(), sizes.end());
    ASSERT_EQ(sizes[0], 530);
    ASSERT_EQ(sizes[1], 1000);
}

TEST_F(SpillFileMergerTest, FinalCleanupNoOpWhenAlreadyBelowTarget) {
    SpillFileMerger merger(4);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));

    ASSERT_OK(merger.RunFinalMergeIfNeeded(3, CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 0);
    ASSERT_EQ(merger.GetAllFiles().size(), 2);
}

TEST_F(SpillFileMergerTest, FinalCleanupConvergesToTarget) {
    // Add many files without running merge (fan_in large enough)
    SpillFileMerger merger(100);
    for (int32_t i = 0; i < 20; ++i) {
        merger.AddFile(MakeFile(i, (i + 1) * 10));
    }
    ASSERT_EQ(merger.GetAllFiles().size(), 20);

    ASSERT_OK(merger.RunFinalMergeIfNeeded(3, CreateMockMergeFn()));
    ASSERT_LE(static_cast<int32_t>(merger.GetAllFiles().size()), 3);
}

TEST_F(SpillFileMergerTest, MergeFnFailurePreservesState) {
    SpillFileMerger merger(2);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));

    auto status = merger.RunMergeIfNeeded(CreateFailingMergeFn());
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(merge_call_count_, 1);

    // Files should still be present since merge failed
    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 2);
}

TEST_F(SpillFileMergerTest, ClearRemovesAllFiles) {
    SpillFileMerger merger(4);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));
    merger.AddFile(MakeFile(3, 300));

    merger.Clear();
    ASSERT_EQ(merger.GetAllFiles().size(), 0);
}

TEST_F(SpillFileMergerTest, SetMaxFanInAffectsMerge) {
    SpillFileMerger merger(4);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));
    merger.AddFile(MakeFile(3, 300));

    // No merge at fan_in=4
    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 0);

    // Lower fan_in to 3, now merge should trigger
    merger.SetMaxFanIn(3);
    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);
    ASSERT_EQ(merger.GetAllFiles().size(), 1);
}

TEST_F(SpillFileMergerTest, SetMaxFanInToLargerValueSuppressesMerge) {
    SpillFileMerger merger(3);

    merger.AddFile(MakeFile(1, 100));
    merger.AddFile(MakeFile(2, 200));
    merger.AddFile(MakeFile(3, 300));

    // At fan_in=3, merge should trigger
    // But first, increase fan_in to 5 before running merge
    merger.SetMaxFanIn(5);
    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 0);

    // All 3 files should still be present
    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 3);

    // Add more files up to 5, still no merge
    merger.AddFile(MakeFile(4, 400));
    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 0);
    ASSERT_EQ(merger.GetAllFiles().size(), 4);

    // Add 5th file, now merge triggers
    merger.AddFile(MakeFile(5, 500));
    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));
    ASSERT_EQ(merge_call_count_, 1);
}

TEST_F(SpillFileMergerTest, MergeOnlyTakesFanInFilesFromLevel) {
    SpillFileMerger merger(3);

    // Add 5 files to level 0 (exceeds fan_in=3)
    for (int32_t i = 0; i < 5; ++i) {
        merger.AddFile(MakeFile(i, 100));
    }

    ASSERT_OK(merger.RunMergeIfNeeded(CreateMockMergeFn()));

    // First merge takes 3 from level 0 -> 1 at level 1
    // Remaining: 2 at level 0, 1 at level 1 = 3 total
    auto files = merger.GetAllFiles();
    ASSERT_EQ(files.size(), 3);
}

}  // namespace paimon::test
