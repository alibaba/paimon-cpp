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

#include "paimon/core/append/bucketed_append_compact_manager.h"

#include <chrono>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/executor.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/result.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BucketedAppendCompactManagerTest : public testing::Test {
 public:
    void SetUp() override {
        executor_ = CreateDefaultExecutor();
    }

    std::vector<std::shared_ptr<DataFileMeta>> GenerateDataFileMeta() {
        std::vector<std::shared_ptr<DataFileMeta>> metas;
        metas.push_back(DataFileMeta::ForAppend("file1", 100, 100, SimpleStats::EmptyStats(),
                                                /*min_sequence_number=*/1,
                                                /*max_sequence_number=*/10, 0, FileSource::Append(),
                                                std::nullopt, std::nullopt, std::nullopt,
                                                std::nullopt)
                            .value());
        metas.push_back(DataFileMeta::ForAppend("file2", 200, 200, SimpleStats::EmptyStats(),
                                                /*min_sequence_number=*/5,
                                                /*max_sequence_number=*/15, 0, FileSource::Append(),
                                                std::nullopt, std::nullopt, std::nullopt,
                                                std::nullopt)
                            .value());
        metas.push_back(DataFileMeta::ForAppend("file3", 200, 200, SimpleStats::EmptyStats(),
                                                /*min_sequence_number=*/20,
                                                /*max_sequence_number=*/30, 0, FileSource::Append(),
                                                std::nullopt, std::nullopt, std::nullopt,
                                                std::nullopt)
                            .value());
        return metas;
    }

 private:
    void InnerTest(const std::vector<std::shared_ptr<DataFileMeta>>& to_compact_before_pick,
                   bool expected_present,
                   const std::vector<std::shared_ptr<DataFileMeta>>& expected_compact_before,
                   const std::vector<std::shared_ptr<DataFileMeta>>& to_compact_after_pick) {
        int32_t min_file_num = 4;
        int64_t target_file_size = 1024;
        int64_t threshold = target_file_size / 10 * 7;
        BucketedAppendCompactManager manager(
            executor_, to_compact_before_pick,
            /*dv_maintainer=*/nullptr, min_file_num, target_file_size, threshold,
            /*force_rewrite_all_files=*/false, /*rewriter=*/nullptr, /*reporter=*/nullptr,
            /*cancellation_controller=*/std::make_shared<CancellationController>());
        auto actual = manager.PickCompactBefore();
        if (expected_present) {
            ASSERT_TRUE(actual.has_value());
            ExpectVectorsEqual(actual.value(), expected_compact_before);
        } else {
            ASSERT_FALSE(actual.has_value());
        }
        auto pq = manager.GetToCompact();
        std::vector<std::shared_ptr<DataFileMeta>> to_compact;
        while (!pq.empty()) {
            to_compact.push_back(pq.top());
            pq.pop();
        }
        ExpectVectorsEqual(to_compact, to_compact_after_pick);
    }

    std::shared_ptr<DataFileMeta> NewFile(int64_t min_sequence_number,
                                          int64_t max_sequence_number) {
        return std::make_shared<DataFileMeta>(
            /*file_name=*/"", /*file_size=*/max_sequence_number - min_sequence_number + 1,
            /*row_count=*/0,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            /*value_stats=*/SimpleStats::EmptyStats(), min_sequence_number, max_sequence_number,
            /*schema_id=*/0,
            /*level=*/DataFileMeta::DUMMY_LEVEL,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1724090888706ll, 0),
            /*delete_row_count=*/max_sequence_number - min_sequence_number + 1,
            /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    std::shared_ptr<DataFileMeta> NewNamedFile(const std::string& file_name, int64_t file_size,
                                               int64_t min_sequence_number,
                                               int64_t max_sequence_number) {
        return std::make_shared<DataFileMeta>(
            file_name, file_size,
            /*row_count=*/0,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            /*value_stats=*/SimpleStats::EmptyStats(), min_sequence_number, max_sequence_number,
            /*schema_id=*/0,
            /*level=*/DataFileMeta::DUMMY_LEVEL,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1724090888706ll, 0),
            /*delete_row_count=*/0,
            /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    std::shared_ptr<BucketedDvMaintainer> CreateTestDvMaintainer(
        const std::string& root_path,
        const std::map<std::string, std::shared_ptr<DeletionVector>>& deletion_vectors) {
        auto pool = GetDefaultPool();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                             FileSystemFactory::Get("local", root_path, {}));
        auto path_factory = std::make_shared<MockIndexPathFactory>(root_path);
        auto dv_index_file =
            std::make_shared<DeletionVectorsIndexFile>(fs, path_factory, /*bitmap64=*/false, pool);
        return std::make_shared<BucketedDvMaintainer>(dv_index_file, deletion_vectors);
    }

    std::shared_ptr<DeletionVector> CreateSimpleDeletionVector(int32_t deleted_position) {
        RoaringBitmap32 bitmap;
        bitmap.Add(deleted_position);
        return std::make_shared<BitmapDeletionVector>(bitmap);
    }

    void ExpectVectorsEqual(const std::vector<std::shared_ptr<DataFileMeta>>& actual,
                            const std::vector<std::shared_ptr<DataFileMeta>>& expected) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            ASSERT_EQ(*actual[i], *expected[i]);
        }
    }

    std::shared_ptr<Executor> executor_;
};

TEST_F(BucketedAppendCompactManagerTest, TestFileComparatorWithoutOverlap) {
    auto files = GenerateDataFileMeta();
    auto& file1 = files[0];
    auto& file2 = files[1];
    auto& file3 = files[2];

    auto comparator = BucketedAppendCompactManager::FileComparator(false);
    ASSERT_TRUE(comparator(file1, file2));
    ASSERT_FALSE(comparator(file2, file1));
    ASSERT_TRUE(comparator(file1, file3));
    ASSERT_FALSE(comparator(file3, file1));
}

TEST_F(BucketedAppendCompactManagerTest, TestFileComparatorWithOverlap) {
    auto files = GenerateDataFileMeta();
    auto& file1 = files[0];
    auto& file2 = files[1];
    auto& file3 = files[2];

    auto comparator = BucketedAppendCompactManager::FileComparator(true);
    ASSERT_TRUE(comparator(file1, file2));
    ASSERT_FALSE(comparator(file2, file1));
    ASSERT_TRUE(comparator(file1, file3));
    ASSERT_FALSE(comparator(file3, file1));
}

TEST_F(BucketedAppendCompactManagerTest, TestIsOverlap) {
    auto files = GenerateDataFileMeta();
    auto& file1 = files[0];
    auto& file2 = files[1];
    auto& file3 = files[2];

    ASSERT_TRUE(BucketedAppendCompactManager::IsOverlap(file1, file2));
    ASSERT_FALSE(BucketedAppendCompactManager::IsOverlap(file1, file3));
    ASSERT_FALSE(BucketedAppendCompactManager::IsOverlap(file2, file3));
}

TEST_F(BucketedAppendCompactManagerTest, TestPickEmptyAndNotRelease) {
    // 1~50 is small enough, so hold it
    std::vector<std::shared_ptr<DataFileMeta>> to_compact = {NewFile(1, 50)};
    InnerTest(to_compact, /*expected_present=*/false, /*expected_compact_before=*/{}, to_compact);
}

TEST_F(BucketedAppendCompactManagerTest, TestPickPresentWhenEnoughSmallFiles) {
    // All four files are small and should be picked once min_file_num is reached.
    std::vector<std::shared_ptr<DataFileMeta>> to_compact_before_pick = {
        NewFile(1, 100), NewFile(101, 200), NewFile(201, 300), NewFile(301, 400)};
    InnerTest(to_compact_before_pick,
              /*expected_present=*/true,
              /*expected_compact_before=*/to_compact_before_pick,
              /*to_compact_after_pick=*/{});
}

TEST_F(BucketedAppendCompactManagerTest, TestPickEmptyAndRelease) {
    // large file, release
    InnerTest(/*to_compact_before_pick=*/{NewFile(1, 2048)}, /*expected_present=*/false,
              /*expected_compact_before=*/{}, /*to_compact_after_pick=*/{});

    // small file at last, release previous
    InnerTest(/*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 2100)},
              /*expected_present=*/false,
              /*expected_compact_before=*/{}, /*to_compact_after_pick=*/{NewFile(2049, 2100)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 2100), NewFile(2101, 2110)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{},
        /*to_compact_after_pick=*/{NewFile(2049, 2100), NewFile(2101, 2110)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 4096), NewFile(4097, 5000)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{}, /*to_compact_after_pick=*/{NewFile(4097, 5000)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 1024), NewFile(1025, 2049), NewFile(2050, 2500),
                                    NewFile(2501, 4096), NewFile(4097, 6000), NewFile(6001, 7000),
                                    NewFile(7001, 7600)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{},
        /*to_compact_after_pick=*/{NewFile(6001, 7000), NewFile(7001, 7600)});

    // ignore single small file (in the middle)
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 4096), NewFile(4097, 4100),
                                    NewFile(4101, 6150)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{},
        /*to_compact_after_pick=*/{NewFile(4101, 6150)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 4096), NewFile(4097, 5000),
                                    NewFile(5001, 6144), NewFile(6145, 7048)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{}, /*to_compact_after_pick=*/{NewFile(6145, 7048)});

    // wait for more file
    InnerTest(/*to_compact_before_pick=*/{NewFile(1, 500), NewFile(501, 1000)},
              /*expected_present=*/false,
              /*expected_compact_before=*/{},
              /*to_compact_after_pick=*/{NewFile(1, 500), NewFile(501, 1000)});

    InnerTest(/*to_compact_before_pick=*/{NewFile(1, 500), NewFile(501, 1000), NewFile(1001, 2048)},
              /*expected_present=*/false,
              /*expected_compact_before=*/{},
              /*to_compact_after_pick=*/{NewFile(501, 1000), NewFile(1001, 2048)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2050), NewFile(2051, 2100), NewFile(2101, 2110)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{},
        /*to_compact_after_pick=*/{NewFile(2051, 2100), NewFile(2101, 2110)});
}

TEST_F(BucketedAppendCompactManagerTest, TestPick) {
    // fileNum is 13 (which > 4) and totalFileSize is 130 (which < 1024)
    InnerTest({NewFile(1, 10), NewFile(11, 20), NewFile(21, 30), NewFile(31, 40), NewFile(41, 50),
               NewFile(51, 60), NewFile(61, 70), NewFile(71, 80), NewFile(81, 90), NewFile(91, 100),
               NewFile(101, 110), NewFile(111, 120), NewFile(121, 130)},
              /*expected_present=*/true, /*expected_compact_before=*/
              {NewFile(1, 10), NewFile(11, 20), NewFile(21, 30), NewFile(31, 40)},
              /*to_compact_after_pick=*/
              {NewFile(41, 50), NewFile(51, 60), NewFile(61, 70), NewFile(71, 80), NewFile(81, 90),
               NewFile(91, 100), NewFile(101, 110), NewFile(111, 120), NewFile(121, 130)});

    // fileNum is 4 (which > 3) and totalFileSize is 1026 (which > 1024)
    InnerTest({NewFile(1, 2), NewFile(3, 500), NewFile(501, 1000), NewFile(1001, 1025),
               NewFile(1026, 1050)},
              /*expected_present=*/true, /*expected_compact_before=*/
              {NewFile(1, 2), NewFile(3, 500), NewFile(501, 1000), NewFile(1001, 1025)},
              /*to_compact_after_pick=*/{NewFile(1026, 1050)});

    // The window shifts right after large files are dropped, then picks contiguous files.
    InnerTest({NewFile(1, 1022), NewFile(1023, 1024), NewFile(1025, 2050),
               // 2051~2510, ..., 2611~2620
               NewFile(2051, 2510), NewFile(2511, 2520), NewFile(2521, 2530), NewFile(2531, 2540),
               NewFile(2541, 2550), NewFile(2551, 2560), NewFile(2561, 2570), NewFile(2571, 2580),
               NewFile(2581, 2590), NewFile(2591, 2600), NewFile(2601, 2610), NewFile(2611, 2620),
               NewFile(2621, 2630)},
              /*expected_present=*/true,
              /*expected_compact_before=*/
              {NewFile(1023, 1024), NewFile(1025, 2050), NewFile(2051, 2510), NewFile(2511, 2520)},
              /*to_compact_after_pick=*/
              {NewFile(2521, 2530), NewFile(2531, 2540), NewFile(2541, 2550), NewFile(2551, 2560),
               NewFile(2561, 2570), NewFile(2571, 2580), NewFile(2581, 2590), NewFile(2591, 2600),
               NewFile(2601, 2610), NewFile(2611, 2620), NewFile(2621, 2630)});
}

TEST_F(BucketedAppendCompactManagerTest, TestCancelCompactionPropagatesToRewriteLoop) {
    auto cancellation_controller = std::make_shared<CancellationController>();
    auto exit_signal = std::make_shared<std::promise<void>>();
    auto exit_future = exit_signal->get_future();

    auto rewriter = [cancellation_controller,
                     exit_signal](const std::vector<std::shared_ptr<DataFileMeta>>& to_compact)
        -> Result<std::vector<std::shared_ptr<DataFileMeta>>> {
        while (!cancellation_controller->IsCancelled()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        exit_signal->set_value();
        return Status::Invalid("compaction cancelled in rewrite loop");
    };

    BucketedAppendCompactManager manager(
        executor_, {NewFile(1, 100), NewFile(101, 200), NewFile(201, 300), NewFile(301, 400)},
        /*dv_maintainer=*/nullptr,
        /*min_file_num=*/4,
        /*target_file_size=*/1024,
        /*compaction_file_size=*/700,
        /*force_rewrite_all_files=*/false, rewriter,
        /*reporter=*/nullptr, cancellation_controller);

    ASSERT_OK(manager.TriggerCompaction(/*full_compaction=*/true));
    manager.CancelAndWaitCompaction();

    ASSERT_EQ(exit_future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
}

TEST_F(BucketedAppendCompactManagerTest, TestTriggerCompactionResetsCancelFlag) {
    auto cancellation_controller = std::make_shared<CancellationController>();
    cancellation_controller->Cancel();
    auto rewriter = [](const std::vector<std::shared_ptr<DataFileMeta>>& to_compact)
        -> Result<std::vector<std::shared_ptr<DataFileMeta>>> { return to_compact; };

    BucketedAppendCompactManager manager(
        executor_, {NewFile(1, 100), NewFile(101, 200), NewFile(201, 300), NewFile(301, 400)},
        /*dv_maintainer=*/nullptr,
        /*min_file_num=*/4,
        /*target_file_size=*/1024,
        /*compaction_file_size=*/700,
        /*force_rewrite_all_files=*/false, rewriter,
        /*reporter=*/nullptr, cancellation_controller);

    ASSERT_OK(manager.TriggerCompaction(/*full_compaction=*/true));
    ASSERT_FALSE(cancellation_controller->IsCancelled());
}

TEST_F(BucketedAppendCompactManagerTest, TestHasDeletionFileLargeFileWithDvRetained) {
    // A large file with a deletion vector should NOT be skipped during full compaction.
    // compaction_file_size = 500, so "big_file" (size=2000) is a large file.
    // Because it has a deletion vector, HasDeletionFile returns true and keeps it in compaction.
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
    deletion_vectors["big_file"] = CreateSimpleDeletionVector(0);
    auto dv_maintainer = CreateTestDvMaintainer(dir->Str(), deletion_vectors);

    auto rewriter = [](const std::vector<std::shared_ptr<DataFileMeta>>& to_compact)
        -> Result<std::vector<std::shared_ptr<DataFileMeta>>> { return to_compact; };

    // big_file: size=2000 → exceeds compaction_file_size=500
    // small_file1, small_file2: size=100 each → below threshold
    auto big_file = NewNamedFile("big_file", 2000, 1, 2000);
    auto small_file1 = NewNamedFile("small_file1", 100, 2001, 2100);
    auto small_file2 = NewNamedFile("small_file2", 100, 2101, 2200);

    BucketedAppendCompactManager manager(
        executor_, {big_file, small_file1, small_file2}, dv_maintainer,
        /*min_file_num=*/4,
        /*target_file_size=*/1024,
        /*compaction_file_size=*/500,
        /*force_rewrite_all_files=*/false, rewriter,
        /*reporter=*/nullptr,
        /*cancellation_controller=*/std::make_shared<CancellationController>());

    ASSERT_OK(manager.TriggerCompaction(/*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(auto result, manager.GetCompactionResult(/*blocking=*/true));
    ASSERT_TRUE(result.has_value());

    // big_file has a deletion vector, so HasDeletionFile returns true and it is retained.
    // All 3 files should appear in Before().
    const auto& before = result.value()->Before();
    ASSERT_EQ(before.size(), 3);
    ASSERT_EQ(before[0]->file_name, "big_file");
    ASSERT_EQ(before[1]->file_name, "small_file1");
    ASSERT_EQ(before[2]->file_name, "small_file2");
}

TEST_F(BucketedAppendCompactManagerTest, TestHasDeletionFileLargeFileWithoutDvSkipped) {
    // A large file WITHOUT a deletion vector should be skipped during full compaction.
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    // dv_maintainer exists but has no deletion vector for "big_file"
    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
    deletion_vectors["other_file"] = CreateSimpleDeletionVector(0);
    auto dv_maintainer = CreateTestDvMaintainer(dir->Str(), deletion_vectors);

    auto rewriter = [](const std::vector<std::shared_ptr<DataFileMeta>>& to_compact)
        -> Result<std::vector<std::shared_ptr<DataFileMeta>>> { return to_compact; };

    // big_file: size=2000, exceeds compaction_file_size=500, but has no DV → skipped
    auto big_file = NewNamedFile("big_file", 2000, 1, 2000);
    auto small_file1 = NewNamedFile("small_file1", 100, 2001, 2100);
    auto small_file2 = NewNamedFile("small_file2", 100, 2101, 2200);

    BucketedAppendCompactManager manager(
        executor_, {big_file, small_file1, small_file2}, dv_maintainer,
        /*min_file_num=*/4,
        /*target_file_size=*/1024,
        /*compaction_file_size=*/500,
        /*force_rewrite_all_files=*/false, rewriter,
        /*reporter=*/nullptr,
        /*cancellation_controller=*/std::make_shared<CancellationController>());

    ASSERT_OK(manager.TriggerCompaction(/*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(auto result, manager.GetCompactionResult(/*blocking=*/true));
    ASSERT_TRUE(result.has_value());

    // big_file has no deletion vector, so HasDeletionFile returns false and it is skipped.
    // Only small files remain in Before().
    const auto& before = result.value()->Before();
    ASSERT_EQ(before.size(), 2);
    ASSERT_EQ(before[0]->file_name, "small_file1");
    ASSERT_EQ(before[1]->file_name, "small_file2");
}

TEST_F(BucketedAppendCompactManagerTest, TestDoCompactWithNullDvMaintainerWithLessBigFile) {
    // When dv_maintainer is nullptr, so large files should be skipped.
    auto rewriter = [](const std::vector<std::shared_ptr<DataFileMeta>>& to_compact)
        -> Result<std::vector<std::shared_ptr<DataFileMeta>>> { return to_compact; };

    auto big_file = NewNamedFile("big_file", 2000, 1, 2000);
    auto small_file1 = NewNamedFile("small1", 100, 2001, 2100);
    auto small_file2 = NewNamedFile("small2", 100, 2101, 2200);
    auto small_file3 = NewNamedFile("small3", 100, 2201, 2300);

    BucketedAppendCompactManager manager(
        executor_, {big_file, small_file1, small_file2, small_file3},
        /*dv_maintainer=*/nullptr,
        /*min_file_num=*/4,
        /*target_file_size=*/1024,
        /*compaction_file_size=*/500,
        /*force_rewrite_all_files=*/false, rewriter,
        /*reporter=*/nullptr,
        /*cancellation_controller=*/std::make_shared<CancellationController>());

    ASSERT_OK(manager.TriggerCompaction(/*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(auto result, manager.GetCompactionResult(/*blocking=*/true));
    ASSERT_TRUE(result.has_value());

    // No dv_maintainer → HasDeletionFile returns false → big_file is skipped.
    // Only the 3 small files appear in Before().
    const auto& before = result.value()->Before();
    ASSERT_EQ(before.size(), 3);
    ASSERT_EQ(before[0]->file_name, "small1");
    ASSERT_EQ(before[1]->file_name, "small2");
    ASSERT_EQ(before[2]->file_name, "small3");
}

TEST_F(BucketedAppendCompactManagerTest, TestDoNoCompact) {
    auto rewriter = [](const std::vector<std::shared_ptr<DataFileMeta>>& to_compact)
        -> Result<std::vector<std::shared_ptr<DataFileMeta>>> { return to_compact; };

    auto small_file = NewNamedFile("small_file", 100, 1, 100);
    auto big_file1 = NewNamedFile("big_file1", 2000, 101, 2100);
    auto big_file2 = NewNamedFile("big_file2", 2000, 2101, 4100);

    BucketedAppendCompactManager manager(
        executor_, {small_file, big_file1, big_file2},
        /*dv_maintainer=*/nullptr,
        /*min_file_num=*/4,
        /*target_file_size=*/1024,
        /*compaction_file_size=*/500,
        /*force_rewrite_all_files=*/false, rewriter,
        /*reporter=*/nullptr,
        /*cancellation_controller=*/std::make_shared<CancellationController>());

    ASSERT_OK(manager.TriggerCompaction(/*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(auto result, manager.GetCompactionResult(/*blocking=*/true));
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value()->Before().empty());
    ASSERT_TRUE(result.value()->After().empty());
}

TEST_F(BucketedAppendCompactManagerTest, TestAllFilesWithoutCompacting) {
    // When no compaction is running, AllFiles returns only the to_compact_ files.
    auto rewriter = [](const std::vector<std::shared_ptr<DataFileMeta>>& to_compact)
        -> Result<std::vector<std::shared_ptr<DataFileMeta>>> { return to_compact; };

    auto file1 = NewNamedFile("file1", 100, 1, 100);
    auto file2 = NewNamedFile("file2", 200, 101, 300);

    BucketedAppendCompactManager manager(
        executor_, {file1, file2},
        /*dv_maintainer=*/nullptr,
        /*min_file_num=*/4,
        /*target_file_size=*/1024,
        /*compaction_file_size=*/500,
        /*force_rewrite_all_files=*/false, rewriter,
        /*reporter=*/nullptr,
        /*cancellation_controller=*/std::make_shared<CancellationController>());

    auto all_files = manager.AllFiles();
    // to_compact_ is a min-heap sorted by min_sequence_number, so file1 comes first.
    ASSERT_EQ(all_files.size(), 2);
    ASSERT_EQ(all_files[0]->file_name, "file1");
    ASSERT_EQ(all_files[1]->file_name, "file2");
}

TEST_F(BucketedAppendCompactManagerTest, TestAllFilesWithCompacting) {
    // When compaction is running, AllFiles returns compacting_ + to_compact_ files.
    auto exit_signal = std::make_shared<std::promise<void>>();
    auto proceed_signal = std::make_shared<std::promise<void>>();
    auto proceed_future = std::make_shared<std::shared_future<void>>(proceed_signal->get_future());

    auto rewriter = [exit_signal,
                     proceed_future](const std::vector<std::shared_ptr<DataFileMeta>>& to_compact)
        -> Result<std::vector<std::shared_ptr<DataFileMeta>>> {
        // Signal that rewriter has started
        exit_signal->set_value();
        // Wait for the test to check AllFiles before completing
        proceed_future->wait();
        return to_compact;
    };

    auto file1 = NewNamedFile("file1", 100, 1, 100);
    auto file2 = NewNamedFile("file2", 100, 101, 200);
    auto file3 = NewNamedFile("file3", 100, 201, 300);
    auto file4 = NewNamedFile("file4", 100, 301, 400);

    BucketedAppendCompactManager manager(
        executor_, {file1, file2, file3, file4},
        /*dv_maintainer=*/nullptr,
        /*min_file_num=*/4,
        /*target_file_size=*/1024,
        /*compaction_file_size=*/500,
        /*force_rewrite_all_files=*/false, rewriter,
        /*reporter=*/nullptr,
        /*cancellation_controller=*/std::make_shared<CancellationController>());

    // Trigger compaction — all 4 small files will be picked
    ASSERT_OK(manager.TriggerCompaction(/*full_compaction=*/true));

    // Wait until the rewriter is running (compaction in progress)
    auto started_future = exit_signal->get_future();
    ASSERT_EQ(started_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);

    // While compaction is in progress, AllFiles should return all files (compacting_ + to_compact_)
    auto all_files = manager.AllFiles();
    ASSERT_EQ(all_files.size(), 4);
    ASSERT_EQ(all_files[0]->file_name, "file1");
    ASSERT_EQ(all_files[1]->file_name, "file2");
    ASSERT_EQ(all_files[2]->file_name, "file3");
    ASSERT_EQ(all_files[3]->file_name, "file4");

    // Let compaction finish
    proceed_signal->set_value();

    // After getting the result, compacting_ is cleared
    ASSERT_OK_AND_ASSIGN(auto result, manager.GetCompactionResult(/*blocking=*/true));
    ASSERT_TRUE(result.has_value());

    // Now AllFiles should be empty (no to_compact_, compacting_ cleared)
    // But the last file might be put back if it's small enough
    auto all_files_after = manager.AllFiles();
    // file4 (size=100 < compaction_file_size=500) should be put back to to_compact_
    ASSERT_EQ(all_files_after.size(), 1);
    ASSERT_EQ(all_files_after[0]->file_name, "file4");
}

}  // namespace paimon::test
