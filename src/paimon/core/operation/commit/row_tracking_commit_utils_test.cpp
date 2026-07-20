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

#include "paimon/core/operation/commit/row_tracking_commit_utils.h"

#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class RowTrackingCommitUtilsTest : public testing::Test {
 protected:
    BinaryRow CreateIntRow(int32_t value) const {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
        writer.WriteInt(0, value);
        writer.Complete();
        return row;
    }

    ManifestEntry CreateEntry(const std::string& file_name, int64_t row_count,
                              int64_t min_seq_number, int64_t max_seq_number,
                              const std::optional<FileSource>& file_source,
                              const std::optional<std::vector<std::string>>& write_cols) const {
        auto file_meta = std::make_shared<DataFileMeta>(
            file_name, /*file_size=*/row_count, row_count, DataFileMeta::EmptyMinKey(),
            DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            min_seq_number, max_seq_number,
            /*schema_id=*/1, /*level=*/0,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, file_source,
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt, write_cols);
        return ManifestEntry(FileKind::Add(), CreateIntRow(1), /*bucket=*/0, /*total_buckets=*/1,
                             file_meta);
    }

    ManifestEntry CreateEntryWithFirstRowId(
        const std::string& file_name, int64_t row_count, int64_t min_seq_number,
        int64_t max_seq_number, const std::optional<FileSource>& file_source,
        const std::optional<std::vector<std::string>>& write_cols,
        const std::optional<int64_t>& first_row_id) const {
        auto file_meta = std::make_shared<DataFileMeta>(
            file_name, /*file_size=*/row_count, row_count, DataFileMeta::EmptyMinKey(),
            DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            min_seq_number, max_seq_number,
            /*schema_id=*/1, /*level=*/0,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, file_source,
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, first_row_id, write_cols);
        return ManifestEntry(FileKind::Add(), CreateIntRow(1), /*bucket=*/0, /*total_buckets=*/1,
                             file_meta);
    }
};

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingStampsSequence) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("new-file", /*row_count=*/10, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"f0"}));
    input.push_back(CreateEntry("partial-modified", /*row_count=*/8, /*min_seq_number=*/7,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"f0"}));
    input.push_back(CreateEntry("compact-file", /*row_count=*/6, /*min_seq_number=*/3,
                                /*max_seq_number=*/5, FileSource::Compact(),
                                std::vector<std::string>{"f0"}));

    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned assigned,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/100, /*first_row_id_start=*/0, input));

    ASSERT_EQ(3u, assigned.assigned_entries.size());
    EXPECT_EQ(100, assigned.assigned_entries[0].File()->min_sequence_number);
    EXPECT_EQ(100, assigned.assigned_entries[0].File()->max_sequence_number);
    EXPECT_EQ(7, assigned.assigned_entries[1].File()->min_sequence_number);
    EXPECT_EQ(100, assigned.assigned_entries[1].File()->max_sequence_number);
    EXPECT_EQ(3, assigned.assigned_entries[2].File()->min_sequence_number);
    EXPECT_EQ(5, assigned.assigned_entries[2].File()->max_sequence_number);
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingStampsSequenceRangeStartingAtZero) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("range-starts-at-zero", /*row_count=*/10,
                                /*min_seq_number=*/0, /*max_seq_number=*/9, FileSource::Append(),
                                std::vector<std::string>{"f0"}));

    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned assigned,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/100, /*first_row_id_start=*/0, input));

    ASSERT_EQ(1u, assigned.assigned_entries.size());
    EXPECT_EQ(100, assigned.assigned_entries[0].File()->min_sequence_number);
    EXPECT_EQ(100, assigned.assigned_entries[0].File()->max_sequence_number);
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTracking) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("normal-file", /*row_count=*/10, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"f0"}));
    input.push_back(CreateEntry("blob-a.blob", /*row_count=*/3, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"blob_a"}));
    input.push_back(CreateEntry("blob-a-2.blob", /*row_count=*/2, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"blob_a"}));
    input.push_back(CreateEntry("vector-1.vector.data", /*row_count=*/4,
                                /*min_seq_number=*/0, /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"vec"}));
    input.push_back(CreateEntry("normal-file-2", /*row_count=*/5, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"f0"}));

    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned assigned,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/200, /*first_row_id_start=*/0, input));

    ASSERT_EQ(5u, assigned.assigned_entries.size());
    EXPECT_EQ(0, assigned.assigned_entries[0].File()->first_row_id.value());
    EXPECT_EQ(0, assigned.assigned_entries[1].File()->first_row_id.value());
    EXPECT_EQ(3, assigned.assigned_entries[2].File()->first_row_id.value());
    EXPECT_EQ(0, assigned.assigned_entries[3].File()->first_row_id.value());
    EXPECT_EQ(10, assigned.assigned_entries[4].File()->first_row_id.value());
    EXPECT_EQ(15, assigned.next_row_id_start);

    for (const auto& entry : assigned.assigned_entries) {
        EXPECT_EQ(200, entry.File()->min_sequence_number);
        EXPECT_EQ(200, entry.File()->max_sequence_number);
    }
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingWithoutFileSource) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("invalid-no-source", /*row_count=*/1, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, std::nullopt,
                                std::vector<std::string>{"f0"}));

    ASSERT_NOK_WITH_MSG(RowTrackingCommitUtils::AssignRowTracking(
                            /*new_snapshot_id=*/1, /*first_row_id_start=*/0, input),
                        "file source field for row-tracking table must present");
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingDoesNotMutateInputEntries) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("normal-file", /*row_count=*/10, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"f0"}));
    input.push_back(CreateEntry("normal-file-2", /*row_count=*/5, /*min_seq_number=*/7,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"f0"}));

    std::shared_ptr<DataFileMeta> input_file_0 = input[0].File();
    std::shared_ptr<DataFileMeta> input_file_1 = input[1].File();

    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned assigned,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/200, /*first_row_id_start=*/1000, input));

    ASSERT_EQ(2u, assigned.assigned_entries.size());

    EXPECT_EQ(0, input[0].File()->min_sequence_number);
    EXPECT_EQ(0, input[0].File()->max_sequence_number);
    EXPECT_EQ(std::nullopt, input[0].File()->first_row_id);

    EXPECT_EQ(7, input[1].File()->min_sequence_number);
    EXPECT_EQ(0, input[1].File()->max_sequence_number);
    EXPECT_EQ(std::nullopt, input[1].File()->first_row_id);

    EXPECT_NE(input_file_0.get(), assigned.assigned_entries[0].File().get());
    EXPECT_NE(input_file_1.get(), assigned.assigned_entries[1].File().get());
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingReassignsOnRetryWithAdvancedRowId) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("retry-file", /*row_count=*/10, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"f0"}));

    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned first_attempt,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/100, /*first_row_id_start=*/1000, input));
    ASSERT_EQ(1u, first_attempt.assigned_entries.size());
    EXPECT_EQ(100, first_attempt.assigned_entries[0].File()->min_sequence_number);
    EXPECT_EQ(100, first_attempt.assigned_entries[0].File()->max_sequence_number);
    ASSERT_TRUE(first_attempt.assigned_entries[0].File()->first_row_id.has_value());
    EXPECT_EQ(1000, first_attempt.assigned_entries[0].File()->first_row_id.value());
    EXPECT_EQ(1010, first_attempt.next_row_id_start);

    // Simulate CAS retry with a newer latest snapshot and advanced next_row_id.
    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned second_attempt,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/101, /*first_row_id_start=*/2000, input));
    ASSERT_EQ(1u, second_attempt.assigned_entries.size());
    EXPECT_EQ(101, second_attempt.assigned_entries[0].File()->min_sequence_number);
    EXPECT_EQ(101, second_attempt.assigned_entries[0].File()->max_sequence_number);
    ASSERT_TRUE(second_attempt.assigned_entries[0].File()->first_row_id.has_value());
    EXPECT_EQ(2000, second_attempt.assigned_entries[0].File()->first_row_id.value());
    EXPECT_EQ(2010, second_attempt.next_row_id_start);

    // Input remains immutable across attempts; retry assignment always starts from fresh metadata.
    EXPECT_EQ(0, input[0].File()->min_sequence_number);
    EXPECT_EQ(0, input[0].File()->max_sequence_number);
    EXPECT_EQ(std::nullopt, input[0].File()->first_row_id);
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingEmptyInput) {
    std::vector<ManifestEntry> input;
    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned assigned,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/100, /*first_row_id_start=*/42, input));
    EXPECT_TRUE(assigned.assigned_entries.empty());
    EXPECT_EQ(42, assigned.next_row_id_start);
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingSkipsFilesWithRowIdColumn) {
    std::vector<ManifestEntry> input;
    input.push_back(
        CreateEntry("has-row-id-column", /*row_count=*/10, /*min_seq_number=*/0,
                    /*max_seq_number=*/0, FileSource::Append(),
                    std::vector<std::string>{std::string(SpecialFields::RowId().Name())}));

    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned assigned,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/100, /*first_row_id_start=*/0, input));

    ASSERT_EQ(1u, assigned.assigned_entries.size());
    // Sequence numbers are still stamped for a new file.
    EXPECT_EQ(100, assigned.assigned_entries[0].File()->min_sequence_number);
    EXPECT_EQ(100, assigned.assigned_entries[0].File()->max_sequence_number);
    // But a file already carrying the row-id column must not be assigned a first row id.
    EXPECT_EQ(std::nullopt, assigned.assigned_entries[0].File()->first_row_id);
    EXPECT_EQ(0, assigned.next_row_id_start);
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingKeepsExistingFirstRowId) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntryWithFirstRowId("already-assigned", /*row_count=*/10,
                                              /*min_seq_number=*/0, /*max_seq_number=*/0,
                                              FileSource::Append(), std::vector<std::string>{"f0"},
                                              /*first_row_id=*/500));

    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned assigned,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/100, /*first_row_id_start=*/0, input));

    ASSERT_EQ(1u, assigned.assigned_entries.size());
    // The existing first row id is preserved and start is not advanced.
    ASSERT_TRUE(assigned.assigned_entries[0].File()->first_row_id.has_value());
    EXPECT_EQ(500, assigned.assigned_entries[0].File()->first_row_id.value());
    EXPECT_EQ(0, assigned.next_row_id_start);
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingCompactFileKeepsNoFirstRowId) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("compact-file", /*row_count=*/6, /*min_seq_number=*/3,
                                /*max_seq_number=*/5, FileSource::Compact(),
                                std::vector<std::string>{"f0"}));

    ASSERT_OK_AND_ASSIGN(RowTrackingCommitUtils::RowTrackingAssigned assigned,
                         RowTrackingCommitUtils::AssignRowTracking(
                             /*new_snapshot_id=*/100, /*first_row_id_start=*/7, input));

    ASSERT_EQ(1u, assigned.assigned_entries.size());
    // Pure compact file keeps its original sequence numbers and gets no first row id.
    EXPECT_EQ(3, assigned.assigned_entries[0].File()->min_sequence_number);
    EXPECT_EQ(5, assigned.assigned_entries[0].File()->max_sequence_number);
    EXPECT_EQ(std::nullopt, assigned.assigned_entries[0].File()->first_row_id);
    EXPECT_EQ(7, assigned.next_row_id_start);
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingBlobBeforeNormalFileFails) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("blob-a.blob", /*row_count=*/3, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"blob_a"}));

    ASSERT_NOK_WITH_MSG(RowTrackingCommitUtils::AssignRowTracking(
                            /*new_snapshot_id=*/100, /*first_row_id_start=*/0, input),
                        "blobStart");
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingVectorStoreBeforeNormalFileFails) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("vector-1.vector.data", /*row_count=*/4, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"vec"}));

    ASSERT_NOK_WITH_MSG(RowTrackingCommitUtils::AssignRowTracking(
                            /*new_snapshot_id=*/100, /*first_row_id_start=*/0, input),
                        "vectorStoreStart");
}

TEST_F(RowTrackingCommitUtilsTest, TestAssignRowTrackingBlobWithoutWriteColsFails) {
    std::vector<ManifestEntry> input;
    input.push_back(CreateEntry("normal-file", /*row_count=*/10, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{"f0"}));
    input.push_back(CreateEntry("blob-a.blob", /*row_count=*/3, /*min_seq_number=*/0,
                                /*max_seq_number=*/0, FileSource::Append(),
                                std::vector<std::string>{}));

    ASSERT_NOK_WITH_MSG(RowTrackingCommitUtils::AssignRowTracking(
                            /*new_snapshot_id=*/100, /*first_row_id_start=*/0, input),
                        "does not have write_cols");
}

}  // namespace paimon::test
