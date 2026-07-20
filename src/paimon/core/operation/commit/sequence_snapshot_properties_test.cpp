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

#include "paimon/core/operation/commit/sequence_snapshot_properties.h"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class SequenceSnapshotPropertiesTest : public testing::Test {
 protected:
    Snapshot MakeSnapshot(
        const std::optional<std::map<std::string, std::string>>& properties) const {
        return Snapshot(
            /*id=*/1,
            /*schema_id=*/1,
            /*base_manifest_list=*/"base-manifest-list",
            /*base_manifest_list_size=*/std::nullopt,
            /*delta_manifest_list=*/"delta-manifest-list",
            /*delta_manifest_list_size=*/std::nullopt,
            /*changelog_manifest_list=*/std::nullopt,
            /*changelog_manifest_list_size=*/std::nullopt,
            /*index_manifest=*/std::nullopt,
            /*commit_user=*/"test-user",
            /*commit_identifier=*/1, Snapshot::CommitKind::Append(),
            /*time_millis=*/0,
            /*total_record_count=*/0,
            /*delta_record_count=*/0,
            /*changelog_record_count=*/std::nullopt,
            /*watermark=*/std::nullopt,
            /*statistics=*/std::nullopt, properties,
            /*next_row_id=*/std::nullopt);
    }

    std::shared_ptr<DataFileMeta> CreateDataFileMeta(int64_t max_sequence_number) const {
        return std::make_shared<DataFileMeta>(
            "data-file", 1024, 8, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_seq_no=*/0,
            /*max_seq_no=*/max_sequence_number,
            /*schema_id=*/1, /*level=*/0,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    ManifestEntry CreateEntry(const FileKind& kind, int64_t max_sequence_number) const {
        return ManifestEntry(kind, BinaryRow(0), /*bucket=*/0, /*total_buckets=*/1,
                             CreateDataFileMeta(max_sequence_number));
    }
};

TEST_F(SequenceSnapshotPropertiesTest, MaxSequenceNumberEmptySnapshot) {
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> result,
                         SequenceSnapshotProperties::MaxSequenceNumber(std::nullopt));
    ASSERT_FALSE(result.has_value());
}

TEST_F(SequenceSnapshotPropertiesTest, MaxSequenceNumberSnapshotWithoutProperties) {
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> result,
                         SequenceSnapshotProperties::MaxSequenceNumber(MakeSnapshot(std::nullopt)));
    ASSERT_FALSE(result.has_value());
}

TEST_F(SequenceSnapshotPropertiesTest, MaxSequenceNumberKeyMissing) {
    std::map<std::string, std::string> properties{{"other-key", "42"}};
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> result,
                         SequenceSnapshotProperties::MaxSequenceNumber(MakeSnapshot(properties)));
    ASSERT_FALSE(result.has_value());
}

TEST_F(SequenceSnapshotPropertiesTest, MaxSequenceNumberValid) {
    std::map<std::string, std::string> properties{
        {SequenceSnapshotProperties::kMaxSequenceNumberKey, "123"}};
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> result,
                         SequenceSnapshotProperties::MaxSequenceNumber(MakeSnapshot(properties)));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(123, result.value());
}

TEST_F(SequenceSnapshotPropertiesTest, MaxSequenceNumberTrailingCharacters) {
    std::map<std::string, std::string> properties{
        {SequenceSnapshotProperties::kMaxSequenceNumberKey, "123abc"}};
    ASSERT_NOK_WITH_MSG(SequenceSnapshotProperties::MaxSequenceNumber(MakeSnapshot(properties)),
                        "trailing characters are not allowed");
}

TEST_F(SequenceSnapshotPropertiesTest, MaxSequenceNumberNotANumber) {
    std::map<std::string, std::string> properties{
        {SequenceSnapshotProperties::kMaxSequenceNumberKey, "not-a-number"}};
    ASSERT_NOK_WITH_MSG(SequenceSnapshotProperties::MaxSequenceNumber(MakeSnapshot(properties)),
                        "Invalid");
}

TEST_F(SequenceSnapshotPropertiesTest, MaxSequenceNumberFromFilesEmpty) {
    EXPECT_FALSE(SequenceSnapshotProperties::MaxSequenceNumberFromFiles({}).has_value());
}

TEST_F(SequenceSnapshotPropertiesTest, MaxSequenceNumberFromFilesOnlyDelete) {
    std::vector<ManifestEntry> files{CreateEntry(FileKind::Delete(), 100)};
    EXPECT_FALSE(SequenceSnapshotProperties::MaxSequenceNumberFromFiles(files).has_value());
}

TEST_F(SequenceSnapshotPropertiesTest, MaxSequenceNumberFromFilesSkipsDelete) {
    std::vector<ManifestEntry> files{CreateEntry(FileKind::Add(), 10),
                                     CreateEntry(FileKind::Delete(), 999),
                                     CreateEntry(FileKind::Add(), 42)};
    std::optional<int64_t> result = SequenceSnapshotProperties::MaxSequenceNumberFromFiles(files);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(42, result.value());
}

TEST_F(SequenceSnapshotPropertiesTest, MergeMaxSequenceNumberNoInput) {
    std::map<std::string, std::string> properties{{"existing", "value"}};
    std::map<std::string, std::string> merged = SequenceSnapshotProperties::MergeMaxSequenceNumber(
        properties, /*latest_max_sequence_number=*/std::nullopt, /*delta_files=*/{});
    EXPECT_EQ(properties, merged);
    EXPECT_EQ(0u, merged.count(SequenceSnapshotProperties::kMaxSequenceNumberKey));
}

TEST_F(SequenceSnapshotPropertiesTest, MergeMaxSequenceNumberLatestOnly) {
    std::map<std::string, std::string> merged = SequenceSnapshotProperties::MergeMaxSequenceNumber(
        /*properties=*/{}, /*latest_max_sequence_number=*/50, /*delta_files=*/{});
    EXPECT_EQ("50", merged.at(SequenceSnapshotProperties::kMaxSequenceNumberKey));
}

TEST_F(SequenceSnapshotPropertiesTest, MergeMaxSequenceNumberDeltaOnly) {
    std::vector<ManifestEntry> delta_files{CreateEntry(FileKind::Add(), 77)};
    std::map<std::string, std::string> merged = SequenceSnapshotProperties::MergeMaxSequenceNumber(
        /*properties=*/{}, /*latest_max_sequence_number=*/std::nullopt, delta_files);
    EXPECT_EQ("77", merged.at(SequenceSnapshotProperties::kMaxSequenceNumberKey));
}

TEST_F(SequenceSnapshotPropertiesTest, MergeMaxSequenceNumberTakesMaximum) {
    std::vector<ManifestEntry> delta_files{CreateEntry(FileKind::Add(), 30)};
    std::map<std::string, std::string> merged = SequenceSnapshotProperties::MergeMaxSequenceNumber(
        /*properties=*/{}, /*latest_max_sequence_number=*/90, delta_files);
    EXPECT_EQ("90", merged.at(SequenceSnapshotProperties::kMaxSequenceNumberKey));

    std::vector<ManifestEntry> larger_delta{CreateEntry(FileKind::Add(), 150)};
    std::map<std::string, std::string> merged2 = SequenceSnapshotProperties::MergeMaxSequenceNumber(
        /*properties=*/{}, /*latest_max_sequence_number=*/90, larger_delta);
    EXPECT_EQ("150", merged2.at(SequenceSnapshotProperties::kMaxSequenceNumberKey));
}

}  // namespace paimon::test
