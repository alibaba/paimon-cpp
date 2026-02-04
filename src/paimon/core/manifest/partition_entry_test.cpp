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

#include "paimon/core/manifest/partition_entry.h"

#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::test {

class PartitionEntryTest : public testing::Test {
    std::shared_ptr<DataFileMeta> GetDataFileMeta(const Timestamp& ts) const {
        return std::make_shared<DataFileMeta>(
            "file_name", /*file_size=*/10, /*row_count=*/5, DataFileMeta::EmptyMinKey(),
            DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            ts,
            /*delete_row_count=*/2,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }
};

TEST_F(PartitionEntryTest, TestSimple) {
    TimezoneGuard guard("Asia/Shanghai");
    {
        ASSERT_OK_AND_ASSIGN(auto partition_entry1, PartitionEntry::FromManifestEntry(ManifestEntry(
                                                        FileKind::Add(), BinaryRow::EmptyRow(),
                                                        /*bucket=*/0, /*total_buckets=*/2,
                                                        GetDataFileMeta(Timestamp(500, 123)))));

        ASSERT_OK_AND_ASSIGN(auto partition_entry2,
                             PartitionEntry::FromManifestEntry(ManifestEntry(
                                 FileKind::Add(), BinaryRow::EmptyRow(), /*bucket=*/1,
                                 /*total_buckets=*/2, GetDataFileMeta(Timestamp(1500, 123)))));

        auto res = partition_entry1.Merge(partition_entry2);
        auto expected_partition_entry =
            PartitionEntry(BinaryRow::EmptyRow(), /*record_count=*/10,
                           /*file_size_in_bytes=*/20, /*file_count=*/2,
                           /*last_file_creation_time=*/-28798500, /*total_buckets=*/2);
        ASSERT_EQ(res, expected_partition_entry);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto partition_entry1, PartitionEntry::FromManifestEntry(ManifestEntry(
                                                        FileKind::Add(), BinaryRow::EmptyRow(),
                                                        /*bucket=*/0, /*total_buckets=*/2,
                                                        GetDataFileMeta(Timestamp(500, 123)))));

        ASSERT_OK_AND_ASSIGN(auto partition_entry2,
                             PartitionEntry::FromManifestEntry(ManifestEntry(
                                 FileKind::Delete(), BinaryRow::EmptyRow(), /*bucket=*/1,
                                 /*total_buckets=*/2, GetDataFileMeta(Timestamp(1500, 123)))));

        auto res = partition_entry1.Merge(partition_entry2);
        auto expected_partition_entry =
            PartitionEntry(BinaryRow::EmptyRow(), /*record_count=*/0,
                           /*file_size_in_bytes=*/0, /*file_count=*/0,
                           /*last_file_creation_time=*/-28798500, /*total_buckets=*/2);
        ASSERT_EQ(res, expected_partition_entry);
    }
}

TEST_F(PartitionEntryTest, TestMerge) {
    TimezoneGuard guard("Asia/Shanghai");
    BinaryRow partition0 = BinaryRowGenerator::GenerateRow({0}, GetDefaultPool().get());
    BinaryRow partition1 = BinaryRowGenerator::GenerateRow({1}, GetDefaultPool().get());

    auto entry1 =
        ManifestEntry(FileKind::Add(), partition0,
                      /*bucket=*/0, /*total_buckets=*/2, GetDataFileMeta(Timestamp(500, 123)));

    auto entry2 = ManifestEntry(FileKind::Add(), partition0, /*bucket=*/1,
                                /*total_buckets=*/2, GetDataFileMeta(Timestamp(1500, 123)));

    auto entry3 = ManifestEntry(FileKind::Add(), partition1, /*bucket=*/0, /*total_buckets=*/2,
                                GetDataFileMeta(Timestamp(0, 123)));

    std::unordered_map<BinaryRow, PartitionEntry> to;
    ASSERT_OK(PartitionEntry::Merge({entry1, entry2, entry3}, &to));
    std::unordered_map<BinaryRow, PartitionEntry> expected_partition_entry;
    expected_partition_entry.emplace(
        std::piecewise_construct, std::forward_as_tuple(partition0),
        std::forward_as_tuple(PartitionEntry(partition0, 10, 20, 2, -28798500, 2)));
    expected_partition_entry.emplace(
        std::piecewise_construct, std::forward_as_tuple(partition1),
        std::forward_as_tuple(PartitionEntry(partition1, 5, 10, 1, -28800000, 2)));
    ASSERT_EQ(to, expected_partition_entry);
}

}  // namespace paimon::test
