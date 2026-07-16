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

#include "paimon/core/manifest/manifest_entry_serializer.h"

#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {

class ManifestEntrySerializerTest : public testing::Test {
 private:
    std::shared_ptr<DataFileMeta> GetDataFileMeta(int64_t row_count) {
        return std::make_shared<DataFileMeta>(
            "some_file_name", 1024, row_count, DataFileMeta::EmptyMinKey(),
            DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/2, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0), /*delete_row_count=*/3,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::optional<std::string>(),
            /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    }
};
TEST_F(ManifestEntrySerializerTest, TestToFromRow) {
    auto pool = GetDefaultPool();
    std::vector<ManifestEntry> entries = {
        ManifestEntry(FileKind::Add(), BinaryRow::EmptyRow(), 0, 2, GetDataFileMeta(8)),
        ManifestEntry(FileKind::Add(), BinaryRowGenerator::GenerateRow({10}, pool.get()), 1, 2,
                      GetDataFileMeta(8))};
    ManifestEntrySerializer serializer(pool);
    for (const auto& entry : entries) {
        ASSERT_OK_AND_ASSIGN(auto row, serializer.ToRow(entry));
        ASSERT_OK_AND_ASSIGN(auto result_entry, serializer.FromRow(row));
        ASSERT_EQ(entry, result_entry);
        ASSERT_EQ(entry.ToString(), result_entry.ToString());
    }
}

TEST_F(ManifestEntrySerializerTest, TestNullableRecordCount) {
    std::vector<ManifestEntry> empty_entries;
    ASSERT_FALSE(ManifestEntry::NullableRecordCount(empty_entries).has_value());

    std::vector<ManifestEntry> entries = {
        ManifestEntry(FileKind::Add(), BinaryRow::EmptyRow(), 0, 2, GetDataFileMeta(3)),
        ManifestEntry(FileKind::Delete(), BinaryRow::EmptyRow(), 0, 2, GetDataFileMeta(8))};
    ASSERT_EQ(11, ManifestEntry::RecordCount(entries));

    std::optional<int64_t> record_count = ManifestEntry::NullableRecordCount(entries);
    ASSERT_TRUE(record_count.has_value());
    ASSERT_EQ(11, record_count.value());
}
}  // namespace paimon::test
