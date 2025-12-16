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

#include "paimon/core/operation/data_evolution_split_read.h"

#include <cstdint>
#include <map>
#include <optional>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class DataEvolutionSplitReadTest : public ::testing::Test {
 public:
    std::shared_ptr<DataFileMeta> CreateDataFileMeta(
        const std::string& file_name, const std::optional<int64_t>& first_row_id,
        const std::optional<FileSource>& file_source) const {
        return DataFileMeta::ForAppend(file_name, /*file_size=*/100, /*row_count=*/100,
                                       /*row_stats=*/SimpleStats::EmptyStats(),
                                       /*min_sequence_number=*/0,
                                       /*max_sequence_number=*/100, /*schema_id=*/0, file_source,
                                       /*value_stats_cols=*/std::nullopt,
                                       /*external_path=*/std::nullopt, first_row_id,
                                       /*write_cols=*/std::nullopt)
            .value();
    }

    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name,
                                                     const std::optional<int64_t>& first_row_id,
                                                     int64_t row_count, int64_t max_seq) const {
        return DataFileMeta::ForAppend(
                   file_name, /*file_size=*/100, row_count,
                   /*row_stats=*/SimpleStats::EmptyStats(),
                   /*min_sequence_number=*/0,
                   /*max_sequence_number=*/max_seq, /*schema_id=*/0, FileSource::Append(),
                   /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt, first_row_id,
                   /*write_cols=*/std::nullopt)
            .value();
    }

    std::shared_ptr<DataFileMeta> CreateNormalFile(const std::string& file_name,
                                                   int64_t first_row_id, int64_t row_count,
                                                   int64_t max_sequence_number) const {
        return DataFileMeta::ForAppend(file_name, /*file_size=*/row_count,
                                       /*row_count=*/row_count,
                                       /*row_stats=*/SimpleStats::EmptyStats(),
                                       /*min_sequence_number=*/0, max_sequence_number,
                                       /*schema_id=*/0, FileSource::Append(),
                                       /*value_stats_cols=*/std::nullopt,
                                       /*external_path=*/std::nullopt, first_row_id,
                                       /*write_cols=*/std::nullopt)
            .value();
    }

    std::shared_ptr<DataFileMeta> CreateBlobFile(
        const std::string& file_name, int64_t first_row_id, int64_t row_count,
        int64_t max_sequence_number,
        const std::optional<std::vector<std::string>>& write_cols) const {
        return DataFileMeta::ForAppend(file_name + ".blob", /*file_size=*/row_count,
                                       /*row_count=*/row_count,
                                       /*row_stats=*/SimpleStats::EmptyStats(),
                                       /*min_sequence_number=*/0, max_sequence_number,
                                       /*schema_id=*/0, FileSource::Append(),
                                       /*value_stats_cols=*/std::nullopt,
                                       /*external_path=*/std::nullopt, first_row_id, write_cols)
            .value();
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(DataEvolutionSplitReadTest, TestAddSingleBlobEntry) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));

    ASSERT_EQ(blob_bunch->Files().size(), 1);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
    ASSERT_EQ(blob_bunch->RowCount(), 100);
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobEntryAndTail) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/100, /*row_count=*/200,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    ASSERT_OK(blob_bunch->Add(blob_tail));

    ASSERT_EQ(blob_bunch->Files().size(), 2);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
    ASSERT_EQ(blob_bunch->Files()[1], blob_tail);
    ASSERT_EQ(blob_bunch->RowCount(), 300);
}

TEST_F(DataEvolutionSplitReadTest, TestAddNonBlobFileInvalid) {
    auto blob_entry =
        CreateDataFileMeta("normal1.parquet", /*first_row_id=*/0, FileSource::Append());
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_NOK_WITH_MSG(blob_bunch->Add(blob_entry),
                        "Only blob file can be added to a blob bunch.");
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobWithSameFirstRowId) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/50,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    ASSERT_NOK_WITH_MSG(blob_bunch->Add(blob_tail),
                        "Blob file with same first row id should have decreasing sequence number.");
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithSameFirstRowIdAndLowerSequenceNumber) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/50,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // Adding file with same firstRowId and lower sequence number should be ignored
    ASSERT_OK(blob_bunch->Add(blob_tail));

    ASSERT_EQ(blob_bunch->Files().size(), 1);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithOverlappingRowId) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/50, /*row_count=*/150,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // Adding file with overlapping row id and lower sequence number should be ignored
    ASSERT_OK(blob_bunch->Add(blob_tail));

    ASSERT_EQ(blob_bunch->Files().size(), 1);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithOverlappingRowIdAndHigherSequenceNumber) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/50, /*row_count=*/150,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    ASSERT_NOK_WITH_MSG(
        blob_bunch->Add(blob_tail),
        "Blob file with overlapping row id should have decreasing sequence number.");
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithNonContinuousRowId) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/200, /*row_count=*/300,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // Adding file with non-continuous row id should return bad status
    ASSERT_NOK_WITH_MSG(blob_bunch->Add(blob_tail),
                        "Blob file first row id should be continuous, expect 100 but got 200");
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithDifferentWriteCols) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/100, /*row_count=*/200,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"diff_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // Adding file with different write columns should return bad status
    ASSERT_NOK_WITH_MSG(blob_bunch->Add(blob_tail),
                        "All files in a blob bunch should have the same write columns.");
}

TEST_F(DataEvolutionSplitReadTest, TestRowIdSelectionWithOverlap) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/10,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_sub1 =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/5,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_sub2 =
        CreateBlobFile("blob3", /*first_row_id=*/5, /*row_count=*/5,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/true);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // blob_sub1 will not be added, for it has been skipped by row_ids in scan process.
    // after blob_sub2 is added, blob_entry is removed
    ASSERT_OK(blob_bunch->Add(blob_sub2));
    ASSERT_EQ(blob_bunch->Files().size(), 1);
    ASSERT_EQ(blob_bunch->Files()[0], blob_sub2);
    ASSERT_EQ(blob_bunch->RowCount(), 5);
}

TEST_F(DataEvolutionSplitReadTest, TestRowIdSelectionWithOverlap2) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/10,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_sub1 =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/5,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_sub2 =
        CreateBlobFile("blob3", /*first_row_id=*/5, /*row_count=*/5,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/true);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // blob_sub1 will not be added, for it has been skipped by row_ids in scan process.
    // after blob_sub2 is added, as blob_sub2 has smaller sequence number, blob_sub2 will be
    // skipped.
    ASSERT_OK(blob_bunch->Add(blob_sub2));
    ASSERT_EQ(blob_bunch->Files().size(), 1);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
    ASSERT_EQ(blob_bunch->RowCount(), 10);
}

TEST_F(DataEvolutionSplitReadTest, TestRowIdSelection) {
    auto blob_entry0 =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/10,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_entry1 =
        CreateBlobFile("blob2", /*first_row_id=*/10, /*row_count=*/10,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry2 =
        CreateBlobFile("blob3", /*first_row_id=*/20, /*row_count=*/10,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/true);
    ASSERT_OK(blob_bunch->Add(blob_entry0));
    // blob_entry1 will not be added, for it has been skipped by row_ids in scan process.
    ASSERT_OK(blob_bunch->Add(blob_entry2));
    ASSERT_EQ(blob_bunch->Files().size(), 2);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry0);
    ASSERT_EQ(blob_bunch->Files()[1], blob_entry2);
    ASSERT_EQ(blob_bunch->RowCount(), 20);
}

TEST_F(DataEvolutionSplitReadTest, TestComplexBlobBunchScenario) {
    auto blob_entry1 =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry2 =
        CreateBlobFile("blob2", /*first_row_id=*/100, /*row_count=*/200,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry3 =
        CreateBlobFile("blob3", /*first_row_id=*/300, /*row_count=*/300,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry4 =
        CreateBlobFile("blob4", /*first_row_id=*/600, /*row_count=*/400,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        INT64_MAX, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry1));
    ASSERT_OK(blob_bunch->Add(blob_entry2));
    ASSERT_OK(blob_bunch->Add(blob_entry3));
    ASSERT_OK(blob_bunch->Add(blob_entry4));

    ASSERT_EQ(blob_bunch->Files().size(), 4);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry1);
    ASSERT_EQ(blob_bunch->Files()[1], blob_entry2);
    ASSERT_EQ(blob_bunch->Files()[2], blob_entry3);
    ASSERT_EQ(blob_bunch->Files()[3], blob_entry4);
    ASSERT_EQ(blob_bunch->RowCount(), 1000);
}

TEST_F(DataEvolutionSplitReadTest, TestComplexBlobBunchScenario2) {
    std::vector<std::shared_ptr<DataFileMeta>> waited;
    auto data = CreateNormalFile("others.parquet", /*first_row_id=*/0, /*row_count=*/1000,
                                 /*max_sequence_number=*/1);
    auto blob_entry1 =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/1000,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_entry2 =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/500,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry3 =
        CreateBlobFile("blob3", /*first_row_id=*/500, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry4 =
        CreateBlobFile("blob4", /*first_row_id=*/750, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_entry5 =
        CreateBlobFile("blob5", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry6 =
        CreateBlobFile("blob6", /*first_row_id=*/100, /*row_count=*/400,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry7 =
        CreateBlobFile("blob7", /*first_row_id=*/750, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry8 =
        CreateBlobFile("blob8", /*first_row_id=*/850, /*row_count=*/150,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_entry9 =
        CreateBlobFile("blob9", /*first_row_id=*/100, /*row_count=*/650,
                       /*max_sequence_number=*/4,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    waited.push_back(data);
    waited.push_back(blob_entry1);
    waited.push_back(blob_entry2);
    waited.push_back(blob_entry3);
    waited.push_back(blob_entry4);
    waited.push_back(blob_entry5);
    waited.push_back(blob_entry6);
    waited.push_back(blob_entry7);
    waited.push_back(blob_entry8);
    waited.push_back(blob_entry9);

    auto input_metas = waited;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> batches,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(input_metas)));
    ASSERT_EQ(batches.size(), 1);

    std::vector<std::shared_ptr<DataFileMeta>> batch = batches[0];
    ASSERT_EQ(batch.size(), 10);
    ASSERT_EQ(batch[0], data);
    ASSERT_EQ(batch[1], blob_entry5);  // pick
    ASSERT_EQ(batch[2], blob_entry2);  // skip
    ASSERT_EQ(batch[3], blob_entry1);  // skip
    ASSERT_EQ(batch[4], blob_entry9);  // pick
    ASSERT_EQ(batch[5], blob_entry6);  // skip
    ASSERT_EQ(batch[6], blob_entry3);  // skip
    ASSERT_EQ(batch[7], blob_entry7);  // pick
    ASSERT_EQ(batch[8], blob_entry4);  // skip
    ASSERT_EQ(batch[9], blob_entry8);  // pick

    auto blob_field_to_field_id = [](const std::shared_ptr<DataFileMeta>&) -> Result<int32_t> {
        return 0;
    };
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> bunch,
                         DataEvolutionSplitRead::SplitFieldBunches(
                             batch, blob_field_to_field_id, /*has_row_ids_selection=*/false));

    ASSERT_EQ(bunch.size(), 2);
    auto blob_bunch = std::dynamic_pointer_cast<DataEvolutionSplitRead::BlobBunch>(bunch[1]);

    ASSERT_EQ(blob_bunch->Files().size(), 4);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry5);
    ASSERT_EQ(blob_bunch->Files()[1], blob_entry9);
    ASSERT_EQ(blob_bunch->Files()[2], blob_entry7);
    ASSERT_EQ(blob_bunch->Files()[3], blob_entry8);
    ASSERT_EQ(blob_bunch->RowCount(), 1000);
}

TEST_F(DataEvolutionSplitReadTest, TestComplexBlobBunchScenario3) {
    std::vector<std::shared_ptr<DataFileMeta>> waited;
    auto data = CreateNormalFile("others.parquet", /*first_row_id=*/0, /*row_count=*/1000,
                                 /*max_sequence_number=*/1);
    auto blob_entry1 =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/1000,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry2 =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/500,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry3 =
        CreateBlobFile("blob3", /*first_row_id=*/500, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry4 =
        CreateBlobFile("blob4", /*first_row_id=*/750, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry5 =
        CreateBlobFile("blob5", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry6 =
        CreateBlobFile("blob6", /*first_row_id=*/100, /*row_count=*/400,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry7 =
        CreateBlobFile("blob7", /*first_row_id=*/750, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry8 =
        CreateBlobFile("blob8", /*first_row_id=*/850, /*row_count=*/150,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry9 =
        CreateBlobFile("blob9", /*first_row_id=*/100, /*row_count=*/650,
                       /*max_sequence_number=*/4,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));

    auto blob_entry11 =
        CreateBlobFile("blob11", /*first_row_id=*/0, /*row_count=*/1000,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry12 =
        CreateBlobFile("blob12", /*first_row_id=*/0, /*row_count=*/500,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry13 =
        CreateBlobFile("blob13", /*first_row_id=*/500, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry14 =
        CreateBlobFile("blob14", /*first_row_id=*/750, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry15 =
        CreateBlobFile("blob15", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry16 =
        CreateBlobFile("blob16", /*first_row_id=*/100, /*row_count=*/400,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry17 =
        CreateBlobFile("blob17", /*first_row_id=*/750, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry18 =
        CreateBlobFile("blob18", /*first_row_id=*/850, /*row_count=*/150,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry19 =
        CreateBlobFile("blob19", /*first_row_id=*/100, /*row_count=*/650,
                       /*max_sequence_number=*/4,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    waited.push_back(data);
    waited.push_back(blob_entry1);
    waited.push_back(blob_entry2);
    waited.push_back(blob_entry3);
    waited.push_back(blob_entry4);
    waited.push_back(blob_entry5);
    waited.push_back(blob_entry6);
    waited.push_back(blob_entry7);
    waited.push_back(blob_entry8);
    waited.push_back(blob_entry9);

    waited.push_back(blob_entry11);
    waited.push_back(blob_entry12);
    waited.push_back(blob_entry13);
    waited.push_back(blob_entry14);
    waited.push_back(blob_entry15);
    waited.push_back(blob_entry16);
    waited.push_back(blob_entry17);
    waited.push_back(blob_entry18);
    waited.push_back(blob_entry19);

    auto input_metas = waited;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> batches,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(input_metas)));
    ASSERT_EQ(batches.size(), 1);

    std::vector<std::shared_ptr<DataFileMeta>> batch = batches[0];
    auto blob_field_to_field_id = [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int32_t> {
        return std::stoi(meta->write_cols.value()[0]);
    };
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> bunch,
                         DataEvolutionSplitRead::SplitFieldBunches(
                             batch, blob_field_to_field_id, /*has_row_ids_selection=*/false));

    ASSERT_EQ(bunch.size(), 3);
    auto blob_bunch = std::dynamic_pointer_cast<DataEvolutionSplitRead::BlobBunch>(bunch[1]);
    ASSERT_EQ(blob_bunch->Files().size(), 4);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry5);
    ASSERT_EQ(blob_bunch->Files()[1], blob_entry9);
    ASSERT_EQ(blob_bunch->Files()[2], blob_entry7);
    ASSERT_EQ(blob_bunch->Files()[3], blob_entry8);
    ASSERT_EQ(blob_bunch->RowCount(), 1000);

    auto blob_bunch2 = std::dynamic_pointer_cast<DataEvolutionSplitRead::BlobBunch>(bunch[2]);
    ASSERT_EQ(blob_bunch2->Files().size(), 4);
    ASSERT_EQ(blob_bunch2->Files()[0], blob_entry15);
    ASSERT_EQ(blob_bunch2->Files()[1], blob_entry19);
    ASSERT_EQ(blob_bunch2->Files()[2], blob_entry17);
    ASSERT_EQ(blob_bunch2->Files()[3], blob_entry18);
    ASSERT_EQ(blob_bunch2->RowCount(), 1000);
}

TEST_F(DataEvolutionSplitReadTest, TestDifferentRowIdRange) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateNormalFile("file0.parquet", 1l, 100, 10),
        CreateNormalFile("file1.parquet", 1l, 50, 20)};
    ASSERT_NOK_WITH_MSG(DataEvolutionSplitRead::MergeRangesAndSort(std::move(files)),
                        "Data files should be all row id ranges same.");
}

TEST_F(DataEvolutionSplitReadTest, TestSplitWithSameFirstRowId) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {CreateDataFileMeta("file0", 1l, 1, 10),
                                                        CreateDataFileMeta("file1", 1l, 1, 20),
                                                        CreateDataFileMeta("file2", 1l, 1, 30)};
    auto tmp_files = files;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_metas,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(tmp_files)));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected_metas = {
        {files[2], files[1], files[0]}};
    ASSERT_EQ(expected_metas, split_metas);
}

TEST_F(DataEvolutionSplitReadTest, TestSplitWithMixedFirstRowId) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateDataFileMeta("file0", 1l, 1, 1), CreateDataFileMeta("file1", 2l, 1, 2),
        CreateDataFileMeta("file2", 1l, 1, 3), CreateDataFileMeta("file3", 2l, 1, 4),
        CreateDataFileMeta("file4", 3l, 1, 5),
    };
    auto tmp_files = files;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_metas,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(tmp_files)));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected_metas = {
        {files[2], files[0]}, {files[3], files[1]}, {files[4]}};
    ASSERT_EQ(expected_metas, split_metas);
}

TEST_F(DataEvolutionSplitReadTest, TestSplitWithComplexScenario) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateDataFileMeta("file0", 1l, 1, 1), CreateDataFileMeta("file1", 2l, 1, 3),
        CreateDataFileMeta("file2", 3l, 1, 5), CreateDataFileMeta("file3", 1l, 1, 2),
        CreateDataFileMeta("file4", 4l, 1, 8), CreateDataFileMeta("file5", 2l, 1, 4),
        CreateDataFileMeta("file6", 3l, 1, 6), CreateDataFileMeta("file7", 3l, 1, 7),
        CreateDataFileMeta("file8", 5l, 1, 9),
    };
    auto tmp_files = files;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_metas,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(tmp_files)));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected_metas = {
        {files[3], files[0]},
        {files[5], files[1]},
        {files[7], files[6], files[2]},
        {files[4]},
        {files[8]}};
    ASSERT_EQ(expected_metas, split_metas);
}

TEST_F(DataEvolutionSplitReadTest, TestSplitWithMultipleBlobFilesPerGroup) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateDataFileMeta("file0.parquet", 1l, 10, 1),
        CreateDataFileMeta("file1.blob", 1l, 1, 1),
        CreateDataFileMeta("file2.blob", 2l, 9, 1),
        CreateDataFileMeta("file3.parquet", 20l, 10, 2),
        CreateDataFileMeta("file4.blob", 20l, 5, 2),
        CreateDataFileMeta("file5.blob", 25l, 5, 2),
        CreateDataFileMeta("file6.parquet", 1l, 10, 3)};
    auto tmp_files = files;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_metas,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(tmp_files)));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected_metas = {
        {files[6], files[0], files[1], files[2]}, {files[3], files[4], files[5]}};
    ASSERT_EQ(expected_metas, split_metas);
}

}  // namespace paimon::test
