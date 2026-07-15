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

#include "paimon/core/operation/commit/conflict_detection.h"

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/memory/bytes.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

Snapshot MakeSnapshot(const Snapshot::CommitKind& commit_kind) {
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
        /*commit_identifier=*/1, commit_kind,
        /*time_millis=*/0,
        /*total_record_count=*/0,
        /*delta_record_count=*/0,
        /*changelog_record_count=*/std::nullopt,
        /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt,
        /*properties=*/std::nullopt,
        /*next_row_id=*/std::nullopt);
}

Status CheckConflicts(const ConflictDetection& detection,
                      const std::vector<ManifestEntry>& base_entries,
                      const std::vector<ManifestEntry>& delta_entries,
                      const Snapshot::CommitKind& commit_kind) {
    return detection.CheckConflicts(MakeSnapshot(commit_kind), base_entries, delta_entries,
                                    /*delta_index_entries=*/{},
                                    /*row_id_column_conflict_checker=*/std::nullopt, commit_kind);
}

Status CheckConflicts(const ConflictDetection& detection,
                      const std::vector<ManifestEntry>& base_entries,
                      const std::vector<ManifestEntry>& delta_entries,
                      const std::vector<IndexManifestEntry>& delta_index_entries,
                      const Snapshot::CommitKind& commit_kind) {
    return detection.CheckConflicts(MakeSnapshot(commit_kind), base_entries, delta_entries,
                                    delta_index_entries,
                                    /*row_id_column_conflict_checker=*/std::nullopt, commit_kind);
}

}  // namespace

class ConflictDetectionTest : public testing::Test {
 public:
    void SetUp() override {
        fields_ = {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                   arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    }

 protected:
    ManifestEntry CreateManifestEntry(const std::string& file_name, const FileKind& kind) const {
        int32_t arity = 1;
        BinaryRow row(arity);
        BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
        writer.WriteInt(0, 10);
        writer.Complete();
        return CreateManifestEntry(file_name, row, kind);
    }

    ManifestEntry CreateManifestEntry(const std::string& file_name, const BinaryRow& partition,
                                      const FileKind& kind) const {
        return CreateManifestEntry(file_name, partition, kind, DataFileMeta::EmptyMinKey(),
                                   DataFileMeta::EmptyMaxKey(), /*level=*/2, /*bucket=*/0);
    }

    ManifestEntry CreateManifestEntry(const std::string& file_name, const BinaryRow& partition,
                                      const FileKind& kind, const BinaryRow& min_key,
                                      const BinaryRow& max_key, int32_t level, int32_t bucket = 0,
                                      int32_t total_buckets = 2) const {
        auto data_file_meta = std::make_shared<DataFileMeta>(
            file_name, 1024, 8, min_key, max_key, SimpleStats::EmptyStats(),
            SimpleStats::EmptyStats(), /*min_seq_no=*/16, /*max_seq_no=*/32,
            /*schema_id=*/1, level,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/3,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        return ManifestEntry(kind, partition, bucket, total_buckets, data_file_meta);
    }

    ManifestEntry CreateManifestEntryWithFirstRowId(const std::string& file_name,
                                                    const BinaryRow& partition,
                                                    const FileKind& kind, int32_t bucket,
                                                    int64_t first_row_id, int64_t row_count) const {
        auto data_file_meta = std::make_shared<DataFileMeta>(
            file_name, 1024, row_count, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/2,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, first_row_id,
            /*write_cols=*/std::nullopt);
        return ManifestEntry(kind, partition, bucket, /*total_buckets=*/2, data_file_meta);
    }

    IndexManifestEntry CreateGlobalIndexEntry(const std::string& file_name,
                                              const BinaryRow& partition, int32_t bucket,
                                              int64_t row_range_start,
                                              int64_t row_range_end) const {
        GlobalIndexMeta global_index_meta(row_range_start, row_range_end, /*index_field_id=*/1,
                                          /*extra_field_ids=*/std::nullopt,
                                          std::make_shared<Bytes>("meta", GetDefaultPool().get()));
        auto index_file_meta = std::make_shared<IndexFileMeta>(
            "HASH", file_name, /*file_size=*/100, /*row_count=*/5,
            /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt, global_index_meta);
        return IndexManifestEntry(FileKind::Add(), partition, bucket, index_file_meta);
    }

    BinaryRow CreateIntRow(int32_t value) const {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
        writer.WriteInt(0, value);
        writer.Complete();
        return row;
    }

    arrow::FieldVector fields_;
};

TEST_F(ConflictDetectionTest, TestFileDeletionConflicts) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("f1", FileKind::Add()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("f1", FileKind::Delete()));

        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("f2", FileKind::Add()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("f1", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("f3", FileKind::Add()));

        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
            "Trying to delete file f1");
    }
    {
        std::vector<ManifestEntry> base_entries;
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("f1", FileKind::Delete()));

        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
            "Trying to delete file f1");
    }
}

TEST_F(ConflictDetectionTest, TestGlobalIndexRowIdExistenceConflicts) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    const BinaryRow partition = CreateIntRow(10);
    std::vector<ManifestEntry> base_entries;
    base_entries.push_back(CreateManifestEntryWithFirstRowId("base-1", partition, FileKind::Add(),
                                                             /*bucket=*/0, /*first_row_id=*/0,
                                                             /*row_count=*/10));
    base_entries.push_back(CreateManifestEntryWithFirstRowId("base-2", partition, FileKind::Add(),
                                                             /*bucket=*/0, /*first_row_id=*/10,
                                                             /*row_count=*/10));
    std::vector<ManifestEntry> changes;

    ASSERT_OK(
        CheckConflicts(detection, base_entries, changes,
                       {CreateGlobalIndexEntry("global-index-covered", partition, /*bucket=*/0,
                                               /*row_range_start=*/0, /*row_range_end=*/19)},
                       Snapshot::CommitKind::Append()));

    ASSERT_NOK_WITH_MSG(
        CheckConflicts(detection, base_entries, changes,
                       {CreateGlobalIndexEntry("global-index-missing", partition, /*bucket=*/0,
                                               /*row_range_start=*/0, /*row_range_end=*/20)},
                       Snapshot::CommitKind::Append()),
        "Global index row ID existence conflict");
}

TEST_F(ConflictDetectionTest, TestDedicatedStorageRowIdRangeConflicts) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    const BinaryRow partition = CreateIntRow(10);
    std::vector<ManifestEntry> base_entries;
    base_entries.push_back(CreateManifestEntryWithFirstRowId("data-0.orc", partition,
                                                             FileKind::Add(), /*bucket=*/0,
                                                             /*first_row_id=*/0,
                                                             /*row_count=*/10));

    std::vector<ManifestEntry> out_of_range_dedicated_entries;
    out_of_range_dedicated_entries.push_back(CreateManifestEntryWithFirstRowId(
        "blob-0.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/5,
        /*row_count=*/10));
    ASSERT_NOK_WITH_MSG(CheckConflicts(detection, base_entries, out_of_range_dedicated_entries,
                                       Snapshot::CommitKind::Compact()),
                        "row-id range conflicts");

    std::vector<ManifestEntry> contained_dedicated_entries;
    contained_dedicated_entries.push_back(CreateManifestEntryWithFirstRowId(
        "blob-1.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/5,
        /*row_count=*/4));
    ASSERT_OK(CheckConflicts(detection, base_entries, contained_dedicated_entries,
                             Snapshot::CommitKind::Compact()));

    std::vector<ManifestEntry> disjoint_data_entries;
    disjoint_data_entries.push_back(CreateManifestEntryWithFirstRowId(
        "data-a.orc", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0,
        /*row_count=*/5));
    disjoint_data_entries.push_back(CreateManifestEntryWithFirstRowId(
        "data-b.orc", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/10,
        /*row_count=*/5));
    std::vector<ManifestEntry> spanning_dedicated_entries;
    spanning_dedicated_entries.push_back(CreateManifestEntryWithFirstRowId(
        "vector-0.vector.data", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/3,
        /*row_count=*/10));
    ASSERT_NOK_WITH_MSG(CheckConflicts(detection, disjoint_data_entries, spanning_dedicated_entries,
                                       Snapshot::CommitKind::Compact()),
                        "spans multiple data file ranges");

    std::vector<ManifestEntry> no_data_entries;
    std::vector<ManifestEntry> dedicated_only_entries;
    dedicated_only_entries.push_back(CreateManifestEntryWithFirstRowId(
        "blob-only.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0,
        /*row_count=*/1));
    ASSERT_NOK_WITH_MSG(
        CheckConflicts(detection, no_data_entries, dedicated_only_entries,
                       Snapshot::CommitKind::Compact()),
        "is not covered by one data file range");
}

TEST_F(ConflictDetectionTest, TestBucketKeepSame) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));

    const BinaryRow partition = CreateIntRow(10);
    {
        ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   DataFileMeta::EmptyMinKey(),
                                                   DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                                   /*bucket=*/0, /*total_buckets=*/4));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, /*total_buckets=*/4));

        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   DataFileMeta::EmptyMinKey(),
                                                   DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                                   /*bucket=*/0, /*total_buckets=*/2));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, /*total_buckets=*/4));

        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
            "Total buckets of partition");
    }
    {
        ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   DataFileMeta::EmptyMinKey(),
                                                   DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                                   /*bucket=*/0, /*total_buckets=*/2));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", CreateIntRow(20), FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, /*total_buckets=*/4));

        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   DataFileMeta::EmptyMinKey(),
                                                   DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                                   /*bucket=*/0, /*total_buckets=*/2));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, /*total_buckets=*/4));

        ASSERT_OK(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Overwrite()));
    }
}

TEST_F(ConflictDetectionTest, TestBucketKeepSameHelpers) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    const BinaryRow partition = CreateIntRow(10);
    std::vector<ManifestEntry> changes;
    changes.push_back(CreateManifestEntry("delta-1", partition, FileKind::Add(),
                                          DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
                                          /*level=*/1,
                                          /*bucket=*/0, /*total_buckets=*/4));
    changes.push_back(CreateManifestEntry("delta-2", partition, FileKind::Add(),
                                          DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
                                          /*level=*/1,
                                          /*bucket=*/1, /*total_buckets=*/4));

    std::unordered_map<BinaryRow, int32_t> expected_total_buckets;
    ASSERT_OK(detection.CollectUncheckedBucketPartitions(changes, &expected_total_buckets));
    ASSERT_EQ(1U, expected_total_buckets.size());
    ASSERT_EQ(4, expected_total_buckets.at(partition));

    std::unordered_map<BinaryRow, int32_t> previous_total_buckets;
    previous_total_buckets.emplace(partition, 4);
    ASSERT_OK(
        detection.CheckSameBucketByTotalBuckets(expected_total_buckets, previous_total_buckets));

    std::unordered_map<BinaryRow, int32_t> cached_total_buckets;
    ASSERT_OK(detection.CollectUncheckedBucketPartitions(changes, &cached_total_buckets));
    ASSERT_TRUE(cached_total_buckets.empty());

    ConflictDetection mismatch_detection(table_schema, core_options, nullptr, nullptr, nullptr,
                                         nullptr);
    ASSERT_NOK_WITH_MSG(
        mismatch_detection.CheckSameBucketByTotalBuckets(expected_total_buckets, {{partition, 2}}),
        "Total buckets of partition");
}

TEST_F(ConflictDetectionTest, TestCollectUncheckedBucketPartitionsMismatch) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    const BinaryRow partition = CreateIntRow(10);
    std::vector<ManifestEntry> changes;
    changes.push_back(CreateManifestEntry("delta-1", partition, FileKind::Add(),
                                          DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
                                          /*level=*/1,
                                          /*bucket=*/0, /*total_buckets=*/2));
    changes.push_back(CreateManifestEntry("delta-2", partition, FileKind::Add(),
                                          DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
                                          /*level=*/1,
                                          /*bucket=*/1, /*total_buckets=*/4));

    std::unordered_map<BinaryRow, int32_t> total_buckets;
    ASSERT_NOK_WITH_MSG(detection.CollectUncheckedBucketPartitions(changes, &total_buckets),
                        "Total buckets of partition");
}

TEST_F(ConflictDetectionTest, TestBucketKeepSameCacheEviction) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    const int32_t total_buckets = 4;
    for (int32_t value = 0; value <= 1000; ++value) {
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", CreateIntRow(value), FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, total_buckets));

        std::unordered_map<BinaryRow, int32_t> expected_total_buckets;
        ASSERT_OK(detection.CollectUncheckedBucketPartitions(changes, &expected_total_buckets));
        ASSERT_EQ(1U, expected_total_buckets.size());
        ASSERT_OK(detection.CheckSameBucketByTotalBuckets(expected_total_buckets,
                                                          expected_total_buckets));
    }

    std::vector<ManifestEntry> evicted_partition_changes;
    evicted_partition_changes.push_back(
        CreateManifestEntry("delta", CreateIntRow(0), FileKind::Add(), DataFileMeta::EmptyMinKey(),
                            DataFileMeta::EmptyMaxKey(), /*level=*/1, /*bucket=*/0, total_buckets));
    std::unordered_map<BinaryRow, int32_t> evicted_partition_buckets;
    ASSERT_OK(detection.CollectUncheckedBucketPartitions(evicted_partition_changes,
                                                         &evicted_partition_buckets));
    ASSERT_EQ(1U, evicted_partition_buckets.size());
}

TEST_F(ConflictDetectionTest, TestDeletionVectorsNotSupportedWithBucketUnawareMode) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::BUCKET, "0"},
                                               {Options::DELETION_VECTORS_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    ASSERT_NOK_WITH_MSG(CheckConflicts(detection, /*base_entries=*/{}, /*delta_entries=*/{},
                                       Snapshot::CommitKind::Append()),
                        "not yet support dv with BUCKET_UNAWARE mode");
}

TEST_F(ConflictDetectionTest,
       TestDeletionVectorsNotSupportedWithResolvedBucketUnawareModeFromMinusOne) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::BUCKET, "-1"},
                                               {Options::DELETION_VECTORS_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    ASSERT_NOK_WITH_MSG(CheckConflicts(detection, /*base_entries=*/{}, /*delta_entries=*/{},
                                       Snapshot::CommitKind::Append()),
                        "not yet support dv with BUCKET_UNAWARE mode");
}

TEST_F(ConflictDetectionTest, TestDeletionVectorsAllowedWithResolvedDynamicBucketMode) {
    auto fields = {arrow::field("f0", arrow::int32(), /*nullable=*/false),
                   arrow::field("f1", arrow::int32(), /*nullable=*/false),
                   arrow::field("f2", arrow::int32())};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{"f1", "f0"}, {{Options::BUCKET, "-1"}}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::BUCKET, "-1"},
                                               {Options::DELETION_VECTORS_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    ASSERT_OK(CheckConflicts(detection, /*base_entries=*/{}, /*delta_entries=*/{},
                             Snapshot::CommitKind::Append()));
}

TEST_F(ConflictDetectionTest, TestCheckLsmKeyRangeConflict) {
    auto fields = {arrow::field("f0", arrow::int32(), /*nullable=*/false),
                   arrow::field("f1", arrow::int32(), /*nullable=*/false),
                   arrow::field("f2", arrow::int32())};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{"f1", "f0"}, {{Options::BUCKET, "4"}}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({{Options::BUCKET, "4"}}));
    ConflictDetection detection(table_schema, core_options, nullptr, nullptr, nullptr, nullptr);

    const BinaryRow partition = CreateIntRow(10);
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/1));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(), CreateIntRow(3),
                                              CreateIntRow(5), /*level=*/1));
        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
            "LSM conflicts detected");
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/1));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(), CreateIntRow(4),
                                              CreateIntRow(5), /*level=*/1));
        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/0));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(), CreateIntRow(2),
                                              CreateIntRow(5), /*level=*/0));
        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/1, /*bucket=*/0));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(), CreateIntRow(2),
                                              CreateIntRow(5), /*level=*/1,
                                              /*bucket=*/1));
        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/1));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", CreateIntRow(20), FileKind::Add(),
                                              CreateIntRow(2), CreateIntRow(5), /*level=*/1));
        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
}

}  // namespace paimon::test
