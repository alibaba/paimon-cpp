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

#include "paimon/core/operation/key_value_file_store_scan.h"

#include <cstdint>
#include <map>
#include <optional>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/metrics/scan_metrics.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/format/file_format.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/scan_context.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::test {
class KeyValueFileStoreScanTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {
        pool_.reset();
    }

    Result<std::unique_ptr<KeyValueFileStoreScan>> CreateFileStoreScan(
        const std::string& table_path, const std::shared_ptr<ScanFilter>& scan_filter,
        int32_t table_schema_id, int32_t snapshot_id) const {
        std::map<std::string, std::string> options_map = {{Options::MANIFEST_FORMAT, "orc"}};
        PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_map));
        auto fs = core_options.GetFileSystem();
        auto schema_manager = std::make_shared<SchemaManager>(fs, table_path);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                               schema_manager->ReadSchema(table_schema_id));

        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                               core_options.CreateExternalPaths());
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                               core_options.CreateGlobalIndexExternalPath());

        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                table_path, arrow_schema, table_schema->PartitionKeys(),
                core_options.GetPartitionDefaultName(),
                core_options.GetWriteFileFormat()->Identifier(), core_options.DataFilePrefix(),
                core_options.LegacyPartitionNameEnabled(), external_paths,
                global_index_external_path, core_options.IndexFileInDataFileDir(), pool_));
        auto manifest_file_format = core_options.GetManifestFormat();
        auto snapshot_manager = std::make_shared<SnapshotManager>(fs, table_path);

        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ManifestList> manifest_list,
            ManifestList::Create(fs, manifest_file_format, core_options.GetManifestCompression(),
                                 path_factory, pool_));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Schema> partition_schema,
            FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ManifestFile> manifest_file,
            ManifestFile::Create(fs, manifest_file_format, core_options.GetManifestCompression(),
                                 path_factory, core_options.GetManifestTargetFileSize(), pool_,
                                 core_options, partition_schema));
        if (table_schema->PrimaryKeys().empty()) {
            return Status::Invalid("not a pk table in KeyValueFileStoreScan");
        }
        PAIMON_ASSIGN_OR_RAISE(auto trimmed_pk, table_schema->TrimmedPrimaryKeys());
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<KeyValueFileStoreScan> scan,
            KeyValueFileStoreScan::Create(snapshot_manager, schema_manager, manifest_list,
                                          manifest_file, table_schema, arrow_schema, scan_filter,
                                          core_options, CreateDefaultExecutor(), pool_));
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager->LoadSnapshot(snapshot_id));
        scan->WithSnapshot(snapshot);
        return scan;
    }

    static int64_t GetMaxSequenceNumberOfRawPlan(
        const std::shared_ptr<FileStoreScan::RawPlan>& raw_plan) {
        std::vector<std::shared_ptr<DataFileMeta>> metas;
        const std::vector<ManifestEntry>& files = raw_plan->Files();
        for (const auto& file : files) {
            metas.push_back(file.File());
        }
        return DataFileMeta::GetMaxSequenceNumber(metas);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(KeyValueFileStoreScanTest, TestMaxSequenceNumber) {
    {
        // test with single partition field, scan multiple times with snapshot2 and 4
        std::string table_path = paimon::test::GetDataDir() +
                                 "orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality";
        std::vector<std::map<std::string, std::string>> partition_filters = {{{"f1", "10"}}};
        auto scan_filter =
            std::make_shared<ScanFilter>(/*predicate=*/nullptr,
                                         /*partition_filters=*/partition_filters,
                                         /*bucket_filter=*/0, /*vector_search=*/nullptr);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreScan> scan,
                             CreateFileStoreScan(table_path, scan_filter,
                                                 /*table_schema_id=*/0, /*snapshot_id=*/2));

        const auto started = std::chrono::high_resolution_clock::now();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> raw_plan, scan->CreatePlan());
        std::shared_ptr<Metrics> metrics = scan->GetScanMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t last_scan_duration,
                             metrics->GetCounter(ScanMetrics::LAST_SCAN_DURATION));
        ASSERT_LE(last_scan_duration, std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::high_resolution_clock::now() - started)
                                          .count());
        ASSERT_OK_AND_ASSIGN(uint64_t last_scanned_snapshot_id,
                             metrics->GetCounter(ScanMetrics::LAST_SCANNED_SNAPSHOT_ID));
        ASSERT_EQ(last_scanned_snapshot_id, 2u);
        ASSERT_OK_AND_ASSIGN(uint64_t last_scanned_manifests,
                             metrics->GetCounter(ScanMetrics::LAST_SCANNED_MANIFESTS));
        ASSERT_EQ(last_scanned_manifests, 2u);
        ASSERT_OK_AND_ASSIGN(uint64_t last_scan_skipped_table_files,
                             metrics->GetCounter(ScanMetrics::LAST_SCAN_SKIPPED_TABLE_FILES));
        ASSERT_EQ(last_scan_skipped_table_files, 1u);
        ASSERT_OK_AND_ASSIGN(uint64_t last_scan_resulted_table_files,
                             metrics->GetCounter(ScanMetrics::LAST_SCAN_RESULTED_TABLE_FILES));
        ASSERT_EQ(last_scan_resulted_table_files, 1u);
        int64_t max_sequence_num = GetMaxSequenceNumberOfRawPlan(raw_plan);
        ASSERT_EQ(max_sequence_num, 1);
        // test multiple scan
        ASSERT_OK_AND_ASSIGN(Snapshot snapshot, scan->GetSnapshotManager()->LoadSnapshot(4));
        scan->WithSnapshot(snapshot);
        ASSERT_OK_AND_ASSIGN(raw_plan, scan->CreatePlan());
        max_sequence_num = GetMaxSequenceNumberOfRawPlan(raw_plan);
        ASSERT_EQ(max_sequence_num, 2);
    }
    {
        // test with single partition field with bucket 1, latest snapshot is snapshot4
        std::string table_path = paimon::test::GetDataDir() +
                                 "orc/pk_table_with_dv_cardinality.db/"
                                 "pk_table_with_dv_cardinality";
        std::vector<std::map<std::string, std::string>> partition_filters = {{{"f1", "10"}}};
        auto scan_filter =
            std::make_shared<ScanFilter>(/*predicate=*/nullptr,
                                         /*partition_filters=*/partition_filters,
                                         /*bucket_filter=*/1, /*vector_search=*/nullptr);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreScan> scan,
                             CreateFileStoreScan(table_path, scan_filter,
                                                 /*table_schema_id=*/0, /*snapshot_id=*/4));

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> raw_plan, scan->CreatePlan());
        int64_t max_sequence_num = GetMaxSequenceNumberOfRawPlan(raw_plan);
        ASSERT_EQ(max_sequence_num, 6);
    }
    {
        // test with multi partition field, latest snapshot is snapshot1
        std::string table_path =
            paimon::test::GetDataDir() + "orc/pk_table_with_mor.db/pk_table_with_mor";
        std::vector<std::map<std::string, std::string>> partition_filters = {
            {{"p0", "1"}, {"p1", "0"}}};
        auto scan_filter =
            std::make_shared<ScanFilter>(/*predicate=*/nullptr,
                                         /*partition_filters=*/partition_filters,
                                         /*bucket_filter=*/0, /*vector_search=*/nullptr);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreScan> scan,
                             CreateFileStoreScan(table_path, scan_filter,
                                                 /*table_schema_id=*/0, /*snapshot_id=*/1));

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> raw_plan, scan->CreatePlan());
        int64_t max_sequence_num = GetMaxSequenceNumberOfRawPlan(raw_plan);
        ASSERT_EQ(max_sequence_num, 0);
    }
    {
        // test with multi partition field, latest snapshot is snapshot2
        std::string table_path =
            paimon::test::GetDataDir() + "orc/pk_table_with_mor.db/pk_table_with_mor";
        std::vector<std::map<std::string, std::string>> partition_filters = {
            {{"p0", "0"}, {"p1", "0"}}};
        auto scan_filter =
            std::make_shared<ScanFilter>(/*predicate=*/nullptr,
                                         /*partition_filters=*/partition_filters,
                                         /*bucket_filter=*/0, /*vector_search=*/nullptr);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreScan> scan,
                             CreateFileStoreScan(table_path, scan_filter,
                                                 /*table_schema_id=*/0, /*snapshot_id=*/2));

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> raw_plan, scan->CreatePlan());
        int64_t max_sequence_num = GetMaxSequenceNumberOfRawPlan(raw_plan);
        ASSERT_EQ(max_sequence_num, 7);
    }
    {
        // test without partition field
        std::string table_path =
            paimon::test::GetDataDir() + "orc/pk_table_partial_update.db/pk_table_partial_update";
        std::vector<std::map<std::string, std::string>> partition_filters = {};
        auto scan_filter =
            std::make_shared<ScanFilter>(/*predicate=*/nullptr,
                                         /*partition_filters=*/partition_filters,
                                         /*bucket_filter=*/0, /*vector_search=*/nullptr);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreScan> scan,
                             CreateFileStoreScan(table_path, scan_filter,
                                                 /*table_schema_id=*/0, /*snapshot_id=*/2));

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> raw_plan, scan->CreatePlan());
        int64_t max_sequence_num = GetMaxSequenceNumberOfRawPlan(raw_plan);
        ASSERT_EQ(max_sequence_num, 7);
    }
}

TEST_F(KeyValueFileStoreScanTest, TestSplitAndSetKeyValueFilter) {
    std::string table_path =
        paimon::test::GetDataDir() + "orc/pk_table_with_mor.db/pk_table_with_mor";
    std::vector<std::map<std::string, std::string>> partition_filters = {
        {{"p0", "1"}, {"p1", "0"}}};
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"k1",
                                                FieldType::INT, Literal(20));
    auto equal = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"k0", FieldType::INT,
                                         Literal(10));
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"v0",
                                                      FieldType::DOUBLE, Literal(30.1));
    auto less_than =
        PredicateBuilder::LessThan(/*field_index=*/3, "p0", FieldType::INT, Literal(40));
    ASSERT_OK_AND_ASSIGN(auto predicate,
                         PredicateBuilder::And({not_equal, equal, greater_than, less_than}));
    auto scan_filter = std::make_shared<ScanFilter>(/*predicate=*/predicate,
                                                    /*partition_filters=*/partition_filters,
                                                    /*bucket_filter=*/0, /*vector_search=*/nullptr);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyValueFileStoreScan> scan,
                         CreateFileStoreScan(table_path, scan_filter,
                                             /*table_schema_id=*/0, /*snapshot_id=*/1));
    // check key filter
    auto key_not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"k1",
                                                    FieldType::INT, Literal(20));
    auto key_equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"k0", FieldType::INT,
                                             Literal(10));
    ASSERT_OK_AND_ASSIGN(auto expected_key_filter,
                         PredicateBuilder::And({key_not_equal, key_equal}));
    ASSERT_EQ(*expected_key_filter, *(scan->key_filter_));

    // check value filter
    auto value_greater_than = PredicateBuilder::GreaterThan(/*field_index=*/6, /*field_name=*/"v0",
                                                            FieldType::DOUBLE, Literal(30.1));
    ASSERT_OK_AND_ASSIGN(auto expected_value_filter,
                         PredicateBuilder::And({key_not_equal, key_equal, value_greater_than}));
    ASSERT_EQ(*expected_value_filter, *(scan->value_filter_)) << scan->value_filter_->ToString();

    // check partition filter, less_than is removed as partition_filters overlaps predicates
    auto equal_p0 =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"p0", FieldType::INT, Literal(1));
    auto equal_p1 =
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"p1", FieldType::INT, Literal(0));
    ASSERT_OK_AND_ASSIGN(auto expected_partition_filter,
                         PredicateBuilder::And({equal_p0, equal_p1}));
    ASSERT_EQ(*expected_partition_filter, *(scan->partition_filter_));
}

TEST_F(KeyValueFileStoreScanTest, TestNoOverlapping) {
    auto generate_manifest_entries = [](const std::vector<int32_t>& levels) {
        std::vector<ManifestEntry> entries;
        for (const auto& level : levels) {
            entries.emplace_back(
                /*kind=*/FileKind::Add(), /*partition=*/BinaryRow::EmptyRow(), /*bucket=*/0,
                /*total_buckets=*/1,
                std::make_shared<DataFileMeta>(
                    /*file_name=*/"name", /*file_size=*/1024, /*row_count=*/10,
                    /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
                    /*key_stats=*/SimpleStats::EmptyStats(),
                    /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
                    /*max_sequence_number=*/10, /*schema_id=*/0, level,
                    /*extra_files=*/std::vector<std::optional<std::string>>(),
                    /*creation_time=*/Timestamp(0, 0),
                    /*delete_row_count */ std::nullopt,
                    /*embedded_index=*/nullptr,
                    /*file_source=*/FileSource::Append(), /*external_path=*/std::nullopt,
                    /*value_stats_cols=*/std::nullopt,
                    /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt));
        }
        return entries;
    };
    ASSERT_TRUE(KeyValueFileStoreScan::NoOverlapping(generate_manifest_entries({0})));
    ASSERT_FALSE(KeyValueFileStoreScan::NoOverlapping(generate_manifest_entries({0, 0, 0})));
    ASSERT_TRUE(KeyValueFileStoreScan::NoOverlapping(generate_manifest_entries({1, 1, 1})));
    ASSERT_FALSE(KeyValueFileStoreScan::NoOverlapping(generate_manifest_entries({0, 1, 1})));
    ASSERT_FALSE(KeyValueFileStoreScan::NoOverlapping(generate_manifest_entries({2, 1, 1})));
}
}  // namespace paimon::test
