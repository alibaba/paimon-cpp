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

#include "paimon/core/operation/append_only_file_store_scan.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/manifest/partition_entry.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"
namespace paimon::test {

TEST(AppendOnlyFileStoreScanTest, TestReconstructPredicateWithNonCastedFields) {
    std::string table_root =
        paimon::test::GetDataDir() +
        "/orc/append_table_alter_table_with_cast.db/append_table_alter_table_with_cast";
    auto fs = std::make_shared<LocalFileSystem>();
    auto pool = GetDefaultPool();
    SchemaManager manager(fs, table_root);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(/*schema_id=*/0));
    ASSERT_TRUE(schema);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema1, manager.ReadSchema(/*schema_id=*/1));
    ASSERT_TRUE(schema1);
    auto evo = std::make_shared<SimpleStatsEvolution>(schema->Fields(), schema1->Fields(),
                                                      /*need_mapping=*/true, pool);
    ASSERT_OK_AND_ASSIGN(
        auto child1,
        PredicateBuilder::Or(
            {PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f4", FieldType::TIMESTAMP),
             PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"key0", FieldType::INT),
             PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"key1", FieldType::INT)}));

    auto sub_child1 =
        PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::INT);
    auto sub_child2 =
        PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f1", FieldType::STRING);
    auto sub_child3 =
        PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f2", FieldType::DECIMAL);
    ASSERT_OK_AND_ASSIGN(auto child2, PredicateBuilder::And({sub_child1, sub_child2, sub_child3}));

    auto child3 =
        PredicateBuilder::IsNull(/*field_index=*/6, /*field_name=*/"f0", FieldType::BOOLEAN);
    auto child4 = PredicateBuilder::IsNull(/*field_index=*/7, /*field_name=*/"f6", FieldType::INT);

    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2, child3, child4}));

    ASSERT_OK_AND_ASSIGN(
        auto result,
        AppendOnlyFileStoreScan::ReconstructPredicateWithNonCastedFields(predicate, evo));
    ASSERT_EQ(*result, *child4);
}

TEST(AppendOnlyFileStoreScanTest, TestReadPartitionEntries) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::FILE_FORMAT, "orc")
        .AddOption(Options::MANIFEST_FORMAT, "orc")
        .AddOption(Options::SCAN_SNAPSHOT_ID, "5");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    auto typed_table_scan = dynamic_cast<AbstractTableScan*>(table_scan.get());
    ASSERT_TRUE(typed_table_scan);

    auto file_store_scan = typed_table_scan->snapshot_reader_->scan_;
    ASSERT_TRUE(file_store_scan);
    ASSERT_OK_AND_ASSIGN(std::vector<PartitionEntry> result_partition_entries,
                         file_store_scan->ReadPartitionEntries());

    auto GenerateRow = [&](int32_t value) {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 0, pool.get());
        writer.WriteInt(0, value);
        writer.Complete();
        return row;
    };

    std::vector<PartitionEntry> expected_partition_entries = {
        PartitionEntry(GenerateRow(10), /*record_count=*/9, /*file_size_in_bytes=*/1183,
                       /*file_count=*/2, /*last_file_creation_time=*/1721643834472l - 28800000l,
                       /*total_buckets=*/2),
        PartitionEntry(GenerateRow(20), /*record_count=*/2, /*file_size_in_bytes=*/1047,
                       /*file_count=*/2, /*last_file_creation_time=*/1721643267404l - 28800000l,
                       /*total_buckets=*/2)};
    auto ComparePartitionEntryByPartition = [](const PartitionEntry& lhs,
                                               const PartitionEntry& rhs) -> bool {
        return lhs.Partition().GetInt(0) < rhs.Partition().GetInt(0);
    };

    std::stable_sort(result_partition_entries.begin(), result_partition_entries.end(),
                     ComparePartitionEntryByPartition);
    std::stable_sort(expected_partition_entries.begin(), expected_partition_entries.end(),
                     ComparePartitionEntryByPartition);

    ASSERT_EQ(result_partition_entries, expected_partition_entries);
}
}  // namespace paimon::test
