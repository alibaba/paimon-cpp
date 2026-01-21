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

#include "paimon/core/operation/data_evolution_file_store_scan.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/reader/data_evolution_array.h"
#include "paimon/common/reader/data_evolution_row.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class DataEvolutionFileStoreScanTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    std::shared_ptr<TableSchema> CreateTableSchema(int64_t schema_id,
                                                   const std::vector<DataField>& fields) const {
        EXPECT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::InitSchema(schema_id, fields, /*highest_field_id=*/fields.size(),
                                    /*partition_keys=*/{},
                                    /*primary_keys=*/{}, /*options=*/{}, /*time_millis=*/0));
        return table_schema;
    }
    ManifestEntry CreateManifestEntry(
        int64_t schema_id, const SimpleStats& stats,
        const std::optional<std::vector<std::string>>& value_stats_cols,
        const std::optional<std::vector<std::string>>& write_cols,
        int64_t sequence_number = 0) const {
        EXPECT_OK_AND_ASSIGN(
            auto meta,
            DataFileMeta::ForAppend(
                /*file_name=*/"test-file.parquet", /*file_size=*/100l, /*row_count=*/100l, stats,
                /*min_sequence_number=*/sequence_number,
                /*max_sequence_number=*/sequence_number, schema_id,
                /*file_source=*/FileSource::Append(), value_stats_cols,
                /*external_path=*/std::nullopt, /*first_row_id=*/0, write_cols));
        return ManifestEntry(FileKind::Add(), /*partition=*/BinaryRow::EmptyRow(), /*bucket=*/0,
                             /*total_buckets=*/1, meta);
    }

    ManifestEntry CreateBlobManifestEntry(
        int64_t schema_id, const SimpleStats& stats,
        const std::optional<std::vector<std::string>>& value_stats_cols,
        const std::optional<std::vector<std::string>>& write_cols, int64_t first_row_id,
        int64_t row_count) const {
        EXPECT_OK_AND_ASSIGN(
            auto meta,
            DataFileMeta::ForAppend(
                /*file_name=*/"test-file.blob", /*file_size=*/100l, /*row_count=*/100l, stats,
                /*min_sequence_number=*/0l,
                /*max_sequence_number=*/0l, schema_id,
                /*file_source=*/FileSource::Append(), value_stats_cols,
                /*external_path=*/std::nullopt, /*first_row_id=*/0, write_cols));
        return ManifestEntry(FileKind::Add(), /*partition=*/BinaryRow::EmptyRow(), /*bucket=*/0,
                             /*total_buckets=*/1, meta);
    }

    void CheckFieldCountAndArraySize(const std::shared_ptr<DataEvolutionRow>& min_row,
                                     const std::shared_ptr<DataEvolutionRow>& max_row,
                                     const std::shared_ptr<DataEvolutionArray>& null_counts,
                                     int32_t expected) const {
        ASSERT_EQ(min_row->GetFieldCount(), expected);
        ASSERT_EQ(max_row->GetFieldCount(), expected);
        ASSERT_EQ(null_counts->Size(), expected);
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsSingleFile) {
    std::vector<DataField> fields = {DataField(0, arrow::field("f0", arrow::int32())),
                                     DataField(1, arrow::field("f1", arrow::utf8()))};
    auto table_schema = CreateTableSchema(/*schema_id=*/0, fields);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        assert(schema_id == 0);
        return table_schema;
    };

    auto entry = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a")}, {5, std::string("z")}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry}, table_schema, schema_fetcher));
    auto result_row_count = result_stats.first;
    ASSERT_EQ(result_row_count, 100);
    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 2);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 1);
    ASSERT_EQ(max_row->GetInt(0), 5);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "z");

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 0);
    ASSERT_EQ(null_counts->GetLong(1), 1);
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsMultipleFiles) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
    };

    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, fields);
    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, {fields[0], fields[2]});

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a"), 10}, {3, std::string("c"), 30},
                                          {0, 1, 0}, pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({2, 20}, {4, 40}, {1, 2}, pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry1, entry0}, table_schema0, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 3);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 2);
    ASSERT_EQ(max_row->GetInt(0), 4);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "c");
    ASSERT_EQ(min_row->GetInt(2), 20);
    ASSERT_EQ(max_row->GetInt(2), 40);

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 1);
    ASSERT_EQ(null_counts->GetLong(1), 1);
    ASSERT_EQ(null_counts->GetLong(2), 2);
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithSchemaEvolution) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
    };

    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, {fields[0], fields[1]});
    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, fields);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a")}, {3, std::string("c")}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({2, std::string("b"), 20}, {4, std::string("d"), 40},
                                          {1, 0, 1}, pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry0, entry1}, table_schema1, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 3);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 1);
    ASSERT_EQ(max_row->GetInt(0), 3);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "c");
    ASSERT_EQ(min_row->GetInt(2), 20);
    ASSERT_EQ(max_row->GetInt(2), 40);

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 0);
    ASSERT_EQ(null_counts->GetLong(1), 1);
    ASSERT_EQ(null_counts->GetLong(2), 1);
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithWriteColsNotEqualToValueStatsCols) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
    };

    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, fields);
    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, fields);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a")}, {3, std::string("c")}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f0", "f1"}),
        /*write_cols=*/std::vector<std::string>({"f0", "f1", "f2"}));
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({2, 20}, {4, 40}, {1, 0}, pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f0", "f2"}),
        /*write_cols=*/std::vector<std::string>({"f0", "f2"}));

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry0, entry1}, table_schema1, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 3);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 1);
    ASSERT_EQ(max_row->GetInt(0), 3);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "c");
    ASSERT_TRUE(min_row->IsNullAt(2));
    ASSERT_TRUE(max_row->IsNullAt(2));

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 0);
    ASSERT_EQ(null_counts->GetLong(1), 1);
    ASSERT_TRUE(null_counts->IsNullAt(2));
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithSchemaEvolutionRenameFields) {
    std::vector<DataField> fields0 = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::utf8())),
    };
    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, fields0);
    std::vector<DataField> fields1 = {
        DataField(0, arrow::field("f3", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f0", arrow::int32())),
        DataField(4, arrow::field("f5", arrow::utf8())),
    };
    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, fields1);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({std::string("a"), 10}, {std::string("c"), 15}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f1", "f2"}),
        /*write_cols=*/std::vector<std::string>({"f1", "f2"}));
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({20, 2}, {25, 5}, {1, 0}, pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f2", "f0"}),
        /*write_cols=*/std::vector<std::string>({"f2", "f0"}));
    auto entry2 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({std::string("bb"), std::string("cd")},
                                          {std::string("dd"), std::string("xy")}, {1, 3},
                                          pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f5", "f1"}),
        /*write_cols=*/std::vector<std::string>({"f3", "f1", "f5"}));

    ASSERT_OK_AND_ASSIGN(auto result_stats,
                         DataEvolutionFileStoreScan::EvolutionStats({entry2, entry1, entry0},
                                                                    table_schema1, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 4);

    // check min/max row
    ASSERT_TRUE(min_row->IsNullAt(0));
    ASSERT_TRUE(max_row->IsNullAt(0));
    ASSERT_EQ(min_row->GetString(1).ToString(), "cd");
    ASSERT_EQ(max_row->GetString(1).ToString(), "xy");
    ASSERT_EQ(min_row->GetInt(2), 20);
    ASSERT_EQ(max_row->GetInt(2), 25);
    ASSERT_EQ(min_row->GetString(3).ToString(), "bb");
    ASSERT_EQ(max_row->GetString(3).ToString(), "dd");

    // check null count array
    ASSERT_TRUE(null_counts->IsNullAt(0));
    ASSERT_EQ(null_counts->GetLong(1), 3);
    ASSERT_EQ(null_counts->GetLong(2), 1);
    ASSERT_EQ(null_counts->GetLong(3), 1);
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithSchemaEvolutionUpdateType) {
    std::vector<DataField> fields0 = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::utf8())),
    };
    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, fields0);
    std::vector<DataField> fields1 = {
        DataField(4, arrow::field("f4", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::int32())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::int32())),
    };

    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, fields1);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({10, std::string("a"), 11, std::string("b")},
                                          {20, std::string("c"), 22, std::string("d")},
                                          {0, 1, 2, 3}, pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({2}, {5}, {0}, pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f4"}),
        /*write_cols=*/std::vector<std::string>({"f1", "f4"}));

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry1, entry0}, table_schema1, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 4);

    // check min/max values and null_counts
    ASSERT_EQ(min_row->GetInt(0), 2);
    ASSERT_EQ(max_row->GetInt(0), 5);
    ASSERT_EQ(null_counts->GetLong(0), 0);

    ASSERT_TRUE(min_row->IsNullAt(1));
    ASSERT_TRUE(max_row->IsNullAt(1));
    ASSERT_TRUE(null_counts->IsNullAt(1));

    ASSERT_EQ(min_row->GetInt(2), 11);
    ASSERT_EQ(max_row->GetInt(2), 22);
    ASSERT_EQ(null_counts->GetLong(2), 2);

    // f1 in entry0 type mismatch schema1
    ASSERT_TRUE(min_row->IsNullAt(3));
    ASSERT_TRUE(max_row->IsNullAt(3));
    ASSERT_TRUE(null_counts->IsNullAt(3));
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithBlob) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, BlobUtils::ToArrowField("blob")),
    };
    auto table_schema = CreateTableSchema(/*schema_id=*/0, fields);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        assert(schema_id == 0);
        return table_schema;
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({10, std::string("aa")}, {55, std::string("zz")}, {2, 3},
                                          pool_.get()),
        /*value_stats_cols=*/std::nullopt,
        /*write_cols=*/std::optional<std::vector<std::string>>({"f0", "f1"}),
        /*sequence_number=*/1l);

    auto blob_entry0 = CreateBlobManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {0}, pool_.get()),
        /*value_stats_cols=*/std::nullopt,
        /*write_cols=*/std::optional<std::vector<std::string>>({"f0"}),
        /*first_row_id=*/0, /*row_count=*/10l);

    auto blob_entry1 = CreateBlobManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {0}, pool_.get()),
        /*value_stats_cols=*/std::nullopt,
        /*write_cols=*/std::optional<std::vector<std::string>>({"f0"}),
        /*first_row_id=*/10, /*row_count=*/90l);

    auto entry1 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a")}, {5, std::string("z")}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::nullopt,
        /*write_cols=*/std::optional<std::vector<std::string>>({"f0", "f1"}),
        /*sequence_number=*/2l);

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {blob_entry0, blob_entry1, entry0, entry1},
                                                table_schema, schema_fetcher));

    auto result_row_count = result_stats.first;
    ASSERT_EQ(result_row_count, 100);
    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 3);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 1);
    ASSERT_EQ(max_row->GetInt(0), 5);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "z");
    ASSERT_TRUE(min_row->IsNullAt(2));
    ASSERT_TRUE(max_row->IsNullAt(2));

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 0);
    ASSERT_EQ(null_counts->GetLong(1), 1);
    ASSERT_TRUE(null_counts->IsNullAt(2));
}

TEST_F(DataEvolutionFileStoreScanTest, TestFilterEntryByRowRanges) {
    // row id range [100, 200)
    auto file = std::make_shared<DataFileMeta>(
        "data-0.orc", /*file_size=*/645,
        /*row_count=*/100, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/100, /*max_sequence_number=*/199, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/100, /*write_cols=*/std::nullopt);
    ManifestEntry entry(FileKind::Add(), BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/1,
                        file);
    {
        // row_ids is null
        ASSERT_OK_AND_ASSIGN(bool exist, DataEvolutionFileStoreScan::FilterEntryByRowRanges(
                                             entry, /*row_ranges=*/std::nullopt));
        ASSERT_TRUE(exist);
    }
    {
        // row_ids is empty
        ASSERT_OK_AND_ASSIGN(bool exist, DataEvolutionFileStoreScan::FilterEntryByRowRanges(
                                             entry, /*row_ranges=*/std::vector<Range>()));
        ASSERT_FALSE(exist);
    }
    {
        auto file_without_first_row_id = std::make_shared<DataFileMeta>(
            "data-0.orc", /*file_size=*/645,
            /*row_count=*/100, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/100, /*max_sequence_number=*/199, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1737111915429ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        ManifestEntry entry_without_first_row_id(FileKind::Add(), BinaryRow::EmptyRow(),
                                                 /*bucket=*/0, /*total_buckets=*/1,
                                                 file_without_first_row_id);
        // first row id is null
        ASSERT_OK_AND_ASSIGN(
            bool exist, DataEvolutionFileStoreScan::FilterEntryByRowRanges(
                            entry_without_first_row_id,
                            /*row_ranges=*/std::optional<std::vector<Range>>({Range(0l, 0l)})));
        ASSERT_TRUE(exist);
    }
    {
        ASSERT_OK_AND_ASSIGN(bool exist,
                             DataEvolutionFileStoreScan::FilterEntryByRowRanges(
                                 entry, /*row_ranges=*/std::optional<std::vector<Range>>(
                                     {Range(0l, 0l), Range(10l, 10l)})));
        ASSERT_FALSE(exist);
    }
    {
        ASSERT_OK_AND_ASSIGN(bool exist,
                             DataEvolutionFileStoreScan::FilterEntryByRowRanges(
                                 entry, /*row_ranges=*/std::optional<std::vector<Range>>(
                                     {Range(0l, 0l), Range(101l, 101l)})));
        ASSERT_TRUE(exist);
    }
    {
        ASSERT_OK_AND_ASSIGN(bool exist,
                             DataEvolutionFileStoreScan::FilterEntryByRowRanges(
                                 entry, /*row_ranges=*/std::optional<std::vector<Range>>(
                                     {Range(100l, 100l), Range(189l, 189l)})));
        ASSERT_TRUE(exist);
    }
}

TEST_F(DataEvolutionFileStoreScanTest, TestFilterManifestByRowRanges) {
    // row id [10, 20]
    auto manifest1 =
        ManifestFileMeta("manifest-65b0d403-a1bc-4157-b242-bff73c46596d-0", /*file_size=*/2779,
                         /*num_added_files=*/1, /*num_deleted_files=*/0, SimpleStats::EmptyStats(),
                         /*schema_id=*/0, /*min_bucket=*/0, /*max_bucket=*/0,
                         /*min_level=*/0, /*max_level=*/0,
                         /*min_row_id=*/10, /*max_row_id=*/20);
    ASSERT_TRUE(DataEvolutionFileStoreScan::FilterManifestByRowRanges(manifest1, std::nullopt));
    ASSERT_FALSE(
        DataEvolutionFileStoreScan::FilterManifestByRowRanges(manifest1, std::vector<Range>()));
    ASSERT_TRUE(DataEvolutionFileStoreScan::FilterManifestByRowRanges(
        manifest1, std::optional<std::vector<Range>>({Range(0, 15), Range(100, 200)})));
    ASSERT_FALSE(DataEvolutionFileStoreScan::FilterManifestByRowRanges(
        manifest1, std::optional<std::vector<Range>>({Range(0, 5), Range(100, 200)})));

    auto manifest2 =
        ManifestFileMeta("manifest-65b0d403-a1bc-4157-b242-bff73c46596d-0", /*file_size=*/2779,
                         /*num_added_files=*/1, /*num_deleted_files=*/0, SimpleStats::EmptyStats(),
                         /*schema_id=*/0, /*min_bucket=*/0, /*max_bucket=*/0,
                         /*min_level=*/0, /*max_level=*/0,
                         /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    ASSERT_TRUE(DataEvolutionFileStoreScan::FilterManifestByRowRanges(
        manifest2, std::optional<std::vector<Range>>({Range(0, 0)})));
}
}  // namespace paimon::test
