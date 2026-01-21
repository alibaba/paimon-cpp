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

#include "paimon/core/stats/simple_stats_evolution.h"

#include <cstddef>
#include <functional>
#include <string>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/utils/internal_row_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Schema;
}  // namespace arrow
namespace paimon {
class FileSystem;
class InternalRow;
}  // namespace paimon

namespace paimon::test {
class SimpleStatsEvolutionTest : public ::testing::Test {
 public:
    void SetUp() override {
        std::string old_schema_str = R"==({
  "version" : 3,
  "id" : 0,
  "fields" : [ {
    "id" : 0,
    "name" : "key0",
    "type" : "INT"
  }, {
    "id" : 1,
    "name" : "key1",
    "type" : "INT"
  }, {
    "id" : 2,
    "name" : "f0",
    "type" : "STRING"
  }, {
    "id" : 3,
    "name" : "f1",
    "type" : "TIMESTAMP(9)"
  }, {
    "id" : 4,
    "name" : "f2",
    "type" : "DECIMAL(5, 2)"
  }, {
    "id" : 5,
    "name" : "f3",
    "type" : "STRING"
  }, {
    "id" : 6,
    "name" : "f4",
    "type" : "DATE"
  }, {
    "id" : 7,
    "name" : "f5",
    "type" : "BIGINT"
  } ],
  "highestFieldId" : 7,
  "partitionKeys" : [ "key0", "key1" ],
  "primaryKeys" : [ ],
  "options" : {
    "manifest.format" : "orc",
    "file.format" : "orc"
  },
  "timeMillis" : 1732604147800
})==";

        std::string new_schema_str = R"==({
  "version" : 3,
  "id" : 1,
  "fields" : [ {
    "id" : 6,
    "name" : "f4",
    "type" : "DATE"
  }, {
    "id" : 0,
    "name" : "key0",
    "type" : "INT"
  }, {
    "id" : 1,
    "name" : "key1",
    "type" : "INT"
  }, {
    "id" : 2,
    "name" : "f3",
    "type" : "INT"
  }, {
    "id" : 3,
    "name" : "f1",
    "type" : "TIMESTAMP(9)"
  }, {
    "id" : 4,
    "name" : "f2",
    "type" : "DECIMAL(6, 3)"
  }, {
    "id" : 5,
    "name" : "f0",
    "type" : "BOOLEAN"
  }, {
    "id" : 8,
    "name" : "f6",
    "type" : "INT"
  } ],
  "highestFieldId" : 8,
  "partitionKeys" : [ "key0", "key1" ],
  "primaryKeys" : [ ],
  "options" : {
    "manifest.format" : "orc",
    "file.format" : "orc"
  },
  "timeMillis" : 1732605243483
})==";

        ASSERT_OK_AND_ASSIGN(old_schema_, TableSchema::CreateFromJson(old_schema_str));
        ASSERT_OK_AND_ASSIGN(new_schema_, TableSchema::CreateFromJson(new_schema_str));
    }

    SimpleStats CreateStats() const {
        auto min = BinaryRowGenerator::GenerateRow(
            {0, 1, std::string("100"), TimestampType(Timestamp(0, 0), 9), Decimal(5, 2, 12345),
             std::string("nO"), 10, NullType()},
            pool_.get());
        auto max = BinaryRowGenerator::GenerateRow(
            {20, 21, std::string("110"), TimestampType(Timestamp(0, 1), 9), Decimal(5, 2, 32345),
             std::string("Yes"), 20, NullType()},
            pool_.get());
        auto null_count = BinaryArray::FromLongArray({0, 0, 1, 0, 0, 0, 1, 4}, pool_.get());
        return SimpleStats(min, max, null_count);
    }

    SimpleStats CreateNewStats() const {
        auto min =
            BinaryRowGenerator::GenerateRow({10, 0, 1, 100, TimestampType(Timestamp(0, 0), 9),
                                             Decimal(6, 3, 123450), false, NullType()},
                                            pool_.get());
        auto max =
            BinaryRowGenerator::GenerateRow({20, 20, 21, 110, TimestampType(Timestamp(0, 1), 9),
                                             Decimal(6, 3, 323450), true, NullType()},
                                            pool_.get());
        auto null_count = BinaryArray::FromLongArray({1, 0, 0, 1, 0, 0, 0, 4}, pool_.get());
        return SimpleStats(min, max, null_count);
    }

    void CheckResultRow(const InternalRow& result, const InternalRow& expected,
                        const std::vector<DataField>& fields) const {
        auto collect_variant =
            [](const InternalRow& row,
               const std::shared_ptr<arrow::Schema>& schema) -> std::vector<VariantType> {
            EXPECT_OK_AND_ASSIGN(auto getters,
                                 InternalRowUtils::CreateFieldGetters(schema, /*use_view=*/false));
            std::vector<VariantType> ret;
            for (const auto& getter : getters) {
                ret.push_back(getter(row));
            }
            return ret;
        };
        auto schema = DataField::ConvertDataFieldsToArrowSchema(fields);
        auto result_variants = collect_variant(result, schema);
        auto expected_variants = collect_variant(expected, schema);
        ASSERT_EQ(result_variants, expected_variants);
    }

    void CheckResultArray(const InternalArray& result, const std::vector<VariantType>& expected) {
        ASSERT_EQ(result.Size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            if (DataDefine::IsVariantNull(expected[i])) {
                ASSERT_TRUE(result.IsNullAt(i));
            } else {
                ASSERT_FALSE(result.IsNullAt(i));
                ASSERT_EQ(result.GetLong(i), DataDefine::GetVariantValue<int64_t>(expected[i]));
            }
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    std::shared_ptr<TableSchema> old_schema_;
    std::shared_ptr<TableSchema> new_schema_;
};

TEST_F(SimpleStatsEvolutionTest, TestNoChangeOfFields) {
    std::string table_root =
        paimon::test::GetDataDir() +
        "orc/append_table_alter_table_with_cast.db/append_table_alter_table_with_cast";
    SchemaManager manager(fs_, table_root);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(/*schema_id=*/0));
    ASSERT_TRUE(schema);

    SimpleStatsEvolution evo(schema->Fields(), schema->Fields(), /*need_mapping=*/false, pool_);
    SimpleStats stats = CreateStats();
    ASSERT_OK_AND_ASSIGN(SimpleStatsEvolution::EvolutionStats new_stats,
                         evo.Evolution(stats, /*row_count=*/4, /*dense_fields=*/std::nullopt));
    CheckResultRow(*(new_stats.min_values), stats.min_values_, schema->Fields());
    CheckResultRow(*(new_stats.max_values), stats.max_values_, schema->Fields());
    CheckResultArray(*(new_stats.null_counts), {0l, 0l, 1l, 0l, 0l, 0l, 1l, 4l});
}

TEST_F(SimpleStatsEvolutionTest, TestNoSchemaChangeWithEmptyDenseFields) {
    std::string table_root =
        paimon::test::GetDataDir() +
        "orc/append_table_alter_table_with_cast.db/append_table_alter_table_with_cast";
    SchemaManager manager(fs_, table_root);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(/*schema_id=*/0));
    ASSERT_TRUE(schema);

    SimpleStatsEvolution evo(schema->Fields(), schema->Fields(), /*need_mapping=*/false, pool_);

    auto null_count = BinaryArray::FromLongArray(std::vector<int64_t>(), pool_.get());
    SimpleStats stats(/*min_values=*/BinaryRow::EmptyRow(), /*max_values=*/BinaryRow::EmptyRow(),
                      null_count);

    ASSERT_OK_AND_ASSIGN(SimpleStatsEvolution::EvolutionStats new_stats,
                         evo.Evolution(stats, /*row_count=*/4,
                                       /*dense_fields=*/std::vector<std::string>({})));

    auto expected_min =
        BinaryRowGenerator::GenerateRow({NullType(), NullType(), NullType(), NullType(), NullType(),
                                         NullType(), NullType(), NullType()},
                                        pool_.get());
    auto expected_max =
        BinaryRowGenerator::GenerateRow({NullType(), NullType(), NullType(), NullType(), NullType(),
                                         NullType(), NullType(), NullType()},
                                        pool_.get());

    CheckResultRow(*(new_stats.min_values), expected_min, schema->Fields());
    CheckResultRow(*(new_stats.max_values), expected_max, schema->Fields());
    CheckResultArray(*(new_stats.null_counts), {NullType(), NullType(), NullType(), NullType(),
                                                NullType(), NullType(), NullType(), NullType()});
}

TEST_F(SimpleStatsEvolutionTest, TestNoSchemaChangeWithDenseFields) {
    std::string table_root =
        paimon::test::GetDataDir() +
        "orc/append_table_alter_table_with_cast.db/append_table_alter_table_with_cast";
    SchemaManager manager(fs_, table_root);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(/*schema_id=*/0));
    ASSERT_TRUE(schema);

    SimpleStatsEvolution evo(schema->Fields(), schema->Fields(), /*need_mapping=*/false, pool_);

    auto min = BinaryRowGenerator::GenerateRow(
        {0, 1, TimestampType(Timestamp(0, 1), 9), NullType()}, pool_.get());
    auto max = BinaryRowGenerator::GenerateRow(
        {20, 21, TimestampType(Timestamp(20, 21), 9), NullType()}, pool_.get());
    auto null_count = BinaryArray::FromLongArray({0, 0, 1, 4}, pool_.get());
    SimpleStats stats(min, max, null_count);

    ASSERT_OK_AND_ASSIGN(
        SimpleStatsEvolution::EvolutionStats new_stats,
        evo.Evolution(stats, /*row_count=*/4,
                      /*dense_fields=*/std::vector<std::string>({"key0", "key1", "f1", "f5"})));

    auto expected_min =
        BinaryRowGenerator::GenerateRow({0, 1, NullType(), TimestampType(Timestamp(0, 1), 9),
                                         NullType(), NullType(), NullType(), NullType()},
                                        pool_.get());
    auto expected_max =
        BinaryRowGenerator::GenerateRow({20, 21, NullType(), TimestampType(Timestamp(20, 21), 9),
                                         NullType(), NullType(), NullType(), NullType()},
                                        pool_.get());

    CheckResultRow(*(new_stats.min_values), expected_min, schema->Fields());
    CheckResultRow(*(new_stats.max_values), expected_max, schema->Fields());
    CheckResultArray(*(new_stats.null_counts),
                     {0l, 0l, NullType(), 1l, NullType(), NullType(), NullType(), 4l});
}

TEST_F(SimpleStatsEvolutionTest, TestSchemaChangeWithNoDenseFields) {
    SimpleStatsEvolution evo(old_schema_->Fields(), new_schema_->Fields(), /*need_mapping=*/true,
                             pool_);

    SimpleStats stats = CreateStats();
    SimpleStats expected_stats = CreateNewStats();

    ASSERT_OK_AND_ASSIGN(SimpleStatsEvolution::EvolutionStats result_stats,
                         evo.Evolution(stats, /*row_count=*/4,
                                       /*dense_fields=*/std::nullopt));

    CheckResultRow(*(result_stats.min_values), expected_stats.min_values_, new_schema_->Fields());
    CheckResultRow(*(result_stats.max_values), expected_stats.max_values_, new_schema_->Fields());
    CheckResultArray(*(result_stats.null_counts), {1l, 0l, 0l, 1l, 0l, 0l, 0l, 4l});
}

TEST_F(SimpleStatsEvolutionTest, TestSchemaChangeWithDenseFields) {
    SimpleStatsEvolution evo(old_schema_->Fields(), new_schema_->Fields(), /*need_mapping=*/true,
                             pool_);

    auto min = BinaryRowGenerator::GenerateRow({std::string("no"), 10, 1234l}, pool_.get());
    auto max = BinaryRowGenerator::GenerateRow({std::string("yes"), 20, 2234l}, pool_.get());
    auto null_count = BinaryArray::FromLongArray({0, 1, 1}, pool_.get());
    SimpleStats old_stats(min, max, null_count);

    ASSERT_OK_AND_ASSIGN(
        SimpleStatsEvolution::EvolutionStats result_stats,
        evo.Evolution(old_stats, /*row_count=*/4,
                      /*dense_fields=*/std::vector<std::string>({"f3", "f4", "f5"})));

    auto expected_min = BinaryRowGenerator::GenerateRow(
        {10, NullType(), NullType(), NullType(), NullType(), NullType(), false, NullType()},
        pool_.get());
    auto expected_max = BinaryRowGenerator::GenerateRow(
        {20, NullType(), NullType(), NullType(), NullType(), NullType(), true, NullType()},
        pool_.get());

    CheckResultRow(*(result_stats.min_values), expected_min, new_schema_->Fields());
    CheckResultRow(*(result_stats.max_values), expected_max, new_schema_->Fields());
    CheckResultArray(*(result_stats.null_counts),
                     {1l, NullType(), NullType(), NullType(), NullType(), NullType(), 0l, 4l});
}

TEST_F(SimpleStatsEvolutionTest, TestNoSchemaChangeWithDenseFieldsWithMap) {
    std::string table_root =
        paimon::test::GetDataDir() +
        "orc/append_table_alter_table_with_cast.db/append_table_alter_table_with_cast";
    SchemaManager manager(fs_, table_root);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(/*schema_id=*/0));
    ASSERT_TRUE(schema);

    SimpleStatsEvolution evo(schema->Fields(), schema->Fields(), /*need_mapping=*/false, pool_);
    {
        // first turn
        auto min = BinaryRowGenerator::GenerateRow(
            {0, 1, TimestampType(Timestamp(0, 1), 9), NullType()}, pool_.get());
        auto max = BinaryRowGenerator::GenerateRow(
            {20, 21, TimestampType(Timestamp(20, 21), 9), NullType()}, pool_.get());
        auto null_count = BinaryArray::FromLongArray({0, 0, 1, 4}, pool_.get());
        SimpleStats stats(min, max, null_count);

        ASSERT_OK_AND_ASSIGN(
            SimpleStatsEvolution::EvolutionStats new_stats,
            evo.Evolution(stats, /*row_count=*/4,
                          /*dense_fields=*/std::vector<std::string>({"key0", "key1", "f1", "f5"})));

        auto expected_min =
            BinaryRowGenerator::GenerateRow({0, 1, NullType(), TimestampType(Timestamp(0, 1), 9),
                                             NullType(), NullType(), NullType(), NullType()},
                                            pool_.get());
        auto expected_max = BinaryRowGenerator::GenerateRow(
            {20, 21, NullType(), TimestampType(Timestamp(20, 21), 9), NullType(), NullType(),
             NullType(), NullType()},
            pool_.get());

        CheckResultRow(*(new_stats.min_values), expected_min, schema->Fields());
        CheckResultRow(*(new_stats.max_values), expected_max, schema->Fields());
        CheckResultArray(*(new_stats.null_counts),
                         {0l, 0l, NullType(), 1l, NullType(), NullType(), NullType(), 4l});
        ASSERT_EQ(evo.dense_fields_mapping_.Size(), 1);
    }
    {
        // second turn, evo with same dense_fields
        auto min = BinaryRowGenerator::GenerateRow(
            {0, 1, TimestampType(Timestamp(0, 1), 9), NullType()}, pool_.get());
        auto max = BinaryRowGenerator::GenerateRow(
            {20, 21, TimestampType(Timestamp(20, 21), 9), NullType()}, pool_.get());
        auto null_count = BinaryArray::FromLongArray({0, 0, 1, 4}, pool_.get());
        SimpleStats stats(min, max, null_count);
        ASSERT_OK_AND_ASSIGN(
            SimpleStatsEvolution::EvolutionStats new_stats,
            evo.Evolution(stats, /*row_count=*/4,
                          /*dense_fields=*/std::vector<std::string>({"key0", "key1", "f1", "f5"})));

        auto expected_min =
            BinaryRowGenerator::GenerateRow({0, 1, NullType(), TimestampType(Timestamp(0, 1), 9),
                                             NullType(), NullType(), NullType(), NullType()},
                                            pool_.get());
        auto expected_max = BinaryRowGenerator::GenerateRow(
            {20, 21, NullType(), TimestampType(Timestamp(20, 21), 9), NullType(), NullType(),
             NullType(), NullType()},
            pool_.get());

        CheckResultRow(*(new_stats.min_values), expected_min, schema->Fields());
        CheckResultRow(*(new_stats.max_values), expected_max, schema->Fields());
        CheckResultArray(*(new_stats.null_counts),
                         {0l, 0l, NullType(), 1l, NullType(), NullType(), NullType(), 4l});
        ASSERT_EQ(evo.dense_fields_mapping_.Size(), 1);
    }
    {
        // third turn, evo with different dense_fields
        auto min = BinaryRowGenerator::GenerateRow({TimestampType(Timestamp(0, 1), 9), NullType()},
                                                   pool_.get());
        auto max = BinaryRowGenerator::GenerateRow(
            {TimestampType(Timestamp(20, 21), 9), NullType()}, pool_.get());
        auto null_count = BinaryArray::FromLongArray({1, 4}, pool_.get());
        SimpleStats stats(min, max, null_count);
        ASSERT_OK_AND_ASSIGN(
            SimpleStatsEvolution::EvolutionStats new_stats,
            evo.Evolution(stats, /*row_count=*/4,
                          /*dense_fields=*/std::vector<std::string>({"f1", "f5"})));

        auto expected_min = BinaryRowGenerator::GenerateRow(
            {NullType(), NullType(), NullType(), TimestampType(Timestamp(0, 1), 9), NullType(),
             NullType(), NullType(), NullType()},
            pool_.get());
        auto expected_max = BinaryRowGenerator::GenerateRow(
            {NullType(), NullType(), NullType(), TimestampType(Timestamp(20, 21), 9), NullType(),
             NullType(), NullType(), NullType()},
            pool_.get());

        CheckResultRow(*(new_stats.min_values), expected_min, schema->Fields());
        CheckResultRow(*(new_stats.max_values), expected_max, schema->Fields());
        CheckResultArray(*(new_stats.null_counts), {NullType(), NullType(), NullType(), 1l,
                                                    NullType(), NullType(), NullType(), 4l});
        ASSERT_EQ(evo.dense_fields_mapping_.Size(), 2);
    }
}

TEST_F(SimpleStatsEvolutionTest, TestGetFieldMap) {
    std::string table_root =
        paimon::test::GetDataDir() +
        "orc/append_table_alter_table_with_cast.db/append_table_alter_table_with_cast";
    SchemaManager manager(fs_, table_root);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(/*schema_id=*/0));
    ASSERT_TRUE(schema);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema1, manager.ReadSchema(/*schema_id=*/1));
    ASSERT_TRUE(schema1);

    SimpleStatsEvolution evo(schema->Fields(), schema1->Fields(), /*need_mapping=*/true, pool_);

    std::map<int32_t, std::pair<int32_t, DataField>> expected_id_to_data_fields;
    expected_id_to_data_fields[0] = std::make_pair(0, schema->GetField("key0").value());
    expected_id_to_data_fields[1] = std::make_pair(1, schema->GetField("key1").value());
    expected_id_to_data_fields[2] = std::make_pair(2, schema->GetField("f0").value());
    expected_id_to_data_fields[3] = std::make_pair(3, schema->GetField("f1").value());
    expected_id_to_data_fields[4] = std::make_pair(4, schema->GetField("f2").value());
    expected_id_to_data_fields[5] = std::make_pair(5, schema->GetField("f3").value());
    expected_id_to_data_fields[6] = std::make_pair(6, schema->GetField("f4").value());
    expected_id_to_data_fields[7] = std::make_pair(7, schema->GetField("f5").value());

    std::map<std::string, DataField> expected_name_to_table_fields;
    expected_name_to_table_fields["f4"] = schema1->GetField("f4").value();
    expected_name_to_table_fields["key0"] = schema1->GetField("key0").value();
    expected_name_to_table_fields["key1"] = schema1->GetField("key1").value();
    expected_name_to_table_fields["f3"] = schema1->GetField("f3").value();
    expected_name_to_table_fields["f1"] = schema1->GetField("f1").value();
    expected_name_to_table_fields["f2"] = schema1->GetField("f2").value();
    expected_name_to_table_fields["f0"] = schema1->GetField("f0").value();
    expected_name_to_table_fields["f6"] = schema1->GetField("f6").value();

    ASSERT_EQ(expected_id_to_data_fields, evo.GetFieldIdToDataField());
    ASSERT_EQ(expected_name_to_table_fields, evo.GetFieldNameToTableField());
}

}  // namespace paimon::test
