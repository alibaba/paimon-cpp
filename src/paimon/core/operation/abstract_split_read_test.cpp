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

#include "paimon/core/operation/abstract_split_read.h"

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/io/chain_split_file_path_factory.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

Result<std::shared_ptr<TableSchema>> CreateTableForReadSchemaTest(
    const std::shared_ptr<FileSystem>& fs, const std::string& table_root, const std::string& branch,
    const std::string& field_name) {
    SchemaManager schema_manager(fs, table_root, branch);
    auto schema = arrow::schema({arrow::field(field_name, arrow::int32())});
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"}};
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableSchema> table_schema,
        schema_manager.CreateTable(schema, /*partition_keys=*/{}, /*primary_keys=*/{}, options));
    return std::shared_ptr<TableSchema>(std::move(table_schema));
}

std::shared_ptr<DataFileMeta> CreateDataFileMetaForReadSchemaTest(const std::string& file_name,
                                                                  int64_t schema_id) {
    return std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/1024, /*row_count=*/7, BinaryRow::EmptyRow(),
        BinaryRow::EmptyRow(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/6, schema_id, DataFileMeta::DUMMY_LEVEL,
        std::vector<std::optional<std::string>>(), Timestamp(0, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
}

class AbstractSplitReadForTest : public AbstractSplitRead {
 public:
    AbstractSplitReadForTest(const std::shared_ptr<InternalReadContext>& context,
                             std::unique_ptr<SchemaManager>&& schema_manager)
        : AbstractSplitRead(/*path_factory=*/nullptr, context, std::move(schema_manager),
                            GetDefaultPool(), /*executor=*/nullptr) {}

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<Split>& split) override {
        (void)split;
        return Status::NotImplemented("not used");
    }

    Result<bool> Match(const std::shared_ptr<Split>& split, bool force_keep_delete) const override {
        (void)split;
        (void)force_keep_delete;
        return false;
    }

 private:
    Result<std::unique_ptr<FileBatchReader>> ApplyIndexAndDvReaderIfNeeded(
        std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
        const std::shared_ptr<arrow::Schema>& data_schema,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate, DeletionVector::Factory dv_factory,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const override {
        return std::move(file_reader);
    }
};

}  // namespace

TEST(AbstractSplitReadTest, TestNeedCompleteRowTrackingFields) {
    std::vector<DataField> data_fields = {DataField(0, arrow::field("name", arrow::utf8())),
                                          DataField(1, arrow::field("sex", arrow::utf8())),
                                          DataField(2, arrow::field("age", arrow::int32())),
                                          SpecialFields::RowId(), SpecialFields::SequenceNumber()};
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(data_fields);
    auto fields = arrow_schema->fields();

    ASSERT_TRUE(AbstractSplitRead::NeedCompleteRowTrackingFields(/*row_tracking_enabled=*/true,
                                                                 arrow::schema(fields)));
    ASSERT_TRUE(AbstractSplitRead::NeedCompleteRowTrackingFields(
        /*row_tracking_enabled=*/true, arrow::schema({fields[0], fields[3]})));
    ASSERT_TRUE(AbstractSplitRead::NeedCompleteRowTrackingFields(
        /*row_tracking_enabled=*/true, arrow::schema({fields[0], fields[4]})));
    ASSERT_FALSE(AbstractSplitRead::NeedCompleteRowTrackingFields(
        /*row_tracking_enabled=*/true, arrow::schema({fields[0], fields[1]})));
    ASSERT_FALSE(AbstractSplitRead::NeedCompleteRowTrackingFields(/*row_tracking_enabled=*/false,
                                                                  arrow::schema(fields)));
}

TEST(AbstractSplitReadTest, TestProjectFieldsForRowTrackingAndDataEvolution) {
    {
        // test no partition
        std::vector<DataField> fields = {DataField(0, arrow::field("name", arrow::utf8())),
                                         DataField(1, arrow::field("sex", arrow::utf8())),
                                         DataField(2, arrow::field("age", arrow::int32()))};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, DataField::ConvertDataFieldsToArrowSchema(fields),
                                /*partition_keys=*/{},
                                /*primary_keys=*/{}, /*options=*/{}));

        {
            // test write_cols is std::nullopt
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, /*write_cols=*/std::nullopt));
            std::vector<DataField> expected = fields;
            expected.push_back(SpecialFields::RowId());
            expected.push_back(SpecialFields::SequenceNumber());
            ASSERT_EQ(result, expected);
        }
        {
            // test with write_cols
            std::vector<std::string> write_cols = {"name", "age"};
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, write_cols));
            std::vector<DataField> expected = {fields[0], fields[2], SpecialFields::RowId(),
                                               SpecialFields::SequenceNumber()};
            ASSERT_EQ(result, expected);
        }
        {
            // test with empty write_cols
            std::vector<std::string> write_cols = {};
            ASSERT_NOK_WITH_MSG(AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                    table_schema, write_cols),
                                "write cols cannot be empty");
        }
    }
    {
        // test with partition
        std::vector<DataField> fields = {DataField(0, arrow::field("name", arrow::utf8())),
                                         DataField(1, arrow::field("ds", arrow::utf8())),
                                         DataField(2, arrow::field("sex", arrow::utf8())),
                                         DataField(3, arrow::field("age", arrow::int32()))};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, DataField::ConvertDataFieldsToArrowSchema(fields),
                                /*partition_keys=*/{"ds"},
                                /*primary_keys=*/{}, /*options=*/{}));

        {
            // test write_cols is std::nullopt
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, /*write_cols=*/std::nullopt));
            std::vector<DataField> expected = fields;
            expected.push_back(SpecialFields::RowId());
            expected.push_back(SpecialFields::SequenceNumber());
            ASSERT_EQ(result, expected);
        }
        {
            // test with write_cols and write_cols not contain partition fields
            std::vector<std::string> write_cols = {"name", "age"};
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, write_cols));
            std::vector<DataField> expected = {fields[0], fields[3], fields[1],
                                               SpecialFields::RowId(),
                                               SpecialFields::SequenceNumber()};
            ASSERT_EQ(result, expected);
        }
        {
            // test with write_cols and write_cols contain partition fields
            std::vector<std::string> write_cols = {"age", "name", "ds"};
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, write_cols));
            std::vector<DataField> expected = {fields[3], fields[0], fields[1],
                                               SpecialFields::RowId(),
                                               SpecialFields::SequenceNumber()};
            ASSERT_EQ(result, expected);
        }
        {
            // test with write_cols and write_cols contain row tracking fields
            std::vector<std::string> write_cols = {
                "age",
                "name",
                SpecialFields::RowId().Name(),
                SpecialFields::SequenceNumber().Name(),
            };
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, write_cols));
            std::vector<DataField> expected = {fields[3], fields[0], fields[1],
                                               SpecialFields::RowId(),
                                               SpecialFields::SequenceNumber()};
            ASSERT_EQ(result, expected);
        }
        {
            // test with empty write_cols
            std::vector<std::string> write_cols = {};
            ASSERT_NOK_WITH_MSG(AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                    table_schema, write_cols),
                                "write cols cannot be empty");
        }
    }
}

TEST(AbstractSplitReadTest, ReadDataSchemaUsesChainSplitFileBranchMapping) {
    auto dir = UniqueTestDirectory::Create();
    auto fs = dir->GetFileSystem();
    const std::string table_root = dir->Str();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> main_schema,
                         CreateTableForReadSchemaTest(fs, table_root, "main", "main_field"));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<TableSchema> delta_schema,
                         CreateTableForReadSchemaTest(fs, table_root, "delta", "delta_field"));

    ReadContextBuilder context_builder(table_root);
    context_builder.WithFileSystem(fs);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<InternalReadContext> internal_context,
        InternalReadContext::Create(std::move(read_context), main_schema, main_schema->Options()));
    AbstractSplitReadForTest split_read(internal_context,
                                        std::make_unique<SchemaManager>(fs, table_root, "main"));

    const std::string file_name = "data-branch-aware.parquet";
    auto file_meta = CreateDataFileMetaForReadSchemaTest(file_name, /*schema_id=*/0);
    auto chain_path_factory = std::make_shared<ChainSplitFilePathFactory>(
        std::unordered_map<std::string, std::string>{{file_name, "bucket-0"}},
        std::unordered_map<std::string, std::string>{{file_name, "delta"}});

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> data_schema,
                         split_read.ReadDataSchema(file_meta, chain_path_factory));
    EXPECT_EQ(data_schema->FieldNames(), std::vector<std::string>({"delta_field"}));
}

TEST(AbstractSplitReadTest, ReadDataSchemaRejectsChainSplitMissingBranchMapping) {
    auto dir = UniqueTestDirectory::Create();
    auto fs = dir->GetFileSystem();
    const std::string table_root = dir->Str();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> main_schema,
                         CreateTableForReadSchemaTest(fs, table_root, "main", "main_field"));

    ReadContextBuilder context_builder(table_root);
    context_builder.WithFileSystem(fs);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<InternalReadContext> internal_context,
        InternalReadContext::Create(std::move(read_context), main_schema, main_schema->Options()));
    AbstractSplitReadForTest split_read(internal_context,
                                        std::make_unique<SchemaManager>(fs, table_root, "main"));

    const std::string file_name = "data-missing-branch.parquet";
    auto file_meta = CreateDataFileMetaForReadSchemaTest(file_name, /*schema_id=*/0);
    auto chain_path_factory = std::make_shared<ChainSplitFilePathFactory>(
        std::unordered_map<std::string, std::string>{{file_name, "bucket-0"}});

    ASSERT_NOK_WITH_MSG(split_read.ReadDataSchema(file_meta, chain_path_factory),
                        "branch is missing for ChainSplit file");
}

}  // namespace paimon::test
