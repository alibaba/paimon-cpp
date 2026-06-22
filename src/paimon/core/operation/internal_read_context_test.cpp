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

#include "paimon/core/operation/internal_read_context.h"

#include <utility>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(InternalReadContext, TestReadWithUnspecifiedSchema) {
    // no read schema is specified, read all fields
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithSpecifiedSchema) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"f3", "f0"});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithSpecifiedFieldId) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldIds({3, 0});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithSpecifiedFieldIdAndSchema) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    // read schema is specified, read fields in schema
    // will use field ids instead of field names.
    context_builder.SetReadFieldNames({"f0"});
    context_builder.SetReadFieldIds({3, 0});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithRowTrackingAndScoreFields) {
    {
        // test simple
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames(
            {"f3", "f0", "_ROW_ID", "_SEQUENCE_NUMBER", "_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        auto new_options = table_schema->Options();
        new_options[Options::ROW_TRACKING_ENABLED] = "true";
        new_options[Options::DATA_EVOLUTION_ENABLED] = "true";
        ASSERT_OK_AND_ASSIGN(
            auto internal_context,
            InternalReadContext::Create(std::move(read_context), table_schema, new_options));
        std::vector<DataField> read_fields = {
            DataField(3, arrow::field("f3", arrow::float64())),
            DataField(0, arrow::field("f0", arrow::utf8())), SpecialFields::RowId(),
            SpecialFields::SequenceNumber(), SpecialFields::IndexScore()};
        auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
        ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
    }
    {
        // test invalid case: disable row tracking while read row tracking fields
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f3", "f0", "_ROW_ID", "_SEQUENCE_NUMBER"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        ASSERT_NOK_WITH_MSG(InternalReadContext::Create(std::move(read_context), table_schema,
                                                        table_schema->Options()),
                            "Get field _ROW_ID failed: not exist in table schema");
    }
    {
        // test invalid case: disable data evolution while read score fields
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f3", "f0", "_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        ASSERT_NOK_WITH_MSG(InternalReadContext::Create(std::move(read_context), table_schema,
                                                        table_schema->Options()),
                            "Get field _INDEX_SCORE failed: not exist in table schema");
    }
}

TEST(InternalReadContext, TestReadWithValueKindField) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"f3", "_VALUE_KIND", "f0"});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithFieldIdsAndSpecialFields) {
    {
        // test simple
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        // here we use field ids instead of field names, and specify special ids for row id,
        // sequence number and index score.
        context_builder.SetReadFieldIds({3, 0, SpecialFieldIds::ROW_ID,
                                         SpecialFieldIds::SEQUENCE_NUMBER,
                                         SpecialFieldIds::INDEX_SCORE});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        auto new_options = table_schema->Options();
        new_options[Options::ROW_TRACKING_ENABLED] = "true";
        new_options[Options::DATA_EVOLUTION_ENABLED] = "true";
        ASSERT_OK_AND_ASSIGN(
            auto internal_context,
            InternalReadContext::Create(std::move(read_context), table_schema, new_options));
        std::vector<DataField> read_fields = {
            DataField(3, arrow::field("f3", arrow::float64())),
            DataField(0, arrow::field("f0", arrow::utf8())), SpecialFields::RowId(),
            SpecialFields::SequenceNumber(), SpecialFields::IndexScore()};
        auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
        ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
    }
}

TEST(InternalReadContext, TestReadWithProjectedSchemaAndSpecialFields) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";

    std::vector<DataField> projected_fields = {
        DataField(0, arrow::field("f0", arrow::utf8())), SpecialFields::RowId(),
        SpecialFields::SequenceNumber(), SpecialFields::IndexScore()};
    auto schema_manager = SchemaManager(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

    // Without options, special fields should be rejected in projected-schema path too.
    {
        auto projected_schema = DataField::ConvertDataFieldsToArrowSchema(projected_fields);
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema(&c_schema);
        ASSERT_OK_AND_ASSIGN(auto unique_read_context, context_builder.Finish());
        std::shared_ptr<ReadContext> read_context = std::move(unique_read_context);
        ASSERT_NOK_WITH_MSG(
            InternalReadContext::Create(read_context, table_schema, table_schema->Options()),
            "not exist in table schema");
    }

    // With options enabled, projected-schema path should accept these special fields.
    auto enabled_options = table_schema->Options();
    enabled_options[Options::ROW_TRACKING_ENABLED] = "true";
    enabled_options[Options::DATA_EVOLUTION_ENABLED] = "true";

    {
        auto projected_schema = DataField::ConvertDataFieldsToArrowSchema(projected_fields);
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema(&c_schema);
        ASSERT_OK_AND_ASSIGN(auto unique_read_context, context_builder.Finish());
        std::shared_ptr<ReadContext> read_context = std::move(unique_read_context);
        ASSERT_OK_AND_ASSIGN(
            auto internal_context,
            InternalReadContext::Create(read_context, table_schema, enabled_options));
        auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(projected_fields);
        ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
    }
}

TEST(InternalReadContext, TestReadWithProjectedSchemaWithoutFieldIds) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";

    auto projected_schema =
        arrow::schema({arrow::field("f3", arrow::float64()), arrow::field("f0", arrow::utf8())});
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema(&c_schema);
    ASSERT_OK_AND_ASSIGN(auto unique_read_context, context_builder.Finish());
    std::shared_ptr<ReadContext> read_context = std::move(unique_read_context);

    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

    ASSERT_OK_AND_ASSIGN(
        auto internal_context,
        InternalReadContext::Create(read_context, table_schema, table_schema->Options()));

    std::vector<DataField> expected_fields = {
        DataField(3, arrow::field("f3", arrow::float64())),
        DataField(0, arrow::field("f0", arrow::utf8())),
    };
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(expected_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema, /*check_metadata=*/true));
}

TEST(InternalReadContext, TestProjectedSchemaMetadataWhitelist) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";

    auto read_field = arrow::field("f0", arrow::utf8())
                          ->WithMetadata(arrow::KeyValueMetadata::Make(
                              {DataField::MAP_SELECTED_KEYS, "custom.key"},
                              {"k1,k2", "should_not_propagate"}));
    auto projected_schema = arrow::schema({read_field});
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &c_schema).ok());

    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema(&c_schema);
    ASSERT_OK_AND_ASSIGN(auto unique_read_context, context_builder.Finish());
    std::shared_ptr<ReadContext> read_context = std::move(unique_read_context);

    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

    ASSERT_OK_AND_ASSIGN(
        auto internal_context,
        InternalReadContext::Create(read_context, table_schema, table_schema->Options()));

    auto aligned_field = internal_context->GetReadSchema()->GetFieldByName("f0");
    ASSERT_TRUE(aligned_field);
    ASSERT_TRUE(aligned_field->HasMetadata());
    ASSERT_TRUE(aligned_field->metadata());

    auto selected_keys_result = aligned_field->metadata()->Get(DataField::MAP_SELECTED_KEYS);
    ASSERT_TRUE(selected_keys_result.ok());
    ASSERT_EQ(selected_keys_result.ValueUnsafe(), "k1,k2");

    auto custom_metadata_result = aligned_field->metadata()->Get("custom.key");
    ASSERT_FALSE(custom_metadata_result.ok());
}

}  // namespace paimon::test
