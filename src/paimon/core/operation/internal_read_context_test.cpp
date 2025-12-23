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
    context_builder.SetReadSchema({"f3", "f0"});
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

TEST(InternalReadContext, TestReadWithRowTrackingAndScoreFields) {
    {
        // test simple
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema({"f3", "f0", "_ROW_ID", "_SEQUENCE_NUMBER", "_INDEX_SCORE"});
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
        context_builder.SetReadSchema({"f3", "f0", "_ROW_ID", "_SEQUENCE_NUMBER"});
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
        context_builder.SetReadSchema({"f3", "f0", "_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        ASSERT_NOK_WITH_MSG(InternalReadContext::Create(std::move(read_context), table_schema,
                                                        table_schema->Options()),
                            "Get field _INDEX_SCORE failed: not exist in table schema");
    }
}

}  // namespace paimon::test
