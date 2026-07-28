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

#include "paimon/data/shredding/map_shared_shredding_schema_utils.h"

#include <string>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "gtest/gtest.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(MapSharedShreddingSchemaUtilsTest, AttachMetadataToSchemaBasic) {
    MapSharedShreddingFieldMeta tags_meta;
    tags_meta.name_to_id = {{"host", 0}, {"region", 1}};
    tags_meta.field_to_columns = {{0, {0}}, {1, {1}}};
    tags_meta.num_columns = 2;
    tags_meta.max_row_width = 2;

    auto id_metadata = std::make_shared<arrow::KeyValueMetadata>();
    id_metadata->Append("paimon.field.id", "1");
    auto schema_metadata = std::make_shared<arrow::KeyValueMetadata>();
    schema_metadata->Append("schema.key", "schema.value");
    auto schema = arrow::schema(
        {arrow::field("id", arrow::int32(), true, id_metadata),
         arrow::field("tags", arrow::struct_({arrow::field("__field_mapping",
                                                           arrow::list(arrow::int32()), true),
                                              arrow::field("__col_0", arrow::utf8(), true),
                                              arrow::field("__col_1", arrow::utf8(), true)}))},
        schema_metadata);

    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
    ASSERT_OK_AND_ASSIGN(auto c_updated_schema,
                         MapSharedShreddingSchemaUtils::AttachMetadataToSchema(
                             std::move(c_schema), {{"tags", tags_meta}}, "none"));
    ASSERT_TRUE(c_updated_schema);
    ASSERT_TRUE(c_updated_schema->release);
    auto updated_schema = arrow::ImportSchema(c_updated_schema.get()).ValueOrDie();

    ASSERT_TRUE(updated_schema->metadata()->Equals(*schema_metadata));
    ASSERT_TRUE(updated_schema->field(0)->metadata()->Equals(*id_metadata));

    auto tags_metadata = updated_schema->GetFieldByName("tags")->metadata()->Copy();
    ASSERT_TRUE(MapSharedShreddingUtils::HasShreddingMetadata(tags_metadata));
    ASSERT_OK_AND_ASSIGN(auto deserialized,
                         MapSharedShreddingUtils::DeserializeMetadata(tags_metadata));
    ASSERT_EQ(deserialized, tags_meta);
}

TEST(MapSharedShreddingSchemaUtilsTest, AttachMetadataToSchemaInvalidInput) {
    ASSERT_NOK_WITH_MSG(MapSharedShreddingSchemaUtils::AttachMetadataToSchema(
                            std::unique_ptr<::ArrowSchema>(), {}, "none"),
                        "physical schema is null");

    MapSharedShreddingFieldMeta tags_meta;
    tags_meta.name_to_id = {{"host", 0}};
    tags_meta.field_to_columns = {{0, {0}}};
    tags_meta.num_columns = 1;
    tags_meta.max_row_width = 1;

    auto schema = arrow::schema({arrow::field("id", arrow::int32())});

    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
    ASSERT_NOK_WITH_MSG(MapSharedShreddingSchemaUtils::AttachMetadataToSchema(
                            std::move(c_schema), {{"tags", tags_meta}}, "none"),
                        "Shared-shredding field 'tags' not found in physical schema.");
}

TEST(MapSharedShreddingSchemaUtilsTest, AttachMetadataToSchemaPreservesExistingFieldMetadata) {
    MapSharedShreddingFieldMeta tags_meta;
    tags_meta.name_to_id = {{"host", 0}};
    tags_meta.field_to_columns = {{0, {0}}};
    tags_meta.num_columns = 1;
    tags_meta.max_row_width = 1;

    auto tags_metadata = std::make_shared<arrow::KeyValueMetadata>();
    tags_metadata->Append("paimon.field.id", "7");
    tags_metadata->Append("description", "original tags field");
    auto schema = arrow::schema({arrow::field(
        "tags",
        arrow::struct_({arrow::field("__field_mapping", arrow::list(arrow::int32()), true),
                        arrow::field("__col_0", arrow::utf8(), true)}),
        true, tags_metadata)});

    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
    ASSERT_OK_AND_ASSIGN(auto c_updated_schema,
                         MapSharedShreddingSchemaUtils::AttachMetadataToSchema(
                             std::move(c_schema), {{"tags", tags_meta}}, "none"));
    ASSERT_TRUE(c_updated_schema);
    ASSERT_TRUE(c_updated_schema->release);
    auto updated_schema = arrow::ImportSchema(c_updated_schema.get()).ValueOrDie();

    auto updated_metadata = updated_schema->field(0)->metadata()->Copy();
    ASSERT_EQ(updated_metadata->value(updated_metadata->FindKey("paimon.field.id")), "7");
    ASSERT_EQ(updated_metadata->value(updated_metadata->FindKey("description")),
              "original tags field");
    ASSERT_TRUE(MapSharedShreddingUtils::HasShreddingMetadata(updated_metadata));
}

TEST(MapSharedShreddingSchemaUtilsTest, AttachMetadataToSchemaOverwritesExistingShreddingMetadata) {
    MapSharedShreddingFieldMeta old_meta;
    old_meta.name_to_id = {{"old", 0}};
    old_meta.field_to_columns = {{0, {0}}};
    old_meta.num_columns = 1;
    old_meta.max_row_width = 1;

    MapSharedShreddingFieldMeta tags_meta;
    tags_meta.name_to_id = {{"host", 0}, {"region", 1}};
    tags_meta.field_to_columns = {{0, {0}}, {1, {1}}};
    tags_meta.overflow_field_set = {1};
    tags_meta.num_columns = 2;
    tags_meta.max_row_width = 2;

    auto tags_metadata = std::make_shared<arrow::KeyValueMetadata>();
    tags_metadata->Append("paimon.field.id", "7");
    ASSERT_OK(MapSharedShreddingUtils::SerializeMetadata(old_meta, "none", tags_metadata.get()));
    auto schema = arrow::schema({arrow::field(
        "tags",
        arrow::struct_({arrow::field("__field_mapping", arrow::list(arrow::int32()), true),
                        arrow::field("__col_0", arrow::utf8(), true),
                        arrow::field("__col_1", arrow::utf8(), true)}),
        true, tags_metadata)});

    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
    ASSERT_OK_AND_ASSIGN(auto c_updated_schema,
                         MapSharedShreddingSchemaUtils::AttachMetadataToSchema(
                             std::move(c_schema), {{"tags", tags_meta}}, "none"));
    auto updated_schema = arrow::ImportSchema(c_updated_schema.get()).ValueOrDie();
    auto updated_metadata = updated_schema->field(0)->metadata()->Copy();

    ASSERT_EQ(updated_metadata->value(updated_metadata->FindKey("paimon.field.id")), "7");
    int32_t storage_layout_key_count = 0;
    for (const auto& key : updated_metadata->keys()) {
        if (key == MapShreddingDefine::kStorageLayout) {
            ++storage_layout_key_count;
        }
    }
    ASSERT_EQ(storage_layout_key_count, 1);
    ASSERT_OK_AND_ASSIGN(auto deserialized,
                         MapSharedShreddingUtils::DeserializeMetadata(updated_metadata));
    ASSERT_EQ(deserialized, tags_meta);
}

TEST(MapSharedShreddingSchemaUtilsTest, ExtractMetadataFromField) {
    MapSharedShreddingFieldMeta tags_meta;
    tags_meta.name_to_id = {{"host", 0}, {"region", 1}};
    tags_meta.field_to_columns = {{0, {0}}, {1, {1}}};
    tags_meta.num_columns = 2;
    tags_meta.max_row_width = 2;

    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    ASSERT_OK(MapSharedShreddingUtils::SerializeMetadata(tags_meta, "none", metadata.get()));
    auto field = arrow::field(
        "tags",
        arrow::struct_({arrow::field("__field_mapping", arrow::list(arrow::int32()), true),
                        arrow::field("__col_0", arrow::utf8(), true),
                        arrow::field("__col_1", arrow::utf8(), true)}),
        true, metadata);
    auto schema = arrow::schema({arrow::field("id", arrow::int32()), field});

    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
    ASSERT_OK_AND_ASSIGN(auto parsed_meta, MapSharedShreddingSchemaUtils::ExtractMetadataFromField(
                                               std::move(c_schema), "tags"));
    ASSERT_EQ(parsed_meta, tags_meta);
}

TEST(MapSharedShreddingSchemaUtilsTest, ExtractMetadataFromFieldNoShreddingMetadata) {
    auto schema =
        arrow::schema({arrow::field("id", arrow::int32()), arrow::field("tags", arrow::utf8())});

    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());

    ASSERT_NOK_WITH_MSG(
        MapSharedShreddingSchemaUtils::ExtractMetadataFromField(std::move(c_schema), "tags"),
        "metadata is null or storage layout is not shared-shredding");
}

TEST(MapSharedShreddingSchemaUtilsTest, ExtractMetadataFromFieldInvalidInput) {
    ASSERT_NOK_WITH_MSG(MapSharedShreddingSchemaUtils::ExtractMetadataFromField(
                            std::unique_ptr<::ArrowSchema>(), "tags"),
                        "physical schema is null");

    auto schema = arrow::schema({arrow::field("id", arrow::int32())});

    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());

    ASSERT_NOK_WITH_MSG(
        MapSharedShreddingSchemaUtils::ExtractMetadataFromField(std::move(c_schema), "tags"),
        "Shared-shredding field 'tags' not found in physical schema.");
}

TEST(MapSharedShreddingSchemaUtilsTest, LogicalToPhysicalSchemaInvalidInput) {
    std::map<std::string, int32_t> field_to_num_columns = {{"tags", 2}};

    ASSERT_NOK_WITH_MSG(MapSharedShreddingSchemaUtils::LogicalToPhysicalSchema(
                            std::unique_ptr<::ArrowSchema>(), field_to_num_columns),
                        "logical schema is null");

    auto schema =
        arrow::schema({arrow::field("ts", arrow::int64()), arrow::field("tags", arrow::int32())});

    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());

    ASSERT_NOK_WITH_MSG(MapSharedShreddingSchemaUtils::LogicalToPhysicalSchema(
                            std::move(c_schema), field_to_num_columns),
                        "Field 'tags' is expected to be MAP type");
}

TEST(MapSharedShreddingSchemaUtilsTest, LogicalToPhysicalSchemaNestedListValue) {
    // MAP<STRING, STRUCT<a:array<int32>, b: array<int32>, c: array<int32>>>
    auto nested_value = arrow::struct_({arrow::field("a", arrow::list(arrow::int32())),
                                        arrow::field("b", arrow::list(arrow::int32())),
                                        arrow::field("c", arrow::list(arrow::int32()))});
    auto map_type = arrow::map(arrow::utf8(), nested_value);
    auto schema =
        arrow::schema({arrow::field("ts", arrow::int64()), arrow::field("data", map_type)});

    std::map<std::string, int32_t> field_to_num_columns = {{"data", 3}};
    auto c_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
    ASSERT_OK_AND_ASSIGN(auto c_physical_schema,
                         MapSharedShreddingSchemaUtils::LogicalToPhysicalSchema(
                             std::move(c_schema), field_to_num_columns));
    ASSERT_TRUE(c_physical_schema);
    ASSERT_TRUE(c_physical_schema->release);
    auto physical_schema = arrow::ImportSchema(c_physical_schema.get()).ValueOrDie();

    auto expected_struct = arrow::struct_({
        arrow::field("__field_mapping", arrow::list(arrow::int32()), true),
        arrow::field("__col_0", nested_value, true),
        arrow::field("__col_1", nested_value, true),
        arrow::field("__col_2", nested_value, true),
        arrow::field("__overflow", arrow::map(arrow::int32(), nested_value), true),
    });
    auto expected_schema = arrow::schema(
        {arrow::field("ts", arrow::int64()), arrow::field("data", expected_struct, true)});
    ASSERT_TRUE(physical_schema->Equals(expected_schema));
}

}  // namespace paimon::test
