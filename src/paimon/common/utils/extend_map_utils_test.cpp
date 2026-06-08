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

#include "paimon/common/utils/extend_map_utils.h"

#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "gtest/gtest.h"
#include "paimon/core/core_options.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// ---- IsStringKeyMap ----

TEST(ExtendMapUtilsTest, IsStringKeyMap) {
    ASSERT_TRUE(ExtendMapUtils::IsStringKeyMap(arrow::map(arrow::utf8(), arrow::int32())));
    ASSERT_TRUE(ExtendMapUtils::IsStringKeyMap(arrow::map(arrow::utf8(), arrow::float64())));
    // Nested value type (struct)
    auto nested_value =
        arrow::struct_({arrow::field("x", arrow::int32()), arrow::field("y", arrow::utf8())});
    ASSERT_TRUE(ExtendMapUtils::IsStringKeyMap(arrow::map(arrow::utf8(), nested_value)));
    ASSERT_FALSE(ExtendMapUtils::IsStringKeyMap(arrow::map(arrow::int32(), arrow::utf8())));
    ASSERT_FALSE(ExtendMapUtils::IsStringKeyMap(arrow::int32()));
    ASSERT_FALSE(ExtendMapUtils::IsStringKeyMap(arrow::list(arrow::utf8())));
}

// ---- DetectExtendColumns ----

TEST(ExtendMapUtilsTest, DetectExtendColumnsBasic) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("metrics", arrow::map(arrow::utf8(), arrow::float64())),
        arrow::field("name", arrow::utf8()),
    });

    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{"fields.tags.map-storage-layout", "extend"},
                                               {"fields.metrics.map-storage-layout", "extend"}}));

    ASSERT_OK_AND_ASSIGN(auto indices, ExtendMapUtils::DetectExtendColumns(schema, options));
    ASSERT_EQ(indices.size(), 2);
    ASSERT_EQ(indices[0], 1);
    ASSERT_EQ(indices[1], 2);
}

TEST(ExtendMapUtilsTest, DetectExtendColumnsNoExtend) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
    });

    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    ASSERT_OK_AND_ASSIGN(auto indices, ExtendMapUtils::DetectExtendColumns(schema, options));
    ASSERT_TRUE(indices.empty());
}

TEST(ExtendMapUtilsTest, DetectExtendColumnsInvalidType) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("not_a_map", arrow::utf8()),
    });

    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{"fields.not_a_map.map-storage-layout", "extend"}}));

    ASSERT_NOK_WITH_MSG(ExtendMapUtils::DetectExtendColumns(schema, options), "not MAP<STRING, T>");
}

// ---- LogicalToPhysicalSchema ----

TEST(ExtendMapUtilsTest, LogicalToPhysicalSchemaBasic) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("name", arrow::utf8()),
    });

    std::map<int32_t, int32_t> column_to_num_columns = {{1, 4}};
    ASSERT_OK_AND_ASSIGN(auto physical_schema,
                         ExtendMapUtils::LogicalToPhysicalSchema(schema, column_to_num_columns));

    // Build expected schema for comparison
    auto expected_struct = arrow::struct_({
        arrow::field("__field_mapping", arrow::list(arrow::int32()), false),
        arrow::field("__col_0", arrow::utf8(), true),
        arrow::field("__col_1", arrow::utf8(), true),
        arrow::field("__col_2", arrow::utf8(), true),
        arrow::field("__col_3", arrow::utf8(), true),
        arrow::field("__overflow", arrow::map(arrow::int32(), arrow::utf8()), true),
    });
    auto expected_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", expected_struct, true),
        arrow::field("name", arrow::utf8()),
    });
    ASSERT_TRUE(physical_schema->Equals(expected_schema));
}

TEST(ExtendMapUtilsTest, LogicalToPhysicalSchemaNestedValue) {
    // MAP<STRING, STRUCT<a: int32, b: utf8>>
    auto nested_value =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    auto map_type = arrow::map(arrow::utf8(), nested_value);
    auto schema = arrow::schema({arrow::field("data", map_type)});

    std::map<int32_t, int32_t> column_to_num_columns = {{0, 2}};
    ASSERT_OK_AND_ASSIGN(auto physical_schema,
                         ExtendMapUtils::LogicalToPhysicalSchema(schema, column_to_num_columns));

    auto expected_struct = arrow::struct_({
        arrow::field("__field_mapping", arrow::list(arrow::int32()), false),
        arrow::field("__col_0", nested_value, true),
        arrow::field("__col_1", nested_value, true),
        arrow::field("__overflow", arrow::map(arrow::int32(), nested_value), true),
    });
    auto expected_schema = arrow::schema({arrow::field("data", expected_struct, true)});
    ASSERT_TRUE(physical_schema->Equals(expected_schema));
}

TEST(ExtendMapUtilsTest, LogicalToPhysicalSchemaNullable) {
    // MAP value is nullable
    auto nullable_map = arrow::map(arrow::utf8(), arrow::field("item", arrow::int64(), true));
    auto schema_nullable = arrow::schema({arrow::field("m", nullable_map)});
    std::map<int32_t, int32_t> col_map = {{0, 2}};

    ASSERT_OK_AND_ASSIGN(auto physical,
                         ExtendMapUtils::LogicalToPhysicalSchema(schema_nullable, col_map));
    auto struct_type = physical->field(0)->type();
    ASSERT_TRUE(struct_type->field(1)->nullable());
    ASSERT_TRUE(struct_type->field(2)->nullable());

    // MAP value is non-nullable
    auto non_nullable_map = arrow::map(arrow::utf8(), arrow::field("item", arrow::int64(), false));
    auto schema_non_nullable = arrow::schema({arrow::field("m", non_nullable_map)});

    ASSERT_OK_AND_ASSIGN(auto physical2,
                         ExtendMapUtils::LogicalToPhysicalSchema(schema_non_nullable, col_map));
    auto struct_type2 = physical2->field(0)->type();
    ASSERT_FALSE(struct_type2->field(1)->nullable());
    ASSERT_FALSE(struct_type2->field(2)->nullable());
}

TEST(ExtendMapUtilsTest, LogicalToPhysicalSchemaNoExtendColumns) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("name", arrow::utf8()),
    });

    std::map<int32_t, int32_t> empty_map;
    ASSERT_OK_AND_ASSIGN(auto physical_schema,
                         ExtendMapUtils::LogicalToPhysicalSchema(schema, empty_map));
    ASSERT_TRUE(physical_schema->Equals(schema));
}

// ---- BuildColumnToNumColumns ----

TEST(ExtendMapUtilsTest, BuildColumnToNumColumns) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("metrics", arrow::map(arrow::utf8(), arrow::float64())),
    });

    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{"fields.tags.map-extend.max-columns", "128"},
                                               {"fields.metrics.map-extend.max-columns", "64"}}));

    std::vector<int32_t> extend_indices = {1, 2};
    ASSERT_OK_AND_ASSIGN(auto result,
                         ExtendMapUtils::BuildColumnToNumColumns(extend_indices, schema, options));

    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result[1], 128);
    ASSERT_EQ(result[2], 64);
}

TEST(ExtendMapUtilsTest, BuildColumnToNumColumnsDefault) {
    auto schema = arrow::schema({
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
    });

    // No explicit max-columns config -> default 256
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    std::vector<int32_t> extend_indices = {0};
    ASSERT_OK_AND_ASSIGN(auto result,
                         ExtendMapUtils::BuildColumnToNumColumns(extend_indices, schema, options));
    ASSERT_EQ(result[0], 256);
}

// ---- SerializeMetadata / DeserializeMetadata roundtrip ----

TEST(ExtendMapUtilsTest, MetadataRoundtripNoneCompression) {
    ExtendMapFileMetadata original;
    original.name_to_id = {{"age", 0}, {"name", 1}};
    original.field_to_columns = {{0, {0}}, {1, {1, 2}}};
    original.overflow_field_set = {1, 5};
    original.num_columns = 3;
    original.max_row_width = 2;

    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    ASSERT_OK(ExtendMapUtils::SerializeMetadata(original, "none", metadata.get()));

    // Verify raw KV strings to get intuition of what's stored
    auto find_value = [&](const char* key) -> std::string {
        int32_t idx = metadata->FindKey(key);
        EXPECT_GE(idx, 0);
        return metadata->value(idx);
    };
    ASSERT_EQ(find_value(ExtendMapDefine::kVersion), "1");
    ASSERT_EQ(find_value(ExtendMapDefine::kStorageLayout), "extend");
    ASSERT_EQ(find_value(ExtendMapDefine::kNumColumns), "3");
    ASSERT_EQ(find_value(ExtendMapDefine::kMaxRowWidth), "2");

    std::string expected_dict = "{\"age\":0,\"name\":1}";
    ASSERT_EQ(find_value(ExtendMapDefine::kFieldDict), expected_dict);
    // field_dict_original_size should be the length of the JSON string
    std::string field_dict_original_size = find_value(ExtendMapDefine::kFieldDictOriginalSize);
    ASSERT_EQ(field_dict_original_size, std::to_string(expected_dict.size()));

    std::string expected_field_to_columns = "{\"0\":[0],\"1\":[1,2]}";
    ASSERT_EQ(find_value(ExtendMapDefine::kFieldColumns), expected_field_to_columns);

    // overflow_set is a JSON array of sorted field_ids
    ASSERT_EQ(find_value(ExtendMapDefine::kOverflowSet), "[1,5]");

    // Roundtrip verify
    ASSERT_OK_AND_ASSIGN(auto deserialized, ExtendMapUtils::DeserializeMetadata(metadata, "none"));
    ASSERT_EQ(deserialized, original);
}

TEST(ExtendMapUtilsTest, MetadataRoundtripCompression) {
    ExtendMapFileMetadata original;
    original.name_to_id = {{"alpha", 0}, {"beta", 1}, {"gamma", 2}};
    original.field_to_columns = {{0, {0, 1, 2}}, {1, {3}}, {2, {4, 5}}};
    original.overflow_field_set = {2};
    original.num_columns = 6;
    original.max_row_width = 3;

    auto verify_roundtrip = [&](const std::string& compression) {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        ASSERT_OK(ExtendMapUtils::SerializeMetadata(original, compression, metadata.get()));
        ASSERT_OK_AND_ASSIGN(auto deserialized,
                             ExtendMapUtils::DeserializeMetadata(metadata, compression));
        ASSERT_EQ(deserialized, original);
    };

    verify_roundtrip("none");
    verify_roundtrip("lz4");
    verify_roundtrip("zstd");
}

TEST(ExtendMapUtilsTest, MetadataRoundtripEmptyData) {
    ExtendMapFileMetadata original;

    auto verify_roundtrip = [&](const std::string& compression) {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        ASSERT_OK(ExtendMapUtils::SerializeMetadata(original, compression, metadata.get()));
        ASSERT_OK_AND_ASSIGN(auto deserialized,
                             ExtendMapUtils::DeserializeMetadata(metadata, compression));
        ASSERT_EQ(deserialized, original);
    };

    verify_roundtrip("none");
    verify_roundtrip("lz4");
    verify_roundtrip("zstd");
}

// ---- DeserializeMetadata error cases ----

TEST(ExtendMapUtilsTest, DeserializeMetadataErrors) {
    // nullptr
    { ASSERT_NOK_WITH_MSG(ExtendMapUtils::DeserializeMetadata(nullptr, "none"), "null"); }
    // missing version
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append("some_key", "some_value");
        ASSERT_NOK_WITH_MSG(ExtendMapUtils::DeserializeMetadata(metadata, "none"), "version");
    }
    // wrong version
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(ExtendMapDefine::kVersion, "999");
        metadata->Append(ExtendMapDefine::kFieldDictOriginalSize, "2");
        metadata->Append(ExtendMapDefine::kFieldDict, "{}");
        ASSERT_NOK_WITH_MSG(ExtendMapUtils::DeserializeMetadata(metadata, "none"), "unsupported");
    }
    // missing field_dict
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(ExtendMapDefine::kVersion, "1");
        metadata->Append(ExtendMapDefine::kFieldDictOriginalSize, "2");
        ASSERT_NOK_WITH_MSG(ExtendMapUtils::DeserializeMetadata(metadata, "none"), "field_dict");
    }
}

// ---- HasExtendMetadata ----

TEST(ExtendMapUtilsTest, HasExtendMetadata) {
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(ExtendMapDefine::kStorageLayout, ExtendMapDefine::kStorageLayoutExtend);
        ASSERT_TRUE(ExtendMapUtils::HasExtendMetadata(metadata));
    }
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(ExtendMapDefine::kStorageLayout, "default");
        ASSERT_FALSE(ExtendMapUtils::HasExtendMetadata(metadata));
    }
    { ASSERT_FALSE(ExtendMapUtils::HasExtendMetadata(nullptr)); }
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        ASSERT_FALSE(ExtendMapUtils::HasExtendMetadata(metadata));
    }
}

// ---- PhysicalColumnName ----

TEST(ExtendMapUtilsTest, PhysicalColumnName) {
    ASSERT_EQ(ExtendMapDefine::PhysicalColumnName(0), "__col_0");
    ASSERT_EQ(ExtendMapDefine::PhysicalColumnName(1), "__col_1");
    ASSERT_EQ(ExtendMapDefine::PhysicalColumnName(99), "__col_99");
}

}  // namespace paimon::test
