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

#include "paimon/common/data/shredding/map_shared_shredding_utils.h"

#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "gtest/gtest.h"
#include "paimon/core/core_options.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// ---- IsShreddingKeyMap ----

TEST(MapSharedShreddingUtilsTest, IsShreddingKeyMap) {
    ASSERT_TRUE(
        MapSharedShreddingUtils::IsShreddingKeyMap(arrow::map(arrow::utf8(), arrow::int32())));
    ASSERT_TRUE(
        MapSharedShreddingUtils::IsShreddingKeyMap(arrow::map(arrow::utf8(), arrow::float64())));
    // Nested value type (struct)
    auto nested_value =
        arrow::struct_({arrow::field("x", arrow::int32()), arrow::field("y", arrow::utf8())});
    ASSERT_TRUE(
        MapSharedShreddingUtils::IsShreddingKeyMap(arrow::map(arrow::utf8(), nested_value)));
    ASSERT_FALSE(
        MapSharedShreddingUtils::IsShreddingKeyMap(arrow::map(arrow::int32(), arrow::utf8())));
    ASSERT_FALSE(MapSharedShreddingUtils::IsShreddingKeyMap(arrow::int32()));
    ASSERT_FALSE(MapSharedShreddingUtils::IsShreddingKeyMap(arrow::list(arrow::utf8())));
}

// ---- DetectShreddingColumns ----

TEST(MapSharedShreddingUtilsTest, DetectShreddingColumnsBasic) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("metrics", arrow::map(arrow::utf8(), arrow::float64())),
        arrow::field("name", arrow::utf8()),
    });

    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{"fields.tags.map.storage-layout", "shared-shredding"},
                              {"fields.metrics.map.storage-layout", "shared-shredding"}}));

    ASSERT_OK_AND_ASSIGN(auto indices,
                         MapSharedShreddingUtils::DetectShreddingColumns(schema, options));
    ASSERT_EQ(indices.size(), 2);
    ASSERT_EQ(indices[0], 1);
    ASSERT_EQ(indices[1], 2);
}

TEST(MapSharedShreddingUtilsTest, DetectShreddingColumnsNoShredding) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
    });

    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    ASSERT_OK_AND_ASSIGN(auto indices,
                         MapSharedShreddingUtils::DetectShreddingColumns(schema, options));
    ASSERT_TRUE(indices.empty());
}

// ---- LogicalToPhysicalSchema ----

TEST(MapSharedShreddingUtilsTest, LogicalToPhysicalSchemaBasic) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("name", arrow::utf8()),
    });

    std::map<int32_t, int32_t> column_to_num_columns = {{1, 4}};
    ASSERT_OK_AND_ASSIGN(auto physical_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                   schema, column_to_num_columns));

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

TEST(MapSharedShreddingUtilsTest, LogicalToPhysicalSchemaNestedValue) {
    // MAP<STRING, STRUCT<a: int32, b: utf8>>
    auto nested_value =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    auto map_type = arrow::map(arrow::utf8(), nested_value);
    auto schema = arrow::schema({arrow::field("data", map_type)});

    std::map<int32_t, int32_t> column_to_num_columns = {{0, 2}};
    ASSERT_OK_AND_ASSIGN(auto physical_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                   schema, column_to_num_columns));

    auto expected_struct = arrow::struct_({
        arrow::field("__field_mapping", arrow::list(arrow::int32()), false),
        arrow::field("__col_0", nested_value, true),
        arrow::field("__col_1", nested_value, true),
        arrow::field("__overflow", arrow::map(arrow::int32(), nested_value), true),
    });
    auto expected_schema = arrow::schema({arrow::field("data", expected_struct, true)});
    ASSERT_TRUE(physical_schema->Equals(expected_schema));
}

TEST(MapSharedShreddingUtilsTest, LogicalToPhysicalSchemaNullable) {
    // MAP value is nullable
    auto nullable_map = arrow::map(arrow::utf8(), arrow::field("item", arrow::int64(), true));
    auto schema_nullable = arrow::schema({arrow::field("m", nullable_map)});
    std::map<int32_t, int32_t> col_map = {{0, 2}};

    ASSERT_OK_AND_ASSIGN(
        auto physical, MapSharedShreddingUtils::LogicalToPhysicalSchema(schema_nullable, col_map));
    auto struct_type = physical->field(0)->type();
    ASSERT_TRUE(struct_type->field(1)->nullable());
    ASSERT_TRUE(struct_type->field(2)->nullable());

    // MAP value is non-nullable
    auto non_nullable_map = arrow::map(arrow::utf8(), arrow::field("item", arrow::int64(), false));
    auto schema_non_nullable = arrow::schema({arrow::field("m", non_nullable_map)});

    ASSERT_OK_AND_ASSIGN(auto physical2, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                             schema_non_nullable, col_map));
    auto struct_type2 = physical2->field(0)->type();
    ASSERT_FALSE(struct_type2->field(1)->nullable());
    ASSERT_FALSE(struct_type2->field(2)->nullable());
}

TEST(MapSharedShreddingUtilsTest, LogicalToPhysicalSchemaNoShreddingColumns) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("name", arrow::utf8()),
    });

    std::map<int32_t, int32_t> empty_map;
    ASSERT_OK_AND_ASSIGN(auto physical_schema,
                         MapSharedShreddingUtils::LogicalToPhysicalSchema(schema, empty_map));
    ASSERT_TRUE(physical_schema->Equals(schema));
}

// ---- BuildColumnToNumColumns ----

TEST(MapSharedShreddingUtilsTest, BuildColumnToNumColumns) {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("metrics", arrow::map(arrow::utf8(), arrow::float64())),
    });

    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{"fields.tags.map.shared-shredding.max-columns", "128"},
                              {"fields.metrics.map.shared-shredding.max-columns", "64"}}));

    std::vector<int32_t> shredding_indices = {1, 2};
    ASSERT_OK_AND_ASSIGN(auto result, MapSharedShreddingUtils::BuildColumnToNumColumns(
                                          shredding_indices, schema, options));

    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result[1], 128);
    ASSERT_EQ(result[2], 64);
}

TEST(MapSharedShreddingUtilsTest, BuildColumnToNumColumnsDefault) {
    auto schema = arrow::schema({
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
    });

    // No explicit max-columns config -> default 256
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    std::vector<int32_t> shredding_indices = {0};
    ASSERT_OK_AND_ASSIGN(auto result, MapSharedShreddingUtils::BuildColumnToNumColumns(
                                          shredding_indices, schema, options));
    ASSERT_EQ(result[0], 256);
}

// ---- SerializeMetadata / DeserializeMetadata roundtrip ----

TEST(MapSharedShreddingUtilsTest, MetadataRoundtripNoneCompression) {
    MapSharedShreddingFieldMeta original;
    original.name_to_id = {{"age", 0}, {"name", 1}};
    original.field_to_columns = {{0, {0}}, {1, {1, 2}}};
    original.overflow_field_set = {1, 5};
    original.num_columns = 3;
    original.max_row_width = 2;

    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    ASSERT_OK(MapSharedShreddingUtils::SerializeMetadata(original, "none", metadata.get()));

    // Verify raw KV strings to get intuition of what's stored
    auto find_value = [&](const char* key) -> std::string {
        int32_t idx = metadata->FindKey(key);
        EXPECT_GE(idx, 0);
        return metadata->value(idx);
    };
    ASSERT_EQ(find_value(MapShreddingDefine::kStorageLayout), "shared-shredding");
    ASSERT_EQ(find_value(MapSharedShreddingDefine::kVersion), "1");
    ASSERT_EQ(find_value(MapSharedShreddingDefine::kNumColumns), "3");
    ASSERT_EQ(find_value(MapSharedShreddingDefine::kMaxRowWidth), "2");

    std::string expected_dict = R"({"age":0,"name":1})";
    ASSERT_EQ(find_value(MapSharedShreddingDefine::kFieldDict), expected_dict);
    // field_dict_original_size should be the length of the JSON string
    std::string field_dict_original_size =
        find_value(MapSharedShreddingDefine::kFieldDictOriginalSize);
    ASSERT_EQ(field_dict_original_size, std::to_string(expected_dict.size()));

    std::string expected_field_to_columns = R"({"0":[0],"1":[1,2]})";
    ASSERT_EQ(find_value(MapSharedShreddingDefine::kFieldColumns), expected_field_to_columns);

    // overflow_set is a JSON array of sorted field_ids
    ASSERT_EQ(find_value(MapSharedShreddingDefine::kOverflowSet), "[1,5]");

    // Roundtrip verify
    ASSERT_OK_AND_ASSIGN(auto deserialized,
                         MapSharedShreddingUtils::DeserializeMetadata(metadata, "none"));
    ASSERT_EQ(deserialized, original);
}

TEST(MapSharedShreddingUtilsTest, MetadataRoundtripCompression) {
    MapSharedShreddingFieldMeta original;
    original.name_to_id = {{"alpha", 0}, {"beta", 1}, {"gamma", 2}};
    original.field_to_columns = {{0, {0, 1, 2}}, {1, {3}}, {2, {4, 5}}};
    original.overflow_field_set = {2};
    original.num_columns = 6;
    original.max_row_width = 3;

    auto verify_roundtrip = [&](const std::string& compression) {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        ASSERT_OK(
            MapSharedShreddingUtils::SerializeMetadata(original, compression, metadata.get()));
        ASSERT_OK_AND_ASSIGN(auto deserialized,
                             MapSharedShreddingUtils::DeserializeMetadata(metadata, compression));
        ASSERT_EQ(deserialized, original);
    };

    verify_roundtrip("none");
    verify_roundtrip("lz4");
    verify_roundtrip("zstd");
}

TEST(MapSharedShreddingUtilsTest, MetadataRoundtripEmptyData) {
    MapSharedShreddingFieldMeta original;

    auto verify_roundtrip = [&](const std::string& compression) {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        ASSERT_OK(
            MapSharedShreddingUtils::SerializeMetadata(original, compression, metadata.get()));
        ASSERT_OK_AND_ASSIGN(auto deserialized,
                             MapSharedShreddingUtils::DeserializeMetadata(metadata, compression));
        ASSERT_EQ(deserialized, original);
    };

    verify_roundtrip("none");
    verify_roundtrip("lz4");
    verify_roundtrip("zstd");
}

// ---- DeserializeMetadata error cases ----

TEST(MapSharedShreddingUtilsTest, DeserializeMetadataErrors) {
    const std::string layout_error = "metadata is null or storage layout is not shared-shredding";
    // nullptr
    ASSERT_NOK_WITH_MSG(MapSharedShreddingUtils::DeserializeMetadata(nullptr, "none"),
                        layout_error);
    // missing storage layout
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append("some_key", "some_value");
        ASSERT_NOK_WITH_MSG(MapSharedShreddingUtils::DeserializeMetadata(metadata, "none"),
                            layout_error);
    }
    // wrong storage layout
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(MapShreddingDefine::kStorageLayout, "default");
        ASSERT_NOK_WITH_MSG(MapSharedShreddingUtils::DeserializeMetadata(metadata, "none"),
                            layout_error);
    }
    // missing version
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(MapShreddingDefine::kStorageLayout,
                         MapShreddingDefine::kStorageLayoutSharedShredding);
        ASSERT_NOK_WITH_MSG(MapSharedShreddingUtils::DeserializeMetadata(metadata, "none"),
                            "missing shredding metadata key: paimon.map.shared-shredding.version");
    }
    // wrong version
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(MapShreddingDefine::kStorageLayout,
                         MapShreddingDefine::kStorageLayoutSharedShredding);
        metadata->Append(MapSharedShreddingDefine::kVersion, "999");
        metadata->Append(MapSharedShreddingDefine::kFieldDictOriginalSize, "2");
        metadata->Append(MapSharedShreddingDefine::kFieldDict, "{}");
        ASSERT_NOK_WITH_MSG(MapSharedShreddingUtils::DeserializeMetadata(metadata, "none"),
                            "unsupported shared-shredding metadata version: 999");
    }
    // missing field_dict
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(MapShreddingDefine::kStorageLayout,
                         MapShreddingDefine::kStorageLayoutSharedShredding);
        metadata->Append(MapSharedShreddingDefine::kVersion, "1");
        metadata->Append(MapSharedShreddingDefine::kFieldDictOriginalSize, "2");
        ASSERT_NOK_WITH_MSG(
            MapSharedShreddingUtils::DeserializeMetadata(metadata, "none"),
            "missing shredding metadata key: paimon.map.shared-shredding.field-dict");
    }
}

// ---- HasShreddingMetadata ----

TEST(MapSharedShreddingUtilsTest, HasShreddingMetadata) {
    ASSERT_FALSE(MapSharedShreddingUtils::HasShreddingMetadata(nullptr));
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(MapShreddingDefine::kStorageLayout,
                         MapShreddingDefine::kStorageLayoutSharedShredding);
        ASSERT_TRUE(MapSharedShreddingUtils::HasShreddingMetadata(metadata));
    }
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(MapShreddingDefine::kStorageLayout, "default");
        ASSERT_FALSE(MapSharedShreddingUtils::HasShreddingMetadata(metadata));
    }
    {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        ASSERT_FALSE(MapSharedShreddingUtils::HasShreddingMetadata(metadata));
    }
}

// ---- PhysicalColumnName ----

TEST(MapSharedShreddingUtilsTest, PhysicalColumnName) {
    ASSERT_EQ(MapSharedShreddingDefine::PhysicalColumnName(0), "__col_0");
    ASSERT_EQ(MapSharedShreddingDefine::PhysicalColumnName(1), "__col_1");
    ASSERT_EQ(MapSharedShreddingDefine::PhysicalColumnName(99), "__col_99");
}

}  // namespace paimon::test
