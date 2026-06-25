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

#include "paimon/core/utils/nested_projection_utils.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// Helper: create an arrow::Field with paimon.id metadata
static std::shared_ptr<arrow::Field> MakeField(const std::string& name,
                                               const std::shared_ptr<arrow::DataType>& type,
                                               int32_t paimon_id) {
    DataField data_field(paimon_id, arrow::field(name, type));
    return DataField::ConvertDataFieldToArrowField(data_field);
}

// ============== GetPaimonFieldId ==============

TEST(NestedProjectionUtilsTest, GetPaimonFieldIdPresent) {
    auto field = MakeField("col", arrow::int32(), 42);
    ASSERT_EQ(NestedProjectionUtils::GetPaimonFieldId(field), 42);
}

TEST(NestedProjectionUtilsTest, GetPaimonFieldIdMissing) {
    auto field = arrow::field("col", arrow::int32());
    ASSERT_EQ(NestedProjectionUtils::GetPaimonFieldId(field), -1);
}

TEST(NestedProjectionUtilsTest, GetPaimonFieldIdNullptr) {
    ASSERT_EQ(NestedProjectionUtils::GetPaimonFieldId(nullptr), -1);
}

// ============== FindFieldByPaimonId ==============

TEST(NestedProjectionUtilsTest, FindFieldByPaimonIdFound) {
    auto struct_type =
        arrow::struct_({MakeField("x", arrow::int32(), 1), MakeField("y", arrow::utf8(), 2)});
    auto found = NestedProjectionUtils::FindFieldByPaimonId(struct_type, 2);
    ASSERT_NE(found, nullptr);
    ASSERT_EQ(found->name(), "y");
}

TEST(NestedProjectionUtilsTest, FindFieldByPaimonIdNotFound) {
    auto struct_type = arrow::struct_({MakeField("x", arrow::int32(), 1)});
    ASSERT_EQ(NestedProjectionUtils::FindFieldByPaimonId(struct_type, 99), nullptr);
}

TEST(NestedProjectionUtilsTest, FindFieldByPaimonIdNonStruct) {
    ASSERT_EQ(NestedProjectionUtils::FindFieldByPaimonId(arrow::int32(), 1), nullptr);
}

// ============== PruneDataType ==============

TEST(NestedProjectionUtilsTest, PruneDataTypeIdenticalTypes) {
    auto type = arrow::int32();
    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(type, type));
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value()->Equals(type));
}

TEST(NestedProjectionUtilsTest, PruneDataTypeAtomicType) {
    // Different atomic types: return data_type
    auto read_type = arrow::int64();
    auto data_type = arrow::int32();
    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(read_type, data_type));
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value()->Equals(data_type));
}

TEST(NestedProjectionUtilsTest, PruneDataTypeStructPruneSubset) {
    // data: STRUCT<x:INT(id=1), y:STRING(id=2), z:DOUBLE(id=3)>
    // read: STRUCT<x:INT(id=1)>
    // expected: STRUCT<x:INT(id=1)>
    auto data_type =
        arrow::struct_({MakeField("x", arrow::int32(), 1), MakeField("y", arrow::utf8(), 2),
                        MakeField("z", arrow::float64(), 3)});
    auto read_type = arrow::struct_({MakeField("x", arrow::int32(), 1)});

    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(read_type, data_type));
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value()->num_fields(), 1);
    ASSERT_EQ(result.value()->field(0)->name(), "x");
}

TEST(NestedProjectionUtilsTest, PruneDataTypeStructAllFieldsPruned) {
    // data: STRUCT<x:INT(id=1)>
    // read: STRUCT<y:INT(id=99)>  — no match
    // expected: fail-fast (struct-internal schema evolution unsupported)
    auto data_type = arrow::struct_({MakeField("x", arrow::int32(), 1)});
    auto read_type = arrow::struct_({MakeField("y", arrow::int32(), 99)});

    ASSERT_NOK_WITH_MSG(NestedProjectionUtils::PruneDataType(read_type, data_type),
                        "does not support schema evolution inside struct");
}

TEST(NestedProjectionUtilsTest, PruneDataTypeNestedStruct) {
    // data: STRUCT<inner:STRUCT<a:INT(id=10), b:STRING(id=11)>(id=1)>
    // read: STRUCT<inner:STRUCT<a:INT(id=10)>(id=1)>
    // expected: STRUCT<inner:STRUCT<a:INT(id=10)>(id=1)>
    auto inner_data =
        arrow::struct_({MakeField("a", arrow::int32(), 10), MakeField("b", arrow::utf8(), 11)});
    auto data_type = arrow::struct_({MakeField("inner", inner_data, 1)});

    auto inner_read = arrow::struct_({MakeField("a", arrow::int32(), 10)});
    auto read_type = arrow::struct_({MakeField("inner", inner_read, 1)});

    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(read_type, data_type));
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value()->num_fields(), 1);
    auto pruned_inner = result.value()->field(0)->type();
    ASSERT_EQ(pruned_inner->num_fields(), 1);
    ASSERT_EQ(pruned_inner->field(0)->name(), "a");
}

TEST(NestedProjectionUtilsTest, PruneDataTypeListWithStructElement) {
    // data: LIST<STRUCT<a:INT(id=10), b:STRING(id=11)>>
    // read: LIST<STRUCT<a:INT(id=10)>>
    auto inner_data =
        arrow::struct_({MakeField("a", arrow::int32(), 10), MakeField("b", arrow::utf8(), 11)});
    auto data_type = arrow::list(arrow::field("item", inner_data));

    auto inner_read = arrow::struct_({MakeField("a", arrow::int32(), 10)});
    auto read_type = arrow::list(arrow::field("item", inner_read));

    ASSERT_NOK_WITH_MSG(NestedProjectionUtils::PruneDataType(read_type, data_type),
                        "partial projection inside list");
}

TEST(NestedProjectionUtilsTest, PruneDataTypeMapWithStructValue) {
    // data: MAP<STRING, STRUCT<a:INT(id=10), b:STRING(id=11)>>
    // read: MAP<STRING, STRUCT<a:INT(id=10)>>
    auto inner_data =
        arrow::struct_({MakeField("a", arrow::int32(), 10), MakeField("b", arrow::utf8(), 11)});
    auto data_type = arrow::map(arrow::utf8(), inner_data);

    auto inner_read = arrow::struct_({MakeField("a", arrow::int32(), 10)});
    auto read_type = arrow::map(arrow::utf8(), inner_read);

    ASSERT_NOK_WITH_MSG(NestedProjectionUtils::PruneDataType(read_type, data_type),
                        "partial projection inside map");
}

TEST(NestedProjectionUtilsTest, HasNestedSubfieldProjectionNoProjection) {
    auto file_schema = arrow::schema({
        MakeField("f0", arrow::int32(), 1),
        MakeField("f1", arrow::struct_({MakeField("a", arrow::int32(), 2)}), 3),
    });
    auto read_schema = arrow::schema({
        MakeField("f0", arrow::int32(), 1),
        MakeField("f1", arrow::struct_({MakeField("a", arrow::int32(), 2)}), 3),
    });
    ASSERT_OK_AND_ASSIGN(
        auto has_nested_projection,
        NestedProjectionUtils::HasNestedSubfieldProjection(file_schema, read_schema));
    ASSERT_FALSE(has_nested_projection);
}

TEST(NestedProjectionUtilsTest, HasNestedSubfieldProjectionWithProjection) {
    auto file_schema = arrow::schema({
        MakeField("f0", arrow::int32(), 1),
        MakeField(
            "f1",
            arrow::struct_({MakeField("a", arrow::int32(), 2), MakeField("b", arrow::utf8(), 4)}),
            3),
    });
    auto read_schema = arrow::schema({
        MakeField("f0", arrow::int32(), 1),
        MakeField("f1", arrow::struct_({MakeField("a", arrow::int32(), 2)}), 3),
    });
    ASSERT_OK_AND_ASSIGN(
        auto has_nested_projection,
        NestedProjectionUtils::HasNestedSubfieldProjection(file_schema, read_schema));
    ASSERT_TRUE(has_nested_projection);
}

TEST(NestedProjectionUtilsTest, HasNestedSubfieldProjectionTypeMismatchReturnsInvalid) {
    auto file_schema = arrow::schema({
        MakeField("f0", arrow::struct_({MakeField("a", arrow::int32(), 2)}), 1),
    });
    auto read_schema = arrow::schema({
        MakeField("f0", arrow::list(arrow::field("item", arrow::int32())), 1),
    });

    ASSERT_NOK_WITH_MSG(
        NestedProjectionUtils::HasNestedSubfieldProjection(file_schema, read_schema),
        "requires same nested type kind");
}

TEST(NestedProjectionUtilsTest, HasNestedSubfieldProjectionAtomicTypeMismatchReturnsFalse) {
    auto file_schema = arrow::schema({
        MakeField("f0", arrow::map(arrow::utf8(), arrow::int32()), 1),
    });
    auto read_schema = arrow::schema({
        MakeField("f0", arrow::map(arrow::utf8(), arrow::int16()), 1),
    });

    ASSERT_OK_AND_ASSIGN(
        auto has_nested_projection,
        NestedProjectionUtils::HasNestedSubfieldProjection(file_schema, read_schema));
    ASSERT_FALSE(has_nested_projection);
}

TEST(NestedProjectionUtilsTest, HasNestedSubfieldProjectionMissingStructChildReturnsInvalid) {
    auto file_schema = arrow::schema({
        MakeField(
            "f0",
            arrow::struct_({MakeField("a", arrow::int32(), 2), MakeField("b", arrow::utf8(), 3)}),
            1),
    });
    auto read_schema = arrow::schema({
        MakeField(
            "f0",
            arrow::struct_({MakeField("a", arrow::int32(), 2), MakeField("c", arrow::utf8(), 4)}),
            1),
    });

    ASSERT_NOK_WITH_MSG(
        NestedProjectionUtils::HasNestedSubfieldProjection(file_schema, read_schema),
        "requested struct child");
}

TEST(NestedProjectionUtilsTest, HasNestedSubfieldProjectionMissingTopLevelFieldReturnsInvalid) {
    auto file_schema = arrow::schema({
        MakeField("f0", arrow::int32(), 1),
    });
    auto read_schema = arrow::schema({
        MakeField("f0", arrow::int32(), 1),
        MakeField("f1", arrow::struct_({MakeField("a", arrow::int32(), 2)}), 3),
    });

    ASSERT_NOK_WITH_MSG(
        NestedProjectionUtils::HasNestedSubfieldProjection(file_schema, read_schema),
        "missing in file schema");
}

// ============== GetMapSelectedKeys ==============
TEST(NestedProjectionUtilsTest, GetMapSelectedKeysPresent) {
    auto metadata =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"key1,key2,key3"});
    auto field =
        arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()), /*nullable=*/true, metadata);
    ASSERT_OK_AND_ASSIGN(auto keys, NestedProjectionUtils::GetMapSelectedKeys(field));
    ASSERT_EQ(keys.size(), 3);
    ASSERT_EQ(keys[0], "key1");
    ASSERT_EQ(keys[1], "key2");
    ASSERT_EQ(keys[2], "key3");
}

TEST(NestedProjectionUtilsTest, GetMapSelectedKeysAbsent) {
    auto field = arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()));
    ASSERT_OK_AND_ASSIGN(auto keys, NestedProjectionUtils::GetMapSelectedKeys(field));
    ASSERT_TRUE(keys.empty());
}

TEST(NestedProjectionUtilsTest, GetMapSelectedKeysEmptyString) {
    auto metadata = arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {""});
    auto field =
        arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()), /*nullable=*/true, metadata);
    ASSERT_OK_AND_ASSIGN(auto keys, NestedProjectionUtils::GetMapSelectedKeys(field));
    ASSERT_EQ(keys.size(), 1);
    ASSERT_EQ(keys[0], "");
}

TEST(NestedProjectionUtilsTest, GetMapSelectedKeysContainsEmptyToken) {
    auto metadata = arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a, ,b"});
    auto field =
        arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()), /*nullable=*/true, metadata);
    ASSERT_OK_AND_ASSIGN(auto keys, NestedProjectionUtils::GetMapSelectedKeys(field));
    ASSERT_EQ(keys.size(), 3);
    ASSERT_EQ(keys[0], "a");
    ASSERT_EQ(keys[1], " ");
    ASSERT_EQ(keys[2], "b");
}

TEST(NestedProjectionUtilsTest, GetMapSelectedKeysWhitespaceOnly) {
    auto metadata = arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"   "});
    auto field =
        arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()), /*nullable=*/true, metadata);
    ASSERT_OK_AND_ASSIGN(auto keys, NestedProjectionUtils::GetMapSelectedKeys(field));
    ASSERT_EQ(keys.size(), 1);
    ASSERT_EQ(keys[0], "   ");
}

TEST(NestedProjectionUtilsTest, GetMapSelectedKeysDuplicateKey) {
    auto metadata = arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a,b,a"});
    auto field =
        arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()), /*nullable=*/true, metadata);
    ASSERT_NOK_WITH_MSG(NestedProjectionUtils::GetMapSelectedKeys(field),
                        "Duplicate selected key 'a'");
}

// ============== FilterMapArrayBySelectedKeys ==============

class NestedProjectionUtilsMapArrayTest : public ::testing::Test {
 protected:
    // Helper to build a MapArray<string, int32> from vectors of key-value pairs.
    static std::shared_ptr<arrow::Array> BuildStringInt32MapArray(
        const std::vector<std::vector<std::pair<std::string, int32_t>>>& maps,
        const std::vector<bool>& null_mask = {}) {
        auto key_builder = std::make_shared<arrow::StringBuilder>();
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        arrow::MapBuilder map_builder(arrow::default_memory_pool(), key_builder, value_builder);
        for (size_t i = 0; i < maps.size(); ++i) {
            if (!null_mask.empty() && !null_mask[i]) {
                EXPECT_TRUE(map_builder.AppendNull().ok());
                continue;
            }
            EXPECT_TRUE(map_builder.Append().ok());
            for (const auto& [k, v] : maps[i]) {
                EXPECT_TRUE(key_builder->Append(k).ok());
                EXPECT_TRUE(value_builder->Append(v).ok());
            }
        }
        std::shared_ptr<arrow::Array> result;
        EXPECT_TRUE(map_builder.Finish(&result).ok());
        return result;
    }
};

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysBasic) {
    // Map with 3 entries each, select only "a" and "c"
    auto map_array = BuildStringInt32MapArray({
        {{"a", 1}, {"b", 2}, {"c", 3}},
        {{"a", 10}, {"d", 40}},
    });

    std::vector<std::string> selected = {"a", "c"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                                            map_array, selected, arrow::default_memory_pool()));

    auto expected = BuildStringInt32MapArray({
        {{"a", 1}, {"c", 3}},
        {{"a", 10}},
    });
    ASSERT_TRUE(filtered->Equals(expected));
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysEmptySelectedKeys) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}}});
    std::vector<std::string> empty_keys;
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                                            map_array, empty_keys, arrow::default_memory_pool()));
    // Should return original array unchanged
    ASSERT_EQ(filtered.get(), map_array.get());
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysAllKept) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}, {"b", 2}}});
    std::vector<std::string> selected = {"a", "b"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                                            map_array, selected, arrow::default_memory_pool()));
    ASSERT_TRUE(filtered->Equals(map_array));
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysNoneKept) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}, {"b", 2}}});
    std::vector<std::string> selected = {"x", "y"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                                            map_array, selected, arrow::default_memory_pool()));
    auto expected = BuildStringInt32MapArray({{}});
    ASSERT_TRUE(filtered->Equals(expected));
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysEmptyStringKeySelected) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}, {"", 9}, {"b", 2}}});
    std::vector<std::string> selected = {""};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                                            map_array, selected, arrow::default_memory_pool()));
    auto expected = BuildStringInt32MapArray({{{"", 9}}});
    ASSERT_TRUE(filtered->Equals(expected));
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysWithNull) {
    // maps[0] = {"a":1}, maps[1] = null, maps[2] = {"b":2,"c":3}
    auto map_array =
        BuildStringInt32MapArray({{{"a", 1}}, {}, {{"b", 2}, {"c", 3}}}, {true, false, true});

    std::vector<std::string> selected = {"a", "c"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                                            map_array, selected, arrow::default_memory_pool()));
    auto expected = BuildStringInt32MapArray({{{"a", 1}}, {}, {{"c", 3}}}, {true, false, true});
    ASSERT_TRUE(filtered->Equals(expected));
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysEmptyArray) {
    auto map_array = BuildStringInt32MapArray({});
    std::vector<std::string> selected = {"a"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                                            map_array, selected, arrow::default_memory_pool()));
    auto expected = BuildStringInt32MapArray({});
    ASSERT_TRUE(filtered->Equals(expected));
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysSelectedOrderWins) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}, {"b", 2}, {"c", 3}}});
    std::vector<std::string> selected = {"c", "a"};

    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                                            map_array, selected, arrow::default_memory_pool()));
    auto expected = BuildStringInt32MapArray({{{"c", 3}, {"a", 1}}});
    ASSERT_TRUE(filtered->Equals(expected));
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysDuplicateSelectedKeys) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}, {"b", 2}}});
    std::vector<std::string> selected = {"a", "a"};

    ASSERT_NOK_WITH_MSG(NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                            map_array, selected, arrow::default_memory_pool()),
                        "Duplicate selected key 'a'");
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysDictionaryStringKey) {
    auto map_array = BuildStringInt32MapArray({
        {{"a", 1}, {"b", 2}, {"c", 3}},
        {{"c", 30}, {"a", 10}},
    });
    auto map = std::static_pointer_cast<arrow::MapArray>(map_array);
    auto string_keys = std::static_pointer_cast<arrow::StringArray>(map->keys());

    arrow::StringDictionaryBuilder dict_builder(arrow::default_memory_pool());
    for (int64_t i = 0; i < string_keys->length(); ++i) {
        ASSERT_TRUE(dict_builder.Append(string_keys->GetView(i)).ok());
    }
    auto dict_keys_result = dict_builder.Finish();
    ASSERT_TRUE(dict_keys_result.ok());
    auto dict_keys = dict_keys_result.ValueUnsafe();

    auto dict_map_type = arrow::map(dict_keys->type(), map->items()->type());
    auto dict_map_array = std::make_shared<arrow::MapArray>(
        dict_map_type, map->length(), map->value_offsets(), dict_keys, map->items(),
        map->null_bitmap(), map->null_count(), map->offset());

    std::vector<std::string> selected = {"c", "a"};
    ASSERT_OK_AND_ASSIGN(auto filtered,
                         NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                             dict_map_array, selected, arrow::default_memory_pool()));

    auto expected = BuildStringInt32MapArray({
        {{"c", 3}, {"a", 1}},
        {{"c", 30}, {"a", 10}},
    });
    ASSERT_TRUE(filtered->Equals(expected));
}

TEST_F(NestedProjectionUtilsMapArrayTest, FilterMapArrayBySelectedKeysDictionaryLargeStringKey) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}, {"b", 2}}});
    auto map = std::static_pointer_cast<arrow::MapArray>(map_array);

    arrow::LargeStringBuilder dict_value_builder(arrow::default_memory_pool());
    ASSERT_TRUE(dict_value_builder.Append("a").ok());
    ASSERT_TRUE(dict_value_builder.Append("b").ok());
    std::shared_ptr<arrow::Array> dict_values;
    ASSERT_TRUE(dict_value_builder.Finish(&dict_values).ok());

    arrow::Int64Builder index_builder(arrow::default_memory_pool());
    ASSERT_TRUE(index_builder.Append(0).ok());
    ASSERT_TRUE(index_builder.Append(1).ok());
    std::shared_ptr<arrow::Array> indices;
    ASSERT_TRUE(index_builder.Finish(&indices).ok());

    auto dict_type = arrow::dictionary(arrow::int64(), arrow::large_utf8());
    auto large_string_dict_keys_result =
        arrow::DictionaryArray::FromArrays(dict_type, indices, dict_values);
    ASSERT_TRUE(large_string_dict_keys_result.ok());
    auto large_string_dict_keys = large_string_dict_keys_result.ValueUnsafe();

    auto dict_map_type = arrow::map(large_string_dict_keys->type(), map->items()->type());
    auto dict_map_array = std::make_shared<arrow::MapArray>(
        dict_map_type, map->length(), map->value_offsets(), large_string_dict_keys, map->items(),
        map->null_bitmap(), map->null_count(), map->offset());

    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(
                                            dict_map_array, {"a"}, arrow::default_memory_pool()));
    auto expected = BuildStringInt32MapArray({{{"a", 1}}});
    ASSERT_TRUE(filtered->Equals(expected));
}

}  // namespace paimon::test
