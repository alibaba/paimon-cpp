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
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
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

TEST(NestedProjectionUtilsTest, GetPaimonFieldId_Present) {
    auto field = MakeField("col", arrow::int32(), 42);
    ASSERT_EQ(NestedProjectionUtils::GetPaimonFieldId(field), 42);
}

TEST(NestedProjectionUtilsTest, GetPaimonFieldId_Missing) {
    auto field = arrow::field("col", arrow::int32());
    ASSERT_EQ(NestedProjectionUtils::GetPaimonFieldId(field), -1);
}

TEST(NestedProjectionUtilsTest, GetPaimonFieldId_Nullptr) {
    ASSERT_EQ(NestedProjectionUtils::GetPaimonFieldId(nullptr), -1);
}

// ============== FindFieldByPaimonId ==============

TEST(NestedProjectionUtilsTest, FindFieldByPaimonId_Found) {
    auto struct_type = arrow::struct_({MakeField("x", arrow::int32(), 1),
                                       MakeField("y", arrow::utf8(), 2)});
    auto found = NestedProjectionUtils::FindFieldByPaimonId(struct_type, 2);
    ASSERT_NE(found, nullptr);
    ASSERT_EQ(found->name(), "y");
}

TEST(NestedProjectionUtilsTest, FindFieldByPaimonId_NotFound) {
    auto struct_type = arrow::struct_({MakeField("x", arrow::int32(), 1)});
    ASSERT_EQ(NestedProjectionUtils::FindFieldByPaimonId(struct_type, 99), nullptr);
}

TEST(NestedProjectionUtilsTest, FindFieldByPaimonId_NonStruct) {
    ASSERT_EQ(NestedProjectionUtils::FindFieldByPaimonId(arrow::int32(), 1), nullptr);
}

// ============== PruneDataType ==============

TEST(NestedProjectionUtilsTest, PruneDataType_IdenticalTypes) {
    auto type = arrow::int32();
    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(type, type));
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value()->Equals(type));
}

TEST(NestedProjectionUtilsTest, PruneDataType_AtomicType) {
    // Different atomic types: return data_type
    auto read_type = arrow::int64();
    auto data_type = arrow::int32();
    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(read_type, data_type));
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value()->Equals(data_type));
}

TEST(NestedProjectionUtilsTest, PruneDataType_StructPruneSubset) {
    // data: STRUCT<x:INT(id=1), y:STRING(id=2), z:DOUBLE(id=3)>
    // read: STRUCT<x:INT(id=1)>
    // expected: STRUCT<x:INT(id=1)>
    auto data_type = arrow::struct_({MakeField("x", arrow::int32(), 1),
                                     MakeField("y", arrow::utf8(), 2),
                                     MakeField("z", arrow::float64(), 3)});
    auto read_type = arrow::struct_({MakeField("x", arrow::int32(), 1)});

    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(read_type, data_type));
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value()->num_fields(), 1);
    ASSERT_EQ(result.value()->field(0)->name(), "x");
}

TEST(NestedProjectionUtilsTest, PruneDataType_StructAllFieldsPruned) {
    // data: STRUCT<x:INT(id=1)>
    // read: STRUCT<y:INT(id=99)>  — no match
    // expected: nullopt
    auto data_type = arrow::struct_({MakeField("x", arrow::int32(), 1)});
    auto read_type = arrow::struct_({MakeField("y", arrow::int32(), 99)});

    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(read_type, data_type));
    ASSERT_FALSE(result.has_value());
}

TEST(NestedProjectionUtilsTest, PruneDataType_NestedStruct) {
    // data: STRUCT<inner:STRUCT<a:INT(id=10), b:STRING(id=11)>(id=1)>
    // read: STRUCT<inner:STRUCT<a:INT(id=10)>(id=1)>
    // expected: STRUCT<inner:STRUCT<a:INT(id=10)>(id=1)>
    auto inner_data = arrow::struct_({MakeField("a", arrow::int32(), 10),
                                      MakeField("b", arrow::utf8(), 11)});
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

TEST(NestedProjectionUtilsTest, PruneDataType_ListWithStructElement) {
    // data: LIST<STRUCT<a:INT(id=10), b:STRING(id=11)>>
    // read: LIST<STRUCT<a:INT(id=10)>>
    auto inner_data = arrow::struct_({MakeField("a", arrow::int32(), 10),
                                      MakeField("b", arrow::utf8(), 11)});
    auto data_type = arrow::list(arrow::field("item", inner_data));

    auto inner_read = arrow::struct_({MakeField("a", arrow::int32(), 10)});
    auto read_type = arrow::list(arrow::field("item", inner_read));

    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(read_type, data_type));
    ASSERT_TRUE(result.has_value());
    auto list_type = std::dynamic_pointer_cast<arrow::ListType>(result.value());
    ASSERT_NE(list_type, nullptr);
    ASSERT_EQ(list_type->value_type()->num_fields(), 1);
    ASSERT_EQ(list_type->value_type()->field(0)->name(), "a");
}

TEST(NestedProjectionUtilsTest, PruneDataType_MapWithStructValue) {
    // data: MAP<STRING, STRUCT<a:INT(id=10), b:STRING(id=11)>>
    // read: MAP<STRING, STRUCT<a:INT(id=10)>>
    auto inner_data = arrow::struct_({MakeField("a", arrow::int32(), 10),
                                      MakeField("b", arrow::utf8(), 11)});
    auto data_type = arrow::map(arrow::utf8(), inner_data);

    auto inner_read = arrow::struct_({MakeField("a", arrow::int32(), 10)});
    auto read_type = arrow::map(arrow::utf8(), inner_read);

    ASSERT_OK_AND_ASSIGN(auto result, NestedProjectionUtils::PruneDataType(read_type, data_type));
    ASSERT_TRUE(result.has_value());
    auto map_type = std::dynamic_pointer_cast<arrow::MapType>(result.value());
    ASSERT_NE(map_type, nullptr);
    ASSERT_TRUE(map_type->key_type()->Equals(arrow::utf8()));
    ASSERT_EQ(map_type->item_type()->num_fields(), 1);
    ASSERT_EQ(map_type->item_type()->field(0)->name(), "a");
}

// ============== PruneArray ==============

TEST(NestedProjectionUtilsTest, PruneArray_StructPrune) {
    // Build a StructArray with fields x:INT, y:STRING
    arrow::Int32Builder x_builder;
    ASSERT_TRUE(x_builder.AppendValues({1, 2, 3}).ok());
    std::shared_ptr<arrow::Array> x_array;
    ASSERT_TRUE(x_builder.Finish(&x_array).ok());

    arrow::StringBuilder y_builder;
    ASSERT_TRUE(y_builder.AppendValues({"a", "b", "c"}).ok());
    std::shared_ptr<arrow::Array> y_array;
    ASSERT_TRUE(y_builder.Finish(&y_array).ok());

    auto struct_type = arrow::struct_({arrow::field("x", arrow::int32()),
                                       arrow::field("y", arrow::utf8())});
    auto struct_result = arrow::StructArray::Make({x_array, y_array},
                                                   struct_type->fields());
    ASSERT_TRUE(struct_result.ok());
    auto struct_array = struct_result.ValueUnsafe();

    // Prune to only keep "x"
    auto target_type = arrow::struct_({arrow::field("x", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(auto pruned, NestedProjectionUtils::PruneArray(struct_array, target_type));

    ASSERT_EQ(pruned->type()->num_fields(), 1);
    ASSERT_EQ(pruned->type()->field(0)->name(), "x");
    ASSERT_EQ(pruned->length(), 3);
}

TEST(NestedProjectionUtilsTest, PruneArray_IdenticalType) {
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.AppendValues({10, 20}).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ASSERT_OK_AND_ASSIGN(auto pruned, NestedProjectionUtils::PruneArray(array, arrow::int32()));
    ASSERT_EQ(pruned.get(), array.get());  // Same pointer — no copy.
}

// ============== GetMapSelectedKeys ==============

TEST(NestedProjectionUtilsTest, GetMapSelectedKeys_Present) {
    auto metadata = arrow::KeyValueMetadata::Make(
        {DataField::MAP_SELECTED_KEYS}, {R"(["key1","key2","key3"])"});
    auto field = arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()), /*nullable=*/true,
                              metadata);
    auto keys = NestedProjectionUtils::GetMapSelectedKeys(field);
    ASSERT_EQ(keys.size(), 3);
    ASSERT_TRUE(keys.count("key1"));
    ASSERT_TRUE(keys.count("key2"));
    ASSERT_TRUE(keys.count("key3"));
}

TEST(NestedProjectionUtilsTest, GetMapSelectedKeys_Absent) {
    auto field = arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()));
    auto keys = NestedProjectionUtils::GetMapSelectedKeys(field);
    ASSERT_TRUE(keys.empty());
}

TEST(NestedProjectionUtilsTest, GetMapSelectedKeys_InvalidJson) {
    auto metadata = arrow::KeyValueMetadata::Make(
        {DataField::MAP_SELECTED_KEYS}, {"not_json"});
    auto field = arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()), /*nullable=*/true,
                              metadata);
    auto keys = NestedProjectionUtils::GetMapSelectedKeys(field);
    ASSERT_TRUE(keys.empty());
}

TEST(NestedProjectionUtilsTest, GetMapSelectedKeys_Nullptr) {
    auto keys = NestedProjectionUtils::GetMapSelectedKeys(nullptr);
    ASSERT_TRUE(keys.empty());
}

// ============== FilterMapArrayBySelectedKeys ==============

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

TEST(NestedProjectionUtilsTest, FilterMapArrayBySelectedKeys_Basic) {
    // Map with 3 entries each, select only "a" and "c"
    auto map_array = BuildStringInt32MapArray({
        {{"a", 1}, {"b", 2}, {"c", 3}},
        {{"a", 10}, {"d", 40}},
    });

    std::set<std::string> selected = {"a", "c"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(map_array, selected));

    auto result = std::static_pointer_cast<arrow::MapArray>(filtered);
    ASSERT_EQ(result->length(), 2);

    // First map: should have "a" and "c"
    ASSERT_EQ(result->value_length(0), 2);
    auto keys0 = std::static_pointer_cast<arrow::StringArray>(result->keys());
    ASSERT_EQ(keys0->GetString(result->value_offset(0)), "a");
    ASSERT_EQ(keys0->GetString(result->value_offset(0) + 1), "c");

    // Second map: should have only "a"
    ASSERT_EQ(result->value_length(1), 1);
    ASSERT_EQ(keys0->GetString(result->value_offset(1)), "a");
}

TEST(NestedProjectionUtilsTest, FilterMapArrayBySelectedKeys_EmptySelectedKeys) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}}});
    std::set<std::string> empty_keys;
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(map_array, empty_keys));
    // Should return original array unchanged
    ASSERT_EQ(filtered.get(), map_array.get());
}

TEST(NestedProjectionUtilsTest, FilterMapArrayBySelectedKeys_AllKept) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}, {"b", 2}}});
    std::set<std::string> selected = {"a", "b"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(map_array, selected));
    // All entries match, should return original
    ASSERT_EQ(filtered.get(), map_array.get());
}

TEST(NestedProjectionUtilsTest, FilterMapArrayBySelectedKeys_NoneKept) {
    auto map_array = BuildStringInt32MapArray({{{"a", 1}, {"b", 2}}});
    std::set<std::string> selected = {"x", "y"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(map_array, selected));
    auto result = std::static_pointer_cast<arrow::MapArray>(filtered);
    ASSERT_EQ(result->length(), 1);
    ASSERT_EQ(result->value_length(0), 0);
}

TEST(NestedProjectionUtilsTest, FilterMapArrayBySelectedKeys_WithNull) {
    // maps[0] = {"a":1}, maps[1] = null, maps[2] = {"b":2,"c":3}
    auto map_array = BuildStringInt32MapArray(
        {{{"a", 1}}, {}, {{"b", 2}, {"c", 3}}},
        {true, false, true});

    std::set<std::string> selected = {"a", "c"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(map_array, selected));
    auto result = std::static_pointer_cast<arrow::MapArray>(filtered);
    ASSERT_EQ(result->length(), 3);
    // maps[0] = {"a":1}
    ASSERT_EQ(result->value_length(0), 1);
    // maps[1] = null
    ASSERT_TRUE(result->IsNull(1));
    // maps[2] = {"c":3}
    ASSERT_EQ(result->value_length(2), 1);
    auto keys = std::static_pointer_cast<arrow::StringArray>(result->keys());
    ASSERT_EQ(keys->GetString(result->value_offset(2)), "c");
}

TEST(NestedProjectionUtilsTest, FilterMapArrayBySelectedKeys_EmptyArray) {
    auto map_array = BuildStringInt32MapArray({});
    std::set<std::string> selected = {"a"};
    ASSERT_OK_AND_ASSIGN(auto filtered, NestedProjectionUtils::FilterMapArrayBySelectedKeys(map_array, selected));
    ASSERT_EQ(filtered->length(), 0);
}

}  // namespace paimon::test
