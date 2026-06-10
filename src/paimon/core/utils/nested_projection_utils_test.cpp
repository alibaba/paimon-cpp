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
    ASSERT_EQ(GetPaimonFieldId(field), 42);
}

TEST(NestedProjectionUtilsTest, GetPaimonFieldId_Missing) {
    auto field = arrow::field("col", arrow::int32());
    ASSERT_EQ(GetPaimonFieldId(field), -1);
}

TEST(NestedProjectionUtilsTest, GetPaimonFieldId_Nullptr) {
    ASSERT_EQ(GetPaimonFieldId(nullptr), -1);
}

// ============== FindFieldByPaimonId ==============

TEST(NestedProjectionUtilsTest, FindFieldByPaimonId_Found) {
    auto struct_type = arrow::struct_({MakeField("x", arrow::int32(), 1),
                                       MakeField("y", arrow::utf8(), 2)});
    auto found = FindFieldByPaimonId(struct_type, 2);
    ASSERT_NE(found, nullptr);
    ASSERT_EQ(found->name(), "y");
}

TEST(NestedProjectionUtilsTest, FindFieldByPaimonId_NotFound) {
    auto struct_type = arrow::struct_({MakeField("x", arrow::int32(), 1)});
    ASSERT_EQ(FindFieldByPaimonId(struct_type, 99), nullptr);
}

TEST(NestedProjectionUtilsTest, FindFieldByPaimonId_NonStruct) {
    ASSERT_EQ(FindFieldByPaimonId(arrow::int32(), 1), nullptr);
}

// ============== PruneDataType ==============

TEST(NestedProjectionUtilsTest, PruneDataType_IdenticalTypes) {
    auto type = arrow::int32();
    ASSERT_OK_AND_ASSIGN(auto result, PruneDataType(type, type));
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value()->Equals(type));
}

TEST(NestedProjectionUtilsTest, PruneDataType_AtomicType) {
    // Different atomic types: return data_type
    auto read_type = arrow::int64();
    auto data_type = arrow::int32();
    ASSERT_OK_AND_ASSIGN(auto result, PruneDataType(read_type, data_type));
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

    ASSERT_OK_AND_ASSIGN(auto result, PruneDataType(read_type, data_type));
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

    ASSERT_OK_AND_ASSIGN(auto result, PruneDataType(read_type, data_type));
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

    ASSERT_OK_AND_ASSIGN(auto result, PruneDataType(read_type, data_type));
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

    ASSERT_OK_AND_ASSIGN(auto result, PruneDataType(read_type, data_type));
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

    ASSERT_OK_AND_ASSIGN(auto result, PruneDataType(read_type, data_type));
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
    ASSERT_OK_AND_ASSIGN(auto pruned, PruneArray(struct_array, target_type));

    ASSERT_EQ(pruned->type()->num_fields(), 1);
    ASSERT_EQ(pruned->type()->field(0)->name(), "x");
    ASSERT_EQ(pruned->length(), 3);
}

TEST(NestedProjectionUtilsTest, PruneArray_IdenticalType) {
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.AppendValues({10, 20}).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ASSERT_OK_AND_ASSIGN(auto pruned, PruneArray(array, arrow::int32()));
    ASSERT_EQ(pruned.get(), array.get());  // Same pointer — no copy.
}

}  // namespace paimon::test
