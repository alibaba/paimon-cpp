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

#include "paimon/common/data/shredding/map_shared_shredding_batch_converter.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/data/shredding/map_shared_shredding_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/core/core_options.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {

using arrow::ipc::internal::json::ArrayFromJSON;

class MapSharedShreddingBatchConverterTest : public ::testing::Test {
 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();

    /// Builds a logical struct array from JSON, converts it, and returns the physical result.
    std::shared_ptr<arrow::Array> RunConvert(const std::shared_ptr<arrow::DataType>& logical_type,
                                             const std::string& input_json,
                                             const std::shared_ptr<arrow::DataType>& physical_type,
                                             MapSharedShreddingBatchConverter* converter) {
        auto input = ArrayFromJSON(logical_type, input_json).ValueOrDie();
        ArrowArray c_input;
        EXPECT_TRUE(arrow::ExportArray(*input, &c_input).ok());
        EXPECT_OK_AND_ASSIGN(auto c_output, converter->Convert(&c_input));
        return arrow::ImportArray(c_output.get(), physical_type).ValueOrDie();
    }

    /// Asserts that two arrays are equal, printing both on failure.
    void AssertArrayEquals(const std::shared_ptr<arrow::Array>& expected,
                           const std::shared_ptr<arrow::Array>& actual) {
        ASSERT_TRUE(expected->Equals(*actual)) << "Expected:\n"
                                               << expected->ToString() << "\nActual:\n"
                                               << actual->ToString();
    }

    Result<CoreOptions> MakeCoreOptions(const std::map<std::string, std::string>& field_to_policy) {
        std::map<std::string, std::string> options;
        for (const auto& [field_name, policy] : field_to_policy) {
            options.emplace(
                "fields." + field_name + ".map.shared-shredding.column-placement-policy", policy);
        }
        return CoreOptions::FromMap(options);
    }
};

TEST_F(MapSharedShreddingBatchConverterTest, BasicConversion) {
    // Schema: id(INT32), tags(MAP<STRING, INT64>), K=3
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto context =
        std::make_shared<MapSharedShreddingContext>(std::map<std::string, int32_t>{{"tags", 3}});
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeCoreOptions({{"tags", "plain"}}));
    ASSERT_OK_AND_ASSIGN(auto converter, MapSharedShreddingBatchConverter::Create(
                                             logical_schema, context, options, pool_));
    auto physical_schema = converter->GetPhysicalSchema();

    auto logical_type = arrow::struct_(logical_schema->fields());
    auto physical_type = arrow::struct_(physical_schema->fields());

    // Input: 2 rows — [id, tags]
    //   Row0: id=100, tags={a:1, b:2}
    //   Row1: id=200, tags={b:3, c:4, a:5}
    auto actual = RunConvert(logical_type, R"([
        [100, [["a", 1], ["b", 2]]],
        [200, [["b", 3], ["c", 4], ["a", 5]]]
    ])",
                             physical_type, converter.get());
    // Expected physical: [id, [mapping, col0, col1, col2, overflow]]
    //   Row0: a=fid0->col0, b=fid1->col1, col2 unused
    //   Row1: b=fid1->col0, c=fid2->col1, a=fid0->col2
    auto expected = ArrayFromJSON(physical_type, R"([
        [100, [[0, 1, -1], 1, 2, null, null]],
        [200, [[1, 2, 0],  3, 4, 5,    null]]
    ])")
                        .ValueOrDie();

    AssertArrayEquals(expected, actual);

    // Verify GetShreddingColumnNames
    ASSERT_EQ(std::vector<std::string>({"tags"}), converter->GetShreddingColumnNames());

    // Verify BuildFieldMeta: a=0,b=1,c=2, K=3, max_row_width=3, no overflow
    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    expected_meta.field_to_columns = {{0, {0, 2}}, {1, {0, 1}}, {2, {1}}};
    expected_meta.num_columns = 3;
    expected_meta.max_row_width = 3;
    ASSERT_EQ(expected_meta, converter->BuildFieldMeta("tags").value());
}

TEST_F(MapSharedShreddingBatchConverterTest, NestedValueStruct) {
    // MAP<STRING, STRUCT<x:INT32, y:DOUBLE>>, K=2
    auto value_type = arrow::struct_({
        arrow::field("x", arrow::int32()),
        arrow::field("y", arrow::float64()),
    });
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("props", arrow::map(arrow::utf8(), value_type)),
    });
    auto context =
        std::make_shared<MapSharedShreddingContext>(std::map<std::string, int32_t>{{"props", 2}});
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeCoreOptions({{"props", "plain"}}));
    ASSERT_OK_AND_ASSIGN(auto converter, MapSharedShreddingBatchConverter::Create(
                                             logical_schema, context, options, pool_));
    auto physical_schema = converter->GetPhysicalSchema();
    auto logical_type = arrow::struct_(logical_schema->fields());
    auto physical_type = arrow::struct_(physical_schema->fields());

    // Row0: props={a:[1,1.5], b:[null,2.5]}   → a=fid0->col0, b=fid1->col1; b.x is null
    // Row1: props={c:[3,3.5]}                  → c=fid2->col0
    // Row2: props={a:[null,null], c:[5,5.5], b:[6,6.5]} → 3 fields K=2: overflow b; a has all-null
    // struct Row3: props=null                         → null row
    auto actual = RunConvert(logical_type, R"([
        [1, [["a", [1, 1.5]], ["b", [null, 2.5]]]],
        [2, [["c", [3, 3.5]]]],
        [3, [["a", [null, null]], ["c", [5, 5.5]], ["b", [6, 6.5]]]],
        [4, null]
    ])",
                             physical_type, converter.get());

    auto expected = ArrayFromJSON(physical_type, R"([
        [1, [[0, 1],  [1, 1.5],     [null, 2.5], null]],
        [2, [[2, -1], [3, 3.5],     null,         null]],
        [3, [[0, 2],  [null, null], [5, 5.5],     [[1, [6, 6.5]]]]],
        [4, null]
    ])")
                        .ValueOrDie();

    AssertArrayEquals(expected, actual);

    // Verify GetShreddingColumnNames
    ASSERT_EQ(std::vector<std::string>({"props"}), converter->GetShreddingColumnNames());

    // Verify BuildFieldMeta: a=0,b=1,c=2; K=2, max_row_width=3, b overflowed in row2
    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    expected_meta.field_to_columns = {{0, {0}}, {1, {1}}, {2, {0, 1}}};
    expected_meta.overflow_field_set = {1};
    expected_meta.num_columns = 2;
    expected_meta.max_row_width = 3;
    ASSERT_EQ(expected_meta, converter->BuildFieldMeta("props").value());
}

TEST_F(MapSharedShreddingBatchConverterTest, NestedValueList) {
    // MAP<STRING, LIST<INT32>>, K=2
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::list(arrow::int32()))),
    });
    auto context =
        std::make_shared<MapSharedShreddingContext>(std::map<std::string, int32_t>{{"tags", 2}});
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeCoreOptions({{"tags", "plain"}}));
    ASSERT_OK_AND_ASSIGN(auto converter, MapSharedShreddingBatchConverter::Create(
                                             logical_schema, context, options, pool_));
    auto physical_schema = converter->GetPhysicalSchema();
    auto logical_type = arrow::struct_(logical_schema->fields());
    auto physical_type = arrow::struct_(physical_schema->fields());

    // Row0: tags={a:[1,null,2], b:[3]}         → a=fid0->col0, b=fid1->col1; a has null element
    // Row1: tags={a:[null]}                      → a=fid0->col0; single null element list
    // Row2: tags={c:[5,6,7]}                     → c=fid2->col0
    // Row3: tags={b:[8], a:[9,10], c:[null]}     → 3 fields K=2: overflow c; c has null element
    auto actual = RunConvert(logical_type, R"([
        [1, [["a", [1, null, 2]], ["b", [3]]]],
        [2, [["a", [null]]]],
        [3, [["c", [5, 6, 7]]]],
        [4, [["b", [8]], ["a", [9, 10]], ["c", [null]]]]
    ])",
                             physical_type, converter.get());

    auto expected = ArrayFromJSON(physical_type, R"([
        [1, [[0, 1],  [1, null, 2], [3],    null]],
        [2, [[0, -1], [null],       null,   null]],
        [3, [[2, -1], [5, 6, 7],    null,   null]],
        [4, [[1, 0],  [8],       [9, 10],   [[2, [null]]]]]
    ])")
                        .ValueOrDie();

    AssertArrayEquals(expected, actual);

    // Verify GetShreddingColumnNames
    ASSERT_EQ(std::vector<std::string>({"tags"}), converter->GetShreddingColumnNames());

    // Verify BuildFieldMeta: a=0,b=1,c=2; K=2, max_row_width=3, c overflowed in row3
    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    expected_meta.field_to_columns = {{0, {0, 1}}, {1, {0, 1}}, {2, {0}}};
    expected_meta.overflow_field_set = {2};
    expected_meta.num_columns = 2;
    expected_meta.max_row_width = 3;
    ASSERT_EQ(expected_meta, converter->BuildFieldMeta("tags").value());
}

TEST_F(MapSharedShreddingBatchConverterTest, NestedValueMap) {
    // MAP<STRING, MAP<STRING, INT32>>, K=2
    auto inner_map_type = arrow::map(arrow::utf8(), arrow::int32());
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("nested", arrow::map(arrow::utf8(), inner_map_type)),
    });
    auto context =
        std::make_shared<MapSharedShreddingContext>(std::map<std::string, int32_t>{{"nested", 2}});
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeCoreOptions({{"nested", "plain"}}));
    ASSERT_OK_AND_ASSIGN(auto converter, MapSharedShreddingBatchConverter::Create(
                                             logical_schema, context, options, pool_));
    auto physical_schema = converter->GetPhysicalSchema();
    auto logical_type = arrow::struct_(logical_schema->fields());
    auto physical_type = arrow::struct_(physical_schema->fields());

    // Row0: nested={a:{x:1,y:null}, b:{z:3}}    → a=fid0->col0, b=fid1->col1; a has null value
    // Row1: nested={c:{p:null}}                   → c=fid2->col0; inner map value all null
    // Row2: nested=null                           → null row
    // Row3: nested={a:{m:7}, b:{n:8}, c:{o:9}}   → 3 fields K=2: overflow c
    auto actual = RunConvert(logical_type, R"([
        [1, [["a", [["x", 1], ["y", null]]], ["b", [["z", 3]]]]],
        [2, [["c", [["p", null]]]]],
        [3, null],
        [4, [["a", [["m", 7]]], ["b", [["n", 8]]], ["c", [["o", 9]]]]]
    ])",
                             physical_type, converter.get());

    auto expected = ArrayFromJSON(physical_type, R"([
        [1, [[0, 1],  [["x", 1], ["y", null]], [["z", 3]],  null]],
        [2, [[2, -1], [["p", null]],            null,        null]],
        [3, null],
        [4, [[0, 1],  [["m", 7]],              [["n", 8]],  [[2, [["o", 9]]]]]]
    ])")
                        .ValueOrDie();

    AssertArrayEquals(expected, actual);

    // Verify GetShreddingColumnNames
    ASSERT_EQ(std::vector<std::string>({"nested"}), converter->GetShreddingColumnNames());

    // Verify BuildFieldMeta: a=0,b=1,c=2; K=2, max_row_width=3, c overflowed in row3
    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    expected_meta.field_to_columns = {{0, {0}}, {1, {1}}, {2, {0}}};
    expected_meta.overflow_field_set = {2};
    expected_meta.num_columns = 2;
    expected_meta.max_row_width = 3;
    ASSERT_EQ(expected_meta, converter->BuildFieldMeta("nested").value());
}

TEST_F(MapSharedShreddingBatchConverterTest, NestedComplex) {
    // MAP<STRING, STRUCT<score:INT32, tags:LIST<STRING>, meta:MAP<STRING,INT32>>>, K=2
    auto value_type = arrow::struct_({
        arrow::field("score", arrow::int32()),
        arrow::field("tags", arrow::list(arrow::utf8())),
        arrow::field("meta", arrow::map(arrow::utf8(), arrow::int32())),
    });
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("data", arrow::map(arrow::utf8(), value_type)),
    });
    auto context =
        std::make_shared<MapSharedShreddingContext>(std::map<std::string, int32_t>{{"data", 2}});
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeCoreOptions({{"data", "plain"}}));
    ASSERT_OK_AND_ASSIGN(auto converter, MapSharedShreddingBatchConverter::Create(
                                             logical_schema, context, options, pool_));
    auto physical_schema = converter->GetPhysicalSchema();
    auto logical_type = arrow::struct_(logical_schema->fields());
    auto physical_type = arrow::struct_(physical_schema->fields());

    // Row0: a=[10,["t1","t2"],{x:1}], b=[20,["t3"],{y:2,z:3}]  → a=fid0->col0, b=fid1->col1
    // Row1: c=[null,null,{p:null}]                               → c=fid2->col0; nulls inside
    // struct Row2: a=[30,[null,"t4"],{}], b=[null,[],{q:5}], c=[40,["t5"],{r:6}] → overflow c Row3:
    // null                                                 → null row
    auto actual = RunConvert(logical_type, R"([
        [1, [["a", [10, ["t1", "t2"], [["x", 1]]]], ["b", [20, ["t3"], [["y", 2], ["z", 3]]]]]],
        [2, [["c", [null, null, [["p", null]]]]]],
        [3, [["a", [30, [null, "t4"], []]], ["b", [null, [], [["q", 5]]]], ["c", [40, ["t5"], [["r", 6]]]]]],
        [4, null]
    ])",
                             physical_type, converter.get());

    auto expected = ArrayFromJSON(physical_type, R"([
        [1, [[0, 1],  [10, ["t1", "t2"], [["x", 1]]], [20, ["t3"], [["y", 2], ["z", 3]]], null]],
        [2, [[2, -1], [null, null, [["p", null]]],     null,                                null]],
        [3, [[0, 1],  [30, [null, "t4"], []],          [null, [], [["q", 5]]],              [[2, [40, ["t5"], [["r", 6]]]]]]],
        [4, null]
    ])")
                        .ValueOrDie();

    AssertArrayEquals(expected, actual);

    // Verify GetShreddingColumnNames
    ASSERT_EQ(std::vector<std::string>({"data"}), converter->GetShreddingColumnNames());

    // Verify BuildFieldMeta: a=0,b=1,c=2; K=2, max_row_width=3, c overflowed in row2
    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    expected_meta.field_to_columns = {{0, {0}}, {1, {1}}, {2, {0}}};
    expected_meta.overflow_field_set = {2};
    expected_meta.num_columns = 2;
    expected_meta.max_row_width = 3;
    ASSERT_EQ(expected_meta, converter->BuildFieldMeta("data").value());
}

TEST_F(MapSharedShreddingBatchConverterTest, MultipleMapFields) {
    // Schema: id(INT32), tags(MAP<STRING,INT64>) K=2, attrs(MAP<STRING,DOUBLE>) K=3
    // Tests that two shared-shredding MAP columns in the same schema are independently converted.
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("attrs", arrow::map(arrow::utf8(), arrow::float64())),
    });
    auto context = std::make_shared<MapSharedShreddingContext>(
        std::map<std::string, int32_t>{{"tags", 2}, {"attrs", 3}});
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         MakeCoreOptions({{"tags", "plain"}, {"attrs", "plain"}}));
    ASSERT_OK_AND_ASSIGN(auto converter, MapSharedShreddingBatchConverter::Create(
                                             logical_schema, context, options, pool_));
    auto physical_schema = converter->GetPhysicalSchema();
    auto logical_type = arrow::struct_(logical_schema->fields());
    auto physical_type = arrow::struct_(physical_schema->fields());

    // Row0: id=1, tags={a:10, b:20}, attrs={x:1.1, y:2.2}
    //   tags: a=fid0->col0, b=fid1->col1 (fits K=2)
    //   attrs: x=fid0->col0, y=fid1->col1, col2 unused (fits K=3)
    // Row1: id=2, tags={c:30, a:40, b:50}, attrs={z:3.3}
    //   tags: c=fid2->col0, a=fid0->col1; b overflows (K=2)
    //   attrs: z=fid2->col0, col1/col2 unused
    // Row2: id=3, tags=null, attrs={x:4.4, y:5.5, z:6.6, w:7.7}
    //   tags: null
    //   attrs: x=fid0->col0, y=fid1->col1, z=fid2->col2; w overflows (K=3)
    auto actual = RunConvert(logical_type, R"([
        [1, [["a", 10], ["b", 20]],         [["x", 1.1], ["y", 2.2]]],
        [2, [["c", 30], ["a", 40], ["b", 50]], [["z", 3.3]]],
        [3, null,                            [["x", 4.4], ["y", 5.5], ["z", 6.6], ["w", 7.7]]]
    ])",
                             physical_type, converter.get());

    auto expected = ArrayFromJSON(physical_type, R"([
        [1, [[0, 1],  10, 20, null],            [[0, 1, -1], 1.1, 2.2, null, null]],
        [2, [[2, 0],  30, 40, [[1, 50]]],       [[2, -1, -1], 3.3, null, null, null]],
        [3, null,                                [[0, 1, 2], 4.4, 5.5, 6.6, [[3, 7.7]]]]
    ])")
                        .ValueOrDie();

    AssertArrayEquals(expected, actual);

    // Verify GetShreddingColumnNames returns both columns in order
    ASSERT_EQ(std::vector<std::string>({"tags", "attrs"}), converter->GetShreddingColumnNames());

    // Verify BuildFieldMeta for tags: a=0,b=1,c=2; K=2, max_row_width=3
    MapSharedShreddingFieldMeta tags_meta;
    tags_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    tags_meta.field_to_columns = {{0, {0, 1}}, {1, {1}}, {2, {0}}};
    tags_meta.overflow_field_set = {1};
    tags_meta.num_columns = 2;
    tags_meta.max_row_width = 3;
    ASSERT_EQ(tags_meta, converter->BuildFieldMeta("tags").value());

    // Verify BuildFieldMeta for attrs: x=0,y=1,z=2,w=3; K=3, max_row_width=4
    MapSharedShreddingFieldMeta attrs_meta;
    attrs_meta.name_to_id = {{"x", 0}, {"y", 1}, {"z", 2}, {"w", 3}};
    attrs_meta.field_to_columns = {{0, {0}}, {1, {1}}, {2, {0, 2}}};
    attrs_meta.overflow_field_set = {3};
    attrs_meta.num_columns = 3;
    attrs_meta.max_row_width = 4;
    ASSERT_EQ(attrs_meta, converter->BuildFieldMeta("attrs").value());
}

TEST_F(MapSharedShreddingBatchConverterTest, BuildFieldMetaInvalidFieldName) {
    // Schema: id(INT32), tags(MAP<STRING, INT64>), K=3
    // Only "tags" is a shredding field
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto context =
        std::make_shared<MapSharedShreddingContext>(std::map<std::string, int32_t>{{"tags", 3}});
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeCoreOptions({{"tags", "plain"}}));
    ASSERT_OK_AND_ASSIGN(auto converter, MapSharedShreddingBatchConverter::Create(
                                             logical_schema, context, options, pool_));

    // Valid case: "tags" exists
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] auto meta, converter->BuildFieldMeta("tags"));

    // Invalid case: "id" is not a shredding field
    ASSERT_NOK_WITH_MSG(converter->BuildFieldMeta("id"), "cannot find field_name 'id'");

    // Invalid case: nonexistent field name
    ASSERT_NOK_WITH_MSG(converter->BuildFieldMeta("nonexistent"),
                        "cannot find field_name 'nonexistent'");
}

TEST_F(MapSharedShreddingBatchConverterTest, SequentialPlacementUsesSmallestColumn) {
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto context =
        std::make_shared<MapSharedShreddingContext>(std::map<std::string, int32_t>{{"tags", 3}});
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeCoreOptions({{"tags", "sequential"}}));
    ASSERT_OK_AND_ASSIGN(auto converter, MapSharedShreddingBatchConverter::Create(
                                             logical_schema, context, options, pool_));
    auto physical_schema = converter->GetPhysicalSchema();

    auto logical_type = arrow::struct_(logical_schema->fields());
    auto physical_type = arrow::struct_(physical_schema->fields());

    auto actual = RunConvert(logical_type, R"([
        [100, [["a", 1], ["b", 2]]],
        [200, [["b", 3], ["c", 4], ["a", 5]]]
    ])",
                             physical_type, converter.get());

    auto expected = ArrayFromJSON(physical_type, R"([
        [100, [[0, 1, -1], 1, 2, null, null]],
        [200, [[0, 1, 2],  5, 3,    4, null]]
    ])")
                        .ValueOrDie();

    AssertArrayEquals(expected, actual);
}

TEST_F(MapSharedShreddingBatchConverterTest, LruPlacementPreservesResidentColumns) {
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto context =
        std::make_shared<MapSharedShreddingContext>(std::map<std::string, int32_t>{{"tags", 3}});
    ASSERT_OK_AND_ASSIGN(CoreOptions options, MakeCoreOptions({{"tags", "lru"}}));
    ASSERT_OK_AND_ASSIGN(auto converter, MapSharedShreddingBatchConverter::Create(
                                             logical_schema, context, options, pool_));
    auto physical_schema = converter->GetPhysicalSchema();

    auto logical_type = arrow::struct_(logical_schema->fields());
    auto physical_type = arrow::struct_(physical_schema->fields());

    auto actual = RunConvert(logical_type, R"([
        [1, [["a", 10], ["b", 20], ["c", 30]]],
        [2, [["a", 40], ["b", 50]]],
        [3, [["d", 60], ["e", 70], ["f", 80]]],
        [4, [["a", 90], ["d", 100], ["e", 110], ["f", 120]]]
    ])",
                             physical_type, converter.get());

    auto expected = ArrayFromJSON(physical_type, R"([
        [1, [[0, 1, 2],  10,  20,  30, null]],
        [2, [[0, 1, -1], 40,  50, null, null]],
        [3, [[4, 5, 3],  70,  80,  60, null]],
        [4, [[4, 5, 3], 110, 120, 100, [[0, 90]]]]
    ])")
                        .ValueOrDie();

    AssertArrayEquals(expected, actual);

    MapSharedShreddingFieldMeta expected_meta;
    expected_meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}, {"d", 3}, {"e", 4}, {"f", 5}};
    expected_meta.field_to_columns = {{0, {0}}, {1, {1}}, {2, {2}}, {3, {2}}, {4, {0}}, {5, {1}}};
    expected_meta.overflow_field_set = {0};
    expected_meta.num_columns = 3;
    expected_meta.max_row_width = 4;
    ASSERT_EQ(expected_meta, converter->BuildFieldMeta("tags").value());
}

}  // namespace paimon
