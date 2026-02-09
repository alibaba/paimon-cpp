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

#include "paimon/common/utils/arrow/arrow_utils.h"

#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {

TEST(ArrowUtilsTest, TestCreateProjection) {
    arrow::FieldVector file_fields = {
        arrow::field("k0", arrow::int32()),   arrow::field("k1", arrow::int32()),
        arrow::field("p1", arrow::int32()),   arrow::field("s1", arrow::utf8()),
        arrow::field("v0", arrow::float64()), arrow::field("v1", arrow::boolean()),
        arrow::field("s0", arrow::utf8())};
    auto file_schema = arrow::schema(file_fields);

    {
        // normal case
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()), arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()), arrow::field("v0", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_OK_AND_ASSIGN(std::vector<int32_t> projection,
                             ArrowUtils::CreateProjection(file_schema, read_schema->fields()));
        std::vector<int32_t> expected_projection = {1, 2, 3, 4, 5};
        ASSERT_EQ(projection, expected_projection);
    }
    {
        // duplicate read field
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()),   arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()),    arrow::field("v0", arrow::float64()),
            arrow::field("v0", arrow::float64()), arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_OK_AND_ASSIGN(std::vector<int32_t> projection,
                             ArrowUtils::CreateProjection(file_schema, read_schema->fields()));
        std::vector<int32_t> expected_projection = {1, 2, 3, 4, 4, 5};
        ASSERT_EQ(projection, expected_projection);
    }
    {
        // duplicate read field, and sizeof(read_fields) > sizeof(file_fields)
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()),   arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()),    arrow::field("v0", arrow::float64()),
            arrow::field("v0", arrow::float64()), arrow::field("v0", arrow::float64()),
            arrow::field("v0", arrow::float64()), arrow::field("v0", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_OK_AND_ASSIGN(std::vector<int32_t> projection,
                             ArrowUtils::CreateProjection(file_schema, read_schema->fields()));
        std::vector<int32_t> expected_projection = {1, 2, 3, 4, 4, 4, 4, 4, 5};
        ASSERT_EQ(projection, expected_projection);
    }
    {
        // read field not found in file schema
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()), arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()), arrow::field("v2", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_NOK_WITH_MSG(ArrowUtils::CreateProjection(file_schema, read_schema->fields()),
                            "Field 'v2' not found or duplicate in file schema");
    }
    {
        // duplicate field in file schema
        arrow::FieldVector file_fields_dup = {
            arrow::field("k0", arrow::int32()),   arrow::field("k1", arrow::int32()),
            arrow::field("p1", arrow::int32()),   arrow::field("s1", arrow::utf8()),
            arrow::field("v0", arrow::float64()), arrow::field("v1", arrow::boolean()),
            arrow::field("v1", arrow::boolean()), arrow::field("s0", arrow::utf8())};
        auto file_schema_dup = arrow::schema(file_fields_dup);
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()), arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()), arrow::field("v1", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_NOK_WITH_MSG(ArrowUtils::CreateProjection(file_schema_dup, read_schema->fields()),
                            "Field 'v1' not found or duplicate in file schema");
    }
    {
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()), arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()), arrow::field("v0", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_OK_AND_ASSIGN(std::vector<int32_t> projection,
                             ArrowUtils::CreateProjection(file_schema, read_schema->fields()));
        std::vector<int32_t> expected_projection = {1, 2, 3, 4, 5};
        ASSERT_EQ(projection, expected_projection);
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchSimple) {
    auto field = arrow::field("column1", arrow::int32(), /*nullable=*/false);
    auto schema = arrow::schema({field});
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
      [20],
      [null],
      [10]
])")
                .ValueOrDie();

        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field column1 not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
      [20],
      [10]
])")
                .ValueOrDie();

        ASSERT_OK(ArrowUtils::CheckNullabilityMatch(schema, array));
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchWithStruct) {
    auto child1 = arrow::field("child1", arrow::int32(), /*nullable=*/false);
    auto child2 = arrow::field("child2", arrow::float64(), /*nullable=*/true);
    auto struct_field =
        arrow::field("parent", arrow::struct_({child1, child2}), /*nullable=*/false);
    auto schema = arrow::schema({struct_field});
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({struct_field}), R"([
      [null]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field parent not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({struct_field}), R"([
      [[1, null]],
      [[null, 10.0]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field child1 not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({struct_field}), R"([
      [[1, null]],
      [[2, 10.0]]
])")
                .ValueOrDie();
        ASSERT_OK(ArrowUtils::CheckNullabilityMatch(schema, array));
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchWithList) {
    auto value_field = arrow::field("value", arrow::int32(), /*nullable=*/false);
    auto list_field = arrow::field("list_column", arrow::list(value_field), /*nullable=*/false);
    auto schema = arrow::schema({list_field});

    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({list_field}), R"([
      [[1, 2, null, 4, 5]],
      [null]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(ArrowUtils::CheckNullabilityMatch(schema, array),
                            "CheckNullabilityMatch failed, field list_column not nullable while "
                            "data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({list_field}), R"([
      [[1, 2, null, 4, 5]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field value not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({list_field}), R"([
      [[1, 2, 3, 4, 5]]
])")
                .ValueOrDie();
        ASSERT_OK(ArrowUtils::CheckNullabilityMatch(schema, array));
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchWithMap) {
    auto key_field = arrow::field("key", arrow::int32(), /*nullable=*/false);
    auto value_field = arrow::field("value", arrow::int32(), /*nullable=*/true);
    auto map_type = std::make_shared<arrow::MapType>(key_field, value_field);
    auto map_field = arrow::field("map_column", map_type, /*nullable=*/false);
    auto schema = arrow::schema({map_field});

    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({map_field}), R"([
      [null]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(ArrowUtils::CheckNullabilityMatch(schema, array),
                            "CheckNullabilityMatch failed, field map_column not nullable while "
                            "data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({map_field}), R"([
      [[[1, null]]]
])")
                .ValueOrDie();
        ASSERT_OK(ArrowUtils::CheckNullabilityMatch(schema, array));
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchComplex) {
    auto key_field = arrow::field("key", arrow::int32(), /*nullable=*/false);
    auto value_field = arrow::field("value", arrow::int32(), /*nullable=*/false);

    auto inner_child1 =
        arrow::field("inner1",
                     arrow::map(arrow::utf8(), arrow::field("inner_list", arrow::list(value_field),
                                                            /*nullable=*/true)),
                     /*nullable=*/false);
    auto inner_child2 = arrow::field(
        "inner2",
        arrow::map(arrow::utf8(), arrow::field("inner_map", arrow::map(arrow::utf8(), value_field),
                                               /*nullable=*/true)),
        /*nullable=*/false);
    auto inner_child3 = arrow::field(
        "inner3",
        arrow::map(arrow::utf8(),
                   arrow::field("inner_struct", arrow::struct_({key_field, value_field}),
                                /*nullable=*/true)),
        /*nullable=*/false);

    auto schema = arrow::schema({inner_child1, inner_child2, inner_child3});
    // test inner1
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3, null]]], [["outer_key", [["key1", 1]]]], [["outer_key", [100, 200]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field value not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", [["key1", 1]]]], [["outer_key", [100, 200]]]],
[[["outer_key", null]], [["outer_key", [["key1", 1]]]], [["outer_key", [100, 200]]]],
[null, [["outer_key", [["key1", 1]]]], [["outer_key", [100, 200]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field inner1 not nullable while data have null value");
    }
    // test inner2
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", null]], [["outer_key", [100, 200]]]],
[[["outer_key", null]], [["outer_key", [["key1", null]]]], [["outer_key", [100, 200]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field value not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", null]], [["outer_key", [100, 200]]]],
[[["outer_key", [1, 2, 3]]], null, [["outer_key", [100, 200]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field inner2 not nullable while data have null value");
    }
    // test inner3
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", null]], [["outer_key", null]]],
[[["outer_key", null]], [["outer_key", [["key1", 2]]]], [["outer_key", [100, null]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field value not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", null]], [["outer_key", null]]],
[[["outer_key", null]], [["outer_key", [["key1", 2]]]], null]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field inner3 not nullable while data have null value");
    }
}

}  // namespace paimon::test
