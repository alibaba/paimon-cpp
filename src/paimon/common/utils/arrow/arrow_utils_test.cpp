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
        // read field not found in src schema
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()), arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()), arrow::field("v2", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_NOK_WITH_MSG(ArrowUtils::CreateProjection(file_schema, read_schema->fields()),
                            "Field 'v2' not found or duplicate in src schema");
    }
    {
        // duplicate field in src schema
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
                            "Field 'v1' not found or duplicate in src schema");
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

TEST(ArrowUtilsTest, TestRemoveFieldFromStructArrayFieldNotFound) {
    auto struct_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                         struct_type, R"([{"a":1,"b":"x"},{"a":2,"b":"y"},{"a":3,"b":"z"}])")
                         .ValueOrDie();
    auto src_struct_array = std::static_pointer_cast<arrow::StructArray>(src_array);

    ASSERT_OK_AND_ASSIGN(auto result,
                         ArrowUtils::RemoveFieldFromStructArray(src_struct_array, "missing"));

    ASSERT_TRUE(result->Equals(src_struct_array));
    ASSERT_EQ(result->type()->num_fields(), 2);
}

TEST(ArrowUtilsTest, TestRemoveFieldFromStructArraySuccess) {
    auto struct_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8()),
                        arrow::field("c", arrow::int64())});
    auto src_array =
        arrow::ipc::internal::json::ArrayFromJSON(
            struct_type,
            R"([{"a":1,"b":"x","c":10},{"a":2,"b":"y","c":20},{"a":3,"b":"z","c":30}])")
            .ValueOrDie();
    auto src_struct_array = std::static_pointer_cast<arrow::StructArray>(src_array);

    ASSERT_OK_AND_ASSIGN(auto result,
                         ArrowUtils::RemoveFieldFromStructArray(src_struct_array, "b"));

    auto expected_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("c", arrow::int64())});
    auto expected_array = arrow::ipc::internal::json::ArrayFromJSON(
                              expected_type, R"([{"a":1,"c":10},{"a":2,"c":20},{"a":3,"c":30}])")
                              .ValueOrDie();
    auto expected_struct_array = std::static_pointer_cast<arrow::StructArray>(expected_array);

    ASSERT_EQ(result->type()->num_fields(), 2);
    ASSERT_EQ(result->type()->field(0)->name(), "a");
    ASSERT_EQ(result->type()->field(1)->name(), "c");
    ASSERT_TRUE(result->Equals(expected_struct_array));
}

TEST(ArrowUtilsTest, TestEqualsIgnoreNullable) {
    {
        // test simple
        ASSERT_FALSE(ArrowUtils::EqualsIgnoreNullable(arrow::int32(), arrow::int64()));
        ASSERT_TRUE(ArrowUtils::EqualsIgnoreNullable(arrow::int32(), arrow::int32()));
    }
    {
        // test struct
        auto child1 = arrow::field("child1", arrow::int32(), /*nullable=*/false);
        auto child2 = arrow::field("child2", arrow::int32(), /*nullable=*/false);
        auto child3 = arrow::field("child1", arrow::int32(), /*nullable=*/true);
        auto struct_type1 = arrow::struct_({child1});
        auto struct_type2 = arrow::struct_({child2});
        auto struct_type3 = arrow::struct_({child3});
        auto struct_type4 = arrow::struct_({child3, child1});
        ASSERT_FALSE(ArrowUtils::EqualsIgnoreNullable(struct_type1, struct_type2));
        ASSERT_TRUE(ArrowUtils::EqualsIgnoreNullable(struct_type1, struct_type3));
        ASSERT_FALSE(ArrowUtils::EqualsIgnoreNullable(struct_type1, struct_type4));
    }
    {
        // test complex
        auto key_field = arrow::field("key", arrow::int32(), /*nullable=*/false);
        auto value_field = arrow::field("value", arrow::int32(), /*nullable=*/false);
        auto inner_child1 = arrow::field(
            "inner1",
            arrow::map(arrow::utf8(), arrow::field("inner_list", arrow::list(value_field),
                                                   /*nullable=*/true)),
            /*nullable=*/false);
        auto inner_child2 = arrow::field(
            "inner2",
            arrow::map(arrow::utf8(),
                       arrow::field("inner_map", arrow::map(arrow::utf8(), value_field),
                                    /*nullable=*/true)),
            /*nullable=*/false);
        auto inner_child3 = arrow::field(
            "inner3",
            arrow::map(arrow::utf8(),
                       arrow::field("inner_struct", arrow::struct_({key_field, value_field}),
                                    /*nullable=*/true)),
            /*nullable=*/false);
        auto struct_type1 = arrow::struct_({inner_child1, inner_child2, inner_child3});

        auto key_field_other = arrow::field("key", arrow::int32(), /*nullable=*/true);
        auto value_field_other = arrow::field("value", arrow::int32(), /*nullable=*/true);
        auto inner_child1_other = arrow::field(
            "inner1",
            arrow::map(arrow::utf8(), arrow::field("inner_list", arrow::list(value_field_other),
                                                   /*nullable=*/false)),
            /*nullable=*/true);
        auto inner_child2_other = arrow::field(
            "inner2",
            arrow::map(arrow::utf8(),
                       arrow::field("inner_map", arrow::map(arrow::utf8(), value_field_other),
                                    /*nullable=*/false)),
            /*nullable=*/true);
        auto inner_child3_other = arrow::field(
            "inner3",
            arrow::map(
                arrow::utf8(),
                arrow::field("inner_struct", arrow::struct_({key_field_other, value_field_other}),
                             /*nullable=*/false)),
            /*nullable=*/true);
        auto struct_type2 =
            arrow::struct_({inner_child1_other, inner_child2_other, inner_child3_other});
        ASSERT_TRUE(ArrowUtils::EqualsIgnoreNullable(struct_type1, struct_type2));
    }
}

TEST(ArrowUtilsTest, TestGetCompressionType) {
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType(""));
        ASSERT_EQ(type, arrow::Compression::UNCOMPRESSED);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("none"));
        ASSERT_EQ(type, arrow::Compression::UNCOMPRESSED);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("uncompressed"));
        ASSERT_EQ(type, arrow::Compression::UNCOMPRESSED);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("zstd"));
        ASSERT_EQ(type, arrow::Compression::ZSTD);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("ZSTD"));
        ASSERT_EQ(type, arrow::Compression::ZSTD);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("lz4"));
        ASSERT_EQ(type, arrow::Compression::LZ4_FRAME);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("snappy"));
        ASSERT_EQ(type, arrow::Compression::SNAPPY);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("gzip"));
        ASSERT_EQ(type, arrow::Compression::GZIP);
    }
    {
        ASSERT_NOK(ArrowUtils::GetCompressionType("invalid_codec"));
    }
}

}  // namespace paimon::test
