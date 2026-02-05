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
#include "gtest/gtest.h"
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

}  // namespace paimon::test
