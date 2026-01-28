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
#include "paimon/common/types/data_field.h"

namespace paimon::test {

TEST(ArrowUtilsTest, TestCreateProjection) {
    std::vector<DataField> read_fields = {DataField(1, arrow::field("k1", arrow::int32())),
                                          DataField(3, arrow::field("p1", arrow::int32())),
                                          DataField(5, arrow::field("s1", arrow::utf8())),
                                          DataField(6, arrow::field("v0", arrow::float64())),
                                          DataField(7, arrow::field("v1", arrow::boolean()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    std::vector<DataField> file_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                          DataField(1, arrow::field("k1", arrow::int32())),
                                          DataField(3, arrow::field("p1", arrow::int32())),
                                          DataField(5, arrow::field("s1", arrow::utf8())),
                                          DataField(6, arrow::field("v0", arrow::float64())),
                                          DataField(7, arrow::field("v1", arrow::boolean())),
                                          DataField(4, arrow::field("s0", arrow::utf8()))};
    auto file_schema = DataField::ConvertDataFieldsToArrowSchema(file_fields);

    auto projection = ArrowUtils::CreateProjection(file_schema, read_schema->fields());
    std::vector<int32_t> expected_projection = {1, 2, 3, 4, 5};
    ASSERT_EQ(projection, expected_projection);
}

}  // namespace paimon::test
