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

#include "paimon/core/utils/primary_key_table_utils.h"

#include <cstdint>
#include <map>
#include <utility>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/defs.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(PrimaryKeyTableUtilsTest, TestCreateSequenceFieldsComparator) {
    {
        std::vector<DataField> value_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                               DataField(1, arrow::field("v1", arrow::int32())),
                                               DataField(1, arrow::field("s0", arrow::int32())),
                                               DataField(1, arrow::field("s1", arrow::int32()))};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                             CoreOptions::FromMap({{Options::SEQUENCE_FIELD, "s0,s1"}}));
        ASSERT_OK_AND_ASSIGN(auto comparator, PrimaryKeyTableUtils::CreateSequenceFieldsComparator(
                                                  value_fields, core_options));
        ASSERT_EQ(comparator->CompareFields(), std::vector<int32_t>({2, 3}));
    }
    {
        std::vector<DataField> value_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                               DataField(1, arrow::field("v1", arrow::int32()))};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                             CoreOptions::FromMap({{Options::SEQUENCE_FIELD, "s0,s1"}}));
        ASSERT_NOK_WITH_MSG(
            PrimaryKeyTableUtils::CreateSequenceFieldsComparator(value_fields, core_options),
            "sequence field s0 does not in value fields");
    }
}

}  // namespace paimon::test
