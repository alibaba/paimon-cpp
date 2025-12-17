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

#include "paimon/common/table/special_fields.h"

#include <memory>

#include "arrow/type.h"
#include "gtest/gtest.h"

namespace paimon::test {

TEST(SpecialFieldsTest, TestSequenceNumberField) {
    ASSERT_EQ(SpecialFields::SequenceNumber().Id(), std::numeric_limits<int32_t>::max() - 1);
    ASSERT_EQ(SpecialFields::SequenceNumber().Name(), "_SEQUENCE_NUMBER");
    ASSERT_EQ(SpecialFields::SequenceNumber().Type()->id(), arrow::Type::INT64);
}

TEST(SpecialFieldsTest, TestValueKindField) {
    ASSERT_EQ(SpecialFields::ValueKind().Id(), std::numeric_limits<int32_t>::max() - 2);
    ASSERT_EQ(SpecialFields::ValueKind().Name(), "_VALUE_KIND");
    ASSERT_EQ(SpecialFields::ValueKind().Type()->id(), arrow::Type::INT8);
}

TEST(SpecialFieldsTest, TestRowIdField) {
    ASSERT_EQ(SpecialFields::RowId().Id(), std::numeric_limits<int32_t>::max() - 5);
    ASSERT_EQ(SpecialFields::RowId().Name(), "_ROW_ID");
    ASSERT_EQ(SpecialFields::RowId().Type()->id(), arrow::Type::INT64);
}

TEST(SpecialFieldsTest, TestIndexScore) {
    ASSERT_EQ(SpecialFields::IndexScore().Id(), std::numeric_limits<int32_t>::max() - 10000 - 1);
    ASSERT_EQ(SpecialFields::IndexScore().Name(), "_INDEX_SCORE");
    ASSERT_EQ(SpecialFields::IndexScore().Type()->id(), arrow::Type::FLOAT);
}

TEST(SpecialFieldsTest, TestKeyValueSpecialFieldCount) {
    ASSERT_EQ(SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT, 2);
}

TEST(SpecialFieldsTest, TestIsSpecialFieldName) {
    ASSERT_TRUE(SpecialFields::IsSpecialFieldName("_SEQUENCE_NUMBER"));
    ASSERT_TRUE(SpecialFields::IsSpecialFieldName("_VALUE_KIND"));
    ASSERT_FALSE(SpecialFields::IsSpecialFieldName("VALUE_KIND"));
    ASSERT_TRUE(SpecialFields::IsSpecialFieldName("_ROW_ID"));
    ASSERT_TRUE(SpecialFields::IsSpecialFieldName("_INDEX_SCORE"));
}

}  // namespace paimon::test
