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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/key_value.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
class KeyValueChecker : public testing::Test {
 public:
    // only support INT field
    static void CheckResult(const std::vector<KeyValue>& expected,
                            const std::vector<KeyValue>& result, int32_t key_arity,
                            int32_t value_arity) {
        ASSERT_EQ(expected.size(), result.size());
        for (size_t i = 0; i < expected.size(); i++) {
            CheckResult(expected[i], result[i], key_arity, value_arity);
        }
    }

    static void CheckResult(const KeyValue& expected, const KeyValue& result, int32_t key_arity,
                            int32_t value_arity) {
        ASSERT_EQ(*(expected.value_kind), *(result.value_kind));
        ASSERT_EQ(expected.sequence_number, result.sequence_number);
        ASSERT_EQ(expected.level, result.level);
        ASSERT_EQ(key_arity, result.key->GetFieldCount());
        ASSERT_EQ(value_arity, result.value->GetFieldCount());
        for (int32_t k = 0; k < key_arity; k++) {
            ASSERT_EQ(expected.key->GetInt(k), result.key->GetInt(k));
        }
        for (int32_t v = 0; v < value_arity; v++) {
            if (expected.value->IsNullAt(v)) {
                ASSERT_TRUE(result.value->IsNullAt(v));
            } else {
                ASSERT_EQ(expected.value->GetInt(v), result.value->GetInt(v));
            }
        }
    }

    // support non-nested field
    static void CheckResult(const std::vector<KeyValue>& expected_vec,
                            const std::vector<KeyValue>& result_vec,
                            const std::vector<DataField>& key_fields,
                            const std::vector<DataField>& value_fields) {
        ASSERT_EQ(expected_vec.size(), result_vec.size());
        ASSERT_OK_AND_ASSIGN(
            auto key_comparator,
            FieldsComparator::Create(key_fields, /*is_ascending_order=*/true, /*use_view=*/false));
        ASSERT_OK_AND_ASSIGN(auto value_comparator,
                             FieldsComparator::Create(value_fields, /*is_ascending_order=*/true,
                                                      /*use_view=*/false));
        for (size_t i = 0; i < expected_vec.size(); i++) {
            const auto& expected = expected_vec[i];
            const auto& result = result_vec[i];
            ASSERT_EQ(*(expected.value_kind), *(result.value_kind));
            ASSERT_EQ(expected.sequence_number, result.sequence_number);
            ASSERT_EQ(expected.level, result.level);
            ASSERT_EQ(key_fields.size(), result.key->GetFieldCount());
            ASSERT_EQ(value_fields.size(), result.value->GetFieldCount());
            ASSERT_EQ(0, key_comparator->CompareTo(*expected.key, *result.key));
            ASSERT_EQ(0, value_comparator->CompareTo(*expected.value, *result.value));
        }
    }

    static std::vector<KeyValue> GenerateKeyValues(
        const std::vector<int64_t>& seq_vec,
        const std::vector<BinaryRowGenerator::ValueType>& key_vec,
        const std::vector<BinaryRowGenerator::ValueType>& value_vec,
        const std::shared_ptr<MemoryPool>& pool) {
        // default row kind is insert, level is 0
        std::vector<RowKind*> row_kind_vec(key_vec.size(), const_cast<RowKind*>(RowKind::Insert()));
        std::vector<int64_t> level_vec(key_vec.size(), 0);
        return GenerateKeyValues(row_kind_vec, seq_vec, level_vec, key_vec, value_vec, pool);
    }

    static std::vector<KeyValue> GenerateKeyValues(
        const std::vector<RowKind*>& row_kind_vec, const std::vector<int64_t>& seq_vec,
        const std::vector<int64_t>& level_vec,
        const std::vector<BinaryRowGenerator::ValueType>& key_vec,
        const std::vector<BinaryRowGenerator::ValueType>& value_vec,
        const std::shared_ptr<MemoryPool>& pool) {
        EXPECT_EQ(row_kind_vec.size(), key_vec.size());
        EXPECT_EQ(seq_vec.size(), key_vec.size());
        EXPECT_EQ(level_vec.size(), key_vec.size());
        EXPECT_EQ(value_vec.size(), key_vec.size());
        std::vector<KeyValue> results;
        for (size_t i = 0; i < seq_vec.size(); i++) {
            results.emplace_back(
                row_kind_vec[i], /*sequence_number=*/seq_vec[i], /*level=*/level_vec[i],
                /*key=*/BinaryRowGenerator::GenerateRowPtr(key_vec[i], pool.get()),
                /*value=*/BinaryRowGenerator::GenerateRowPtr(value_vec[i], pool.get()));
        }
        return results;
    }
};
}  // namespace paimon::test
