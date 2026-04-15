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

#include "paimon/core/mergetree/compact/first_row_merge_function_wrapper.h"

#include <memory>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FirstRowMergeFunctionWrapperTest, TestSimple) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/1, /*level=*/1,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/2, /*level=*/2, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));

    auto mfunc = std::make_unique<FirstRowMergeFunction>(/*ignore_delete=*/true);

    auto contains = [](const std::shared_ptr<InternalRow>& row) { return true; };

    FirstRowMergeFunctionWrapper wrapper(std::move(mfunc), std::move(contains));
    wrapper.Reset();
    ASSERT_OK(wrapper.Add(std::move(kv1)));
    ASSERT_OK(wrapper.Add(std::move(kv2)));
    ASSERT_OK(wrapper.Add(std::move(kv3)));
    ASSERT_OK_AND_ASSIGN(auto result, wrapper.GetResult());
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().sequence_number, 0);
}

TEST(FirstRowMergeFunctionWrapperTest, TestAllLevel0WithContain) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));

    auto mfunc = std::make_unique<FirstRowMergeFunction>(/*ignore_delete=*/true);

    auto contains = [](const std::shared_ptr<InternalRow>& row) { return true; };

    FirstRowMergeFunctionWrapper wrapper(std::move(mfunc), std::move(contains));
    wrapper.Reset();
    ASSERT_OK(wrapper.Add(std::move(kv1)));
    ASSERT_OK(wrapper.Add(std::move(kv2)));
    ASSERT_OK(wrapper.Add(std::move(kv3)));
    ASSERT_OK_AND_ASSIGN(auto result, wrapper.GetResult());
    ASSERT_FALSE(result);
}

TEST(FirstRowMergeFunctionWrapperTest, TestAllLevel0WithoutContain) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));

    auto mfunc = std::make_unique<FirstRowMergeFunction>(/*ignore_delete=*/true);

    auto contains = [](const std::shared_ptr<InternalRow>& row) { return false; };

    FirstRowMergeFunctionWrapper wrapper(std::move(mfunc), std::move(contains));
    wrapper.Reset();
    ASSERT_OK(wrapper.Add(std::move(kv1)));
    ASSERT_OK(wrapper.Add(std::move(kv2)));
    ASSERT_OK(wrapper.Add(std::move(kv3)));
    ASSERT_OK_AND_ASSIGN(auto result, wrapper.GetResult());
    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().sequence_number, 0);
}

}  // namespace paimon::test
