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

#include "paimon/core/mergetree/compact/aggregate/field_listagg_agg.h"

#include <map>
#include <string>
#include <string_view>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/core/core_options.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FieldListaggAggTest : public testing::Test {
 protected:
    static Result<std::unique_ptr<FieldListaggAgg>> MakeAgg(const std::string& delimiter = ",",
                                                            bool distinct = false) {
        std::map<std::string, std::string> opts;
        opts["fields.f.list-agg-delimiter"] = delimiter;
        opts["fields.f.distinct"] = distinct ? "true" : "false";
        PAIMON_ASSIGN_OR_RAISE(auto options, CoreOptions::FromMap(opts));
        return FieldListaggAgg::Create(arrow::utf8(), std::move(options), "f");
    }
};

TEST_F(FieldListaggAggTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(auto agg, MakeAgg());
    auto ret = agg->Agg(std::string_view("hello"), std::string_view(" world"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "hello, world");
}

TEST_F(FieldListaggAggTest, TestDelimiter) {
    ASSERT_OK_AND_ASSIGN(auto agg, MakeAgg("-"));
    auto ret = agg->Agg(std::string_view("user1"), std::string_view("user2"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "user1-user2");
}

TEST_F(FieldListaggAggTest, TestNull) {
    ASSERT_OK_AND_ASSIGN(auto agg, MakeAgg());

    // input null -> return accumulator
    {
        auto ret = agg->Agg(std::string_view("hello"), NullType());
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "hello");
    }
    // accumulator null -> return input
    {
        auto ret = agg->Agg(NullType(), std::string_view("world"));
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "world");
    }
    // both null -> return null
    {
        auto ret = agg->Agg(NullType(), NullType());
        ASSERT_TRUE(DataDefine::IsVariantNull(ret));
    }
}

TEST_F(FieldListaggAggTest, TestEmptyString) {
    ASSERT_OK_AND_ASSIGN(auto agg, MakeAgg());

    // empty input -> return accumulator
    {
        auto ret = agg->Agg(std::string_view("hello"), std::string_view(""));
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "hello");
    }
    // empty accumulator -> return input
    {
        auto ret = agg->Agg(std::string_view(""), std::string_view("world"));
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "world");
    }
    // both empty -> return input (which is empty)
    {
        auto ret = agg->Agg(std::string_view(""), std::string_view(""));
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "");
    }
}

TEST_F(FieldListaggAggTest, TestMultipleAccumulation) {
    ASSERT_OK_AND_ASSIGN(auto agg, MakeAgg());

    // "a" + "," + "b" = "a,b", then "a,b" + "," + "c" = "a,b,c"
    auto ret = agg->Agg(std::string_view("a"), std::string_view("b"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "a,b");
    ret = agg->Agg(std::move(ret), std::string_view("c"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "a,b,c");
}

TEST_F(FieldListaggAggTest, TestDistinct) {
    ASSERT_OK_AND_ASSIGN(auto agg, MakeAgg(";", true));

    // "a;b" + "b;c" -> "a;b;c" (deduplicate "b")
    auto ret = agg->Agg(std::string_view("a;b"), std::string_view("b;c"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "a;b;c");
}

TEST_F(FieldListaggAggTest, TestDistinctNoDuplicates) {
    ASSERT_OK_AND_ASSIGN(auto agg, MakeAgg(" ", true));

    // "a b" + "c d" -> "a b c d" (no dups to remove)
    auto ret = agg->Agg(std::string_view("a b"), std::string_view("c d"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "a b c d");
}

TEST_F(FieldListaggAggTest, TestDistinctEmptyInput) {
    ASSERT_OK_AND_ASSIGN(auto agg, MakeAgg(";", true));

    // empty input -> return accumulator
    auto ret = agg->Agg(std::string_view("a;b"), std::string_view(""));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "a;b");
}

TEST_F(FieldListaggAggTest, TestDistinctFalse) {
    ASSERT_OK_AND_ASSIGN(auto agg, MakeAgg(";", false));

    // "a;b" + "b;c" -> "a;b;b;c" (no dedup)
    auto ret = agg->Agg(std::string_view("a;b"), std::string_view("b;c"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "a;b;b;c");
}

TEST_F(FieldListaggAggTest, TestInvalidType) {
    EXPECT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap({}));
    auto result = FieldListaggAgg::Create(arrow::int32(), options, "f");
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().ToString().find("supposed to be string") != std::string::npos)
        << result.status().ToString();
}

}  // namespace paimon::test
