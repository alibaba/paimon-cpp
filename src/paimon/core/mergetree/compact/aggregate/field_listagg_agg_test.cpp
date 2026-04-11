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

#include "paimon/core/mergetree/compact/aggregate/field_listagg_agg.h"

#include <map>
#include <string>
#include <string_view>

#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/core/core_options.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

static CoreOptions CreateOptions(const std::map<std::string, std::string>& opts = {}) {
    auto result = CoreOptions::FromMap(opts);
    assert(result.ok());
    return std::move(result.value());
}

TEST(FieldListaggAggTest, TestSimple) {
    auto options = CreateOptions({{"fields.f.aggregate-function", "listagg"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldListaggAgg> field_listagg_agg,
                         FieldListaggAgg::Create(arrow::utf8(), options, "f"));
    auto agg_ret = field_listagg_agg->Agg(std::string_view("hello"), std::string_view(" world"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "hello, world");
}

TEST(FieldListaggAggTest, TestDelimiter) {
    auto options = CreateOptions(
        {{"fields.f.aggregate-function", "listagg"}, {"fields.f.list-agg-delimiter", "-"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldListaggAgg> field_listagg_agg,
                         FieldListaggAgg::Create(arrow::utf8(), options, "f"));
    auto agg_ret = field_listagg_agg->Agg(std::string_view("user1"), std::string_view("user2"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "user1-user2");
}

TEST(FieldListaggAggTest, TestNull) {
    auto options = CreateOptions({{"fields.f.aggregate-function", "listagg"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldListaggAgg> field_listagg_agg,
                         FieldListaggAgg::Create(arrow::utf8(), options, "f"));
    // input null -> return accumulator
    {
        auto agg_ret = field_listagg_agg->Agg(std::string_view("hello"), NullType());
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "hello");
    }
    // accumulator null -> return input
    {
        auto agg_ret = field_listagg_agg->Agg(NullType(), std::string_view("world"));
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "world");
    }
    // both null -> return null
    {
        auto agg_ret = field_listagg_agg->Agg(NullType(), NullType());
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
    }
}

TEST(FieldListaggAggTest, TestEmptyString) {
    auto options = CreateOptions({{"fields.f.aggregate-function", "listagg"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldListaggAgg> field_listagg_agg,
                         FieldListaggAgg::Create(arrow::utf8(), options, "f"));
    // empty input -> return accumulator
    {
        auto agg_ret = field_listagg_agg->Agg(std::string_view("hello"), std::string_view(""));
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "hello");
    }
    // empty accumulator -> return input
    {
        auto agg_ret = field_listagg_agg->Agg(std::string_view(""), std::string_view("world"));
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "world");
    }
    // both empty -> return input (which is empty)
    {
        auto agg_ret = field_listagg_agg->Agg(std::string_view(""), std::string_view(""));
        ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "");
    }
}

TEST(FieldListaggAggTest, TestMultipleAccumulation) {
    auto options = CreateOptions({{"fields.f.aggregate-function", "listagg"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldListaggAgg> field_listagg_agg,
                         FieldListaggAgg::Create(arrow::utf8(), options, "f"));
    // simulate iteratively accumulating with default delimiter ",":
    // "a" + "," + "b" = "a,b", then "a,b" + "," + "c" = "a,b,c"
    auto ret = field_listagg_agg->Agg(std::string_view("a"), std::string_view("b"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "a,b");
    ret = field_listagg_agg->Agg(std::move(ret), std::string_view("c"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(ret), "a,b,c");
}

TEST(FieldListaggAggTest, TestDistinct) {
    auto options = CreateOptions({{"fields.f.aggregate-function", "listagg"},
                                  {"fields.f.list-agg-delimiter", ";"},
                                  {"fields.f.distinct", "true"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldListaggAgg> field_listagg_agg,
                         FieldListaggAgg::Create(arrow::utf8(), options, "f"));

    // accumulator="a;b", input="b;c" -> result="a;b;c" (deduplicate "b")
    auto agg_ret = field_listagg_agg->Agg(std::string_view("a;b"), std::string_view("b;c"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "a;b;c");
}

TEST(FieldListaggAggTest, TestDistinctNoDuplicates) {
    auto options = CreateOptions({{"fields.f.aggregate-function", "listagg"},
                                  {"fields.f.list-agg-delimiter", " "},
                                  {"fields.f.distinct", "true"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldListaggAgg> field_listagg_agg,
                         FieldListaggAgg::Create(arrow::utf8(), options, "f"));

    // accumulator="a b", input="c d" -> result="a b c d" (no dups to remove)
    auto agg_ret = field_listagg_agg->Agg(std::string_view("a b"), std::string_view("c d"));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "a b c d");
}

TEST(FieldListaggAggTest, TestDistinctEmptyInput) {
    auto options = CreateOptions({{"fields.f.aggregate-function", "listagg"},
                                  {"fields.f.list-agg-delimiter", ";"},
                                  {"fields.f.distinct", "true"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldListaggAgg> field_listagg_agg,
                         FieldListaggAgg::Create(arrow::utf8(), options, "f"));

    // empty input -> return accumulator
    auto agg_ret = field_listagg_agg->Agg(std::string_view("a;b"), std::string_view(""));
    ASSERT_EQ(DataDefine::GetVariantValue<std::string_view>(agg_ret), "a;b");
}

TEST(FieldListaggAggTest, TestInvalidType) {
    auto options = CreateOptions({{"fields.f.aggregate-function", "listagg"}});
    auto result = FieldListaggAgg::Create(arrow::int32(), options, "f");
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().ToString().find("supposed to be string") != std::string::npos)
        << result.status().ToString();
}

}  // namespace paimon::test
