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

#include "paimon/common/data/columnar/columnar_utils.h"

#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_dict.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {
TEST(ColumnarUtilsTest, TestGetViewAndBytes) {
    auto pool = GetDefaultPool();
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["abc", "def", "hi"])")
                     .ValueOrDie();
    std::string_view view = ColumnarUtils::GetView(array.get(), 2);
    ASSERT_EQ(std::string(view), "hi");
    auto bytes = ColumnarUtils::GetBytes<arrow::BinaryType>(array.get(), 1, pool.get());
    ASSERT_EQ(*std::make_shared<Bytes>("def", pool.get()), *bytes);
}

TEST(ColumnarUtilsTest, TestGetViewAndBytesOfDict) {
    auto pool = GetDefaultPool();
    auto dict = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["foo", "bar", "baz"])")
                    .ValueOrDie();
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 0, 2, 0]").ValueOrDie();
    std::shared_ptr<arrow::DictionaryArray> dict_array =
        std::make_shared<arrow::DictionaryArray>(dict_type, indices, dict);

    ASSERT_EQ("bar", std::string(ColumnarUtils::GetView(dict_array.get(), 0)));
    ASSERT_EQ("baz", std::string(ColumnarUtils::GetView(dict_array.get(), 1)));
    ASSERT_EQ("foo", std::string(ColumnarUtils::GetView(dict_array.get(), 2)));
    ASSERT_EQ("baz", std::string(ColumnarUtils::GetView(dict_array.get(), 3)));
    ASSERT_EQ("foo", std::string(ColumnarUtils::GetView(dict_array.get(), 4)));
}

template <typename ArrowType>
class ColumnarUtilsBinaryDictionaryTest : public ::testing::Test {};

using BinaryDictionaryTypes = ::testing::Types<arrow::BinaryType, arrow::LargeBinaryType>;
TYPED_TEST_SUITE(ColumnarUtilsBinaryDictionaryTest, BinaryDictionaryTypes);

TYPED_TEST(ColumnarUtilsBinaryDictionaryTest, GetViewAndBytes) {
    auto pool = GetDefaultPool();
    const std::vector<std::string> values = {std::string("\x00\xff\x80", 3), "",
                                             std::string("a\0b", 3)};
    typename arrow::TypeTraits<TypeParam>::BuilderType builder;
    ASSERT_TRUE(builder.Append("unused").ok());
    for (const auto& value : values) {
        ASSERT_TRUE(builder.Append(value).ok());
    }
    std::shared_ptr<arrow::Array> dictionary;
    ASSERT_TRUE(builder.Finish(&dictionary).ok());
    dictionary = dictionary->Slice(1);

    const std::vector<std::shared_ptr<arrow::DataType>> index_types = {
        arrow::int8(), arrow::int16(), arrow::int32(), arrow::int64()};
    const std::vector<int32_t> expected_indices = {0, 1, 2, 0, -1, 2};
    for (const auto& index_type : index_types) {
        SCOPED_TRACE(index_type->ToString());
        auto indices =
            arrow::ipc::internal::json::ArrayFromJSON(index_type, "[0, 1, 2, 0, null, 2]")
                .ValueOrDie();
        auto dict_array = arrow::DictionaryArray::FromArrays(indices, dictionary).ValueOrDie();
        ASSERT_TRUE(dict_array->ValidateFull().ok());
        for (int32_t offset : {0, 1}) {
            SCOPED_TRACE(offset);
            auto sliced = dict_array->Slice(offset);
            for (int32_t pos = 0; pos < sliced->length(); ++pos) {
                SCOPED_TRACE(pos);
                int32_t index = expected_indices[offset + pos];
                ASSERT_EQ(index == -1, sliced->IsNull(pos));
                if (index == -1) {
                    continue;
                }
                ASSERT_EQ(values[index], ColumnarUtils::GetView(sliced.get(), pos));
                auto bytes = ColumnarUtils::GetBytes<TypeParam>(sliced.get(), pos, pool.get());
                ASSERT_EQ(Bytes(values[index], pool.get()), *bytes);
            }
        }
    }
}

}  // namespace paimon::test
