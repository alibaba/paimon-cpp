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

#include "paimon/data/variant.h"

#include <string>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class VariantPublicApiTest : public ::testing::Test {
 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(VariantPublicApiTest, FromJsonAndAccessors) {
    ASSERT_OK_AND_ASSIGN(auto variant,
                         Variant::FromJson("{\"age\": 35, \"city\": \"Hangzhou\"}", pool_));
    ASSERT_GT(variant->Value().size(), 0);
    ASSERT_GT(variant->Metadata().size(), 0);
    ASSERT_EQ(variant->SizeInBytes(),
              static_cast<int64_t>(variant->Value().size() + variant->Metadata().size()));
    ASSERT_OK_AND_ASSIGN(std::string json, variant->ToJson());
    ASSERT_EQ(json, "{\"age\":35,\"city\":\"Hangzhou\"}");

    ASSERT_OK_AND_ASSIGN(
        auto rebuilt,
        Variant::Create(variant->Value().data(), variant->Value().size(),
                        variant->Metadata().data(), variant->Metadata().size(), pool_));
    ASSERT_OK_AND_ASSIGN(std::string rebuilt_json, rebuilt->ToJson());
    ASSERT_EQ(rebuilt_json, json);
}

TEST_F(VariantPublicApiTest, VariantGet) {
    ASSERT_OK_AND_ASSIGN(auto variant,
                         Variant::FromJson("{\"age\": 35, \"city\": \"Hangzhou\"}", pool_));
    VariantCastArgs cast_args;
    cast_args.fail_on_error = false;
    {
        auto target = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportField(arrow::Field("t", arrow::int64()), target.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::optional<Literal> literal,
                             variant->VariantGet("$.age", target.get(), cast_args));
        ASSERT_TRUE(literal.has_value());
        ASSERT_EQ(literal->GetValue<int64_t>(), 35);
    }
    {
        auto target = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportField(arrow::Field("t", arrow::utf8()), target.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::optional<Literal> literal,
                             variant->VariantGet("$.missing", target.get(), cast_args));
        ASSERT_FALSE(literal.has_value());
    }
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> sub_json, variant->VariantGetJson("$"));
    ASSERT_TRUE(sub_json.has_value());
    ASSERT_EQ(*sub_json, "{\"age\":35,\"city\":\"Hangzhou\"}");
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> missing_json,
                         variant->VariantGetJson("$.missing"));
    ASSERT_FALSE(missing_json.has_value());
}

TEST_F(VariantPublicApiTest, ArrowField) {
    ASSERT_OK_AND_ASSIGN(auto c_field, Variant::ArrowField("v", /*nullable=*/true));
    auto imported = arrow::ImportField(c_field.get());
    ASSERT_TRUE(imported.ok()) << imported.status().ToString();
    std::shared_ptr<arrow::Field> field = imported.ValueOrDie();
    ASSERT_TRUE(VariantTypeUtils::IsVariantField(field));
    ASSERT_TRUE(field->nullable());
    ASSERT_TRUE(field->type()->Equals(VariantTypeUtils::UnshreddedStructType()));
}

TEST_F(VariantPublicApiTest, VariantGetArrow) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Variant> variant,
                         Variant::FromJson("{\"user\": {\"name\": \"Paimon\"}}", pool_));
    auto target_type = arrow::struct_({arrow::field("name", arrow::utf8())});
    auto target = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportField(arrow::Field("t", target_type), target.get()).ok());
    VariantCastArgs cast_args;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowArray> c_array,
                         variant->VariantGetArrow("$.user", target.get(), cast_args));
    auto imported = arrow::ImportArray(c_array.get(), target_type);
    ASSERT_TRUE(imported.ok()) << imported.status().ToString();
    std::shared_ptr<arrow::Array> array = imported.ValueOrDie();
    ASSERT_EQ(array->length(), 1);
    const auto& row = static_cast<const arrow::StructArray&>(*array);
    ASSERT_EQ(static_cast<const arrow::StringArray&>(*row.field(0)).GetString(0), "Paimon");
}

}  // namespace paimon::test
