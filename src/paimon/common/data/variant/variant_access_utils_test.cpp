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

#include "paimon/common/data/variant/variant_access_utils.h"

#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/key_value_metadata.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/types/data_field.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

// An Arrow field carrying a `paimon.description` metadata entry.
std::shared_ptr<arrow::Field> DescribedField(const std::string& name,
                                             const std::shared_ptr<arrow::DataType>& type,
                                             const std::string& description) {
    return arrow::field(name, type, /*nullable=*/true,
                        arrow::key_value_metadata({DataField::DESCRIPTION}, {description}));
}

std::shared_ptr<arrow::Field> AccessProjection(
    const std::vector<std::shared_ptr<arrow::Field>>& children) {
    return arrow::field("v", arrow::struct_(children));
}

// A shredded file field: struct{[metadata], value, typed_value: struct{<typed_children>}}.
std::shared_ptr<arrow::Field> ShreddedFile(
    const std::vector<std::shared_ptr<arrow::Field>>& typed_children, bool with_metadata = true) {
    arrow::FieldVector fields;
    if (with_metadata) {
        fields.push_back(
            arrow::field(std::string(VariantDefs::kMetadataFieldName), arrow::binary()));
    }
    fields.push_back(arrow::field(std::string(VariantDefs::kValueFieldName), arrow::binary()));
    fields.push_back(arrow::field(std::string(VariantDefs::kTypedValueFieldName),
                                  arrow::struct_(typed_children)));
    return arrow::field("v", arrow::struct_(fields));
}

std::vector<VariantAccessSpec> ObjectKeySpecs() {
    auto proj = AccessProjection({DescribedField(
        "a", arrow::int32(), VariantAccessUtils::BuildVariantMetadata("$.a", false, "UTC"))});
    auto result = VariantAccessUtils::ParseAccessSpecs(proj);
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    return result.value();
}

}  // namespace

TEST(VariantAccessUtilsTest, ParseAccessSpecsRejectsNonProjection) {
    // A plain struct without access descriptions is not a variant-access projection.
    auto plain = arrow::field("v", arrow::struct_({arrow::field("x", arrow::int32())}));
    ASSERT_FALSE(VariantAccessUtils::IsVariantAccessType(plain->type()));
    ASSERT_NOK(VariantAccessUtils::ParseAccessSpecs(plain));
}

TEST(VariantAccessUtilsTest, ParseAccessSpecsRejectsMalformedDescription) {
    const std::string key = VariantAccessUtils::kMetadataKey;
    // A description with no delimiter splits into a single part.
    auto no_delim = AccessProjection({DescribedField("a", arrow::utf8(), key + "$.a")});
    ASSERT_TRUE(VariantAccessUtils::IsVariantAccessType(no_delim->type()));
    ASSERT_NOK(VariantAccessUtils::ParseAccessSpecs(no_delim));
    // A single delimiter splits into two parts (still not the three that a spec needs).
    auto one_delim = AccessProjection({DescribedField("a", arrow::utf8(), key + "$.a;true")});
    ASSERT_TRUE(VariantAccessUtils::IsVariantAccessType(one_delim->type()));
    ASSERT_NOK(VariantAccessUtils::ParseAccessSpecs(one_delim));
}

TEST(VariantAccessUtilsTest, ParseAccessSpecsRoundTripsBuildMetadata) {
    auto proj = AccessProjection({DescribedField(
        "a", arrow::int32(), VariantAccessUtils::BuildVariantMetadata("$.a", true, "+08:00"))});
    ASSERT_OK_AND_ASSIGN(std::vector<VariantAccessSpec> specs,
                         VariantAccessUtils::ParseAccessSpecs(proj));
    ASSERT_EQ(specs.size(), 1);
    EXPECT_EQ(specs[0].path, "$.a");
    EXPECT_TRUE(specs[0].cast_args.fail_on_error);
    EXPECT_EQ(specs[0].cast_args.zone_id, "+08:00");
}

TEST(VariantAccessUtilsTest, ClipRejectsNonStructFileField) {
    auto non_struct = arrow::field("v", arrow::int32());
    ASSERT_NOK(VariantAccessUtils::ClipShreddedFileField(ObjectKeySpecs(), non_struct));
}

TEST(VariantAccessUtilsTest, ClipUnprunablePathsReturnFileUnchanged) {
    auto file = ShreddedFile({arrow::field("a", arrow::int32())});

    // A root path needs the whole variant, so the file field is returned unchanged.
    auto root_proj = AccessProjection({DescribedField(
        "r", arrow::utf8(), VariantAccessUtils::BuildVariantMetadata("$", false, "UTC"))});
    ASSERT_OK_AND_ASSIGN(std::vector<VariantAccessSpec> root_specs,
                         VariantAccessUtils::ParseAccessSpecs(root_proj));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> root_clipped,
                         VariantAccessUtils::ClipShreddedFileField(root_specs, file));
    EXPECT_EQ(root_clipped, file);

    // An array-first path likewise cannot be pruned to a top-level key.
    auto array_proj = AccessProjection({DescribedField(
        "e", arrow::utf8(), VariantAccessUtils::BuildVariantMetadata("$[0]", false, "UTC"))});
    ASSERT_OK_AND_ASSIGN(std::vector<VariantAccessSpec> array_specs,
                         VariantAccessUtils::ParseAccessSpecs(array_proj));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> array_clipped,
                         VariantAccessUtils::ClipShreddedFileField(array_specs, file));
    EXPECT_EQ(array_clipped, file);
}

TEST(VariantAccessUtilsTest, ClipUnshreddedFileFieldReturnedUnchanged) {
    // No typed_value column: there is nothing to prune.
    auto unshredded = arrow::field(
        "v",
        arrow::struct_({arrow::field(std::string(VariantDefs::kMetadataFieldName), arrow::binary()),
                        arrow::field(std::string(VariantDefs::kValueFieldName), arrow::binary())}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> clipped,
                         VariantAccessUtils::ClipShreddedFileField(ObjectKeySpecs(), unshredded));
    EXPECT_EQ(clipped, unshredded);
}

TEST(VariantAccessUtilsTest, ClipMissingMetadataFails) {
    auto file_no_metadata =
        ShreddedFile({arrow::field("a", arrow::int32())}, /*with_metadata=*/false);
    ASSERT_NOK(VariantAccessUtils::ClipShreddedFileField(ObjectKeySpecs(), file_no_metadata));
}

TEST(VariantAccessUtilsTest, ClipNarrowsToRequestedKeys) {
    // "$.a" is requested; the shredded typed_value keeps only "a" and drops the unrelated "b".
    auto file = ShreddedFile({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> clipped,
                         VariantAccessUtils::ClipShreddedFileField(ObjectKeySpecs(), file));
    ASSERT_EQ(clipped->type()->id(), arrow::Type::STRUCT);
    const auto& clipped_struct = static_cast<const arrow::StructType&>(*clipped->type());
    // metadata is always kept; value is dropped because "a" is shredded; typed_value keeps "a".
    ASSERT_NE(clipped_struct.GetFieldByName(VariantDefs::kMetadataFieldName), nullptr);
    EXPECT_EQ(clipped_struct.GetFieldByName(VariantDefs::kValueFieldName), nullptr);
    auto typed = clipped_struct.GetFieldByName(VariantDefs::kTypedValueFieldName);
    ASSERT_NE(typed, nullptr);
    ASSERT_EQ(typed->type()->num_fields(), 1);
    EXPECT_EQ(typed->type()->field(0)->name(), "a");
}

}  // namespace paimon::test
