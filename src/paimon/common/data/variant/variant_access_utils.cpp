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

#include <set>
#include <utility>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/types/data_field.h"

namespace paimon {

namespace {

std::string GetDescription(const std::shared_ptr<arrow::Field>& field) {
    if (field->metadata() == nullptr) {
        return std::string();
    }
    auto result = field->metadata()->Get(DataField::DESCRIPTION);
    if (!result.ok()) {
        return std::string();
    }
    return result.ValueOrDie();
}

// Parses the description body from the right: the last two delimited tokens are `failOnError`
// and `timeZoneId` (neither contains the delimiter), and everything before them is the path,
// which may itself contain the delimiter inside object keys (e.g. `$['a;b']`).
std::vector<std::string> SplitDescription(const std::string& description) {
    std::string body = description.substr(sizeof(VariantAccessUtils::kMetadataKey) - 1);
    size_t tz_sep = body.rfind(VariantAccessUtils::kDelimiter);
    if (tz_sep == std::string::npos) {
        return {body};
    }
    size_t fail_sep =
        tz_sep == 0 ? std::string::npos : body.rfind(VariantAccessUtils::kDelimiter, tz_sep - 1);
    if (fail_sep == std::string::npos) {
        return {body.substr(0, tz_sep), body.substr(tz_sep + 1)};
    }
    return {body.substr(0, fail_sep), body.substr(fail_sep + 1, tz_sep - fail_sep - 1),
            body.substr(tz_sep + 1)};
}

bool HasAccessDescription(const std::shared_ptr<arrow::Field>& field) {
    return GetDescription(field).rfind(VariantAccessUtils::kMetadataKey, 0) == 0;
}

}  // namespace

constexpr char VariantAccessUtils::kMetadataKey[];
constexpr char VariantAccessUtils::kDelimiter;

std::string VariantAccessUtils::BuildVariantMetadata(const std::string& path, bool fail_on_error,
                                                     const std::string& zone_id) {
    return fmt::format("{}{}{}{}{}{}", kMetadataKey, path, kDelimiter,
                       fail_on_error ? "true" : "false", kDelimiter, zone_id);
}

bool VariantAccessUtils::IsVariantAccessType(const std::shared_ptr<arrow::DataType>& type) {
    if (type == nullptr || type->id() != arrow::Type::STRUCT || type->num_fields() == 0) {
        return false;
    }
    for (const auto& child : type->fields()) {
        if (!HasAccessDescription(child)) {
            return false;
        }
    }
    return true;
}

Result<std::vector<VariantAccessSpec>> VariantAccessUtils::ParseAccessSpecs(
    const std::shared_ptr<arrow::Field>& access_field) {
    if (!IsVariantAccessType(access_field->type())) {
        return Status::Invalid(
            fmt::format("field '{}' is not a variant-access projection", access_field->name()));
    }
    std::vector<VariantAccessSpec> specs;
    specs.reserve(access_field->type()->num_fields());
    for (const auto& child : access_field->type()->fields()) {
        std::string description = GetDescription(child);
        std::vector<std::string> parts = SplitDescription(description);
        if (parts.size() != 3) {
            return Status::Invalid(
                fmt::format("malformed variant access description '{}' on field '{}'", description,
                            child->name()));
        }
        VariantAccessSpec spec;
        spec.path = parts[0];
        PAIMON_ASSIGN_OR_RAISE(spec.segments, VariantPathSegment::Parse(spec.path));
        spec.cast_args.fail_on_error = parts[1] == "true";
        spec.cast_args.zone_id = parts[2];
        spec.target_field = child;
        specs.push_back(std::move(spec));
    }
    return specs;
}

Result<std::shared_ptr<arrow::Field>> VariantAccessUtils::ClipShreddedFileField(
    const std::vector<VariantAccessSpec>& specs, const std::shared_ptr<arrow::Field>& file_field) {
    if (file_field->type()->id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            fmt::format("variant file field '{}' is not a struct", file_field->name()));
    }
    const auto& file_struct = static_cast<const arrow::StructType&>(*file_field->type());
    std::shared_ptr<arrow::Field> typed_value =
        file_struct.GetFieldByName(VariantDefs::kTypedValueFieldName);
    if (typed_value == nullptr) {
        // The file stores the column unshredded; there is nothing to prune.
        return file_field;
    }

    bool can_clip = true;
    std::set<std::string> fields_to_read;
    for (const auto& spec : specs) {
        if (spec.segments.empty()) {
            // A root path needs the whole variant.
            can_clip = false;
            break;
        }
        if (spec.segments[0].kind == VariantPathSegment::Kind::kObjectExtraction) {
            // Only top-level object keys are pruned; nested paths still narrow to their
            // top-level key.
            fields_to_read.insert(spec.segments[0].key);
        } else {
            can_clip = false;
            break;
        }
    }
    if (!can_clip) {
        return file_field;
    }

    std::shared_ptr<arrow::Field> metadata_field =
        file_struct.GetFieldByName(VariantDefs::kMetadataFieldName);
    std::shared_ptr<arrow::Field> value_field =
        file_struct.GetFieldByName(VariantDefs::kValueFieldName);
    if (metadata_field == nullptr) {
        return Status::Invalid(
            fmt::format("shredded variant field '{}' misses metadata", file_field->name()));
    }

    arrow::FieldVector typed_fields;
    if (typed_value->type()->id() == arrow::Type::STRUCT) {
        for (const auto& typed_child : typed_value->type()->fields()) {
            if (fields_to_read.erase(typed_child->name()) > 0) {
                typed_fields.push_back(typed_child);
            }
        }
    }

    arrow::FieldVector clipped_fields = {metadata_field};
    if (!fields_to_read.empty() && value_field != nullptr) {
        // Some requested key is not shredded; keep `value` for the binary fallback.
        clipped_fields.push_back(value_field);
    }
    if (!typed_fields.empty()) {
        clipped_fields.push_back(typed_value->WithType(arrow::struct_(typed_fields)));
    }
    return file_field->WithType(arrow::struct_(clipped_fields));
}

}  // namespace paimon
