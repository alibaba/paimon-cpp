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

#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_access_utils.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/data/variant/variant_get.h"
#include "paimon/common/data/variant/variant_path_segment.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

class Variant::Impl {
 public:
    Impl(std::shared_ptr<GenericVariant> variant, std::shared_ptr<MemoryPool> pool)
        : variant_(std::move(variant)), pool_(std::move(pool)), arrow_pool_(GetArrowPool(pool_)) {}

    const std::shared_ptr<GenericVariant>& GetVariant() const {
        return variant_;
    }

    const std::shared_ptr<MemoryPool>& GetPool() const {
        return pool_;
    }

    const std::shared_ptr<arrow::MemoryPool>& GetArrowMemoryPool() const {
        return arrow_pool_;
    }

 private:
    std::shared_ptr<GenericVariant> variant_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

Variant::Variant(std::unique_ptr<Impl>&& impl) : impl_(std::move(impl)) {}
Variant::~Variant() = default;

Result<std::unique_ptr<Variant>> Variant::FromJson(const std::string& json,
                                                   const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> variant,
                           GenericVariant::FromJson(json, pool));
    auto impl = std::make_unique<Impl>(std::move(variant), pool);
    return std::unique_ptr<Variant>(new Variant(std::move(impl)));
}

Result<std::unique_ptr<Variant>> Variant::Create(const char* value, uint64_t value_length,
                                                 const char* metadata, uint64_t metadata_length,
                                                 const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<GenericVariant> variant,
        GenericVariant::Create(std::string_view(value, value_length),
                               std::string_view(metadata, metadata_length), pool));
    auto impl = std::make_unique<Impl>(std::move(variant), pool);
    return std::unique_ptr<Variant>(new Variant(std::move(impl)));
}

std::string_view Variant::Value() const {
    return impl_->GetVariant()->RawValue();
}

std::string_view Variant::Metadata() const {
    return impl_->GetVariant()->Metadata();
}

int64_t Variant::SizeInBytes() const {
    return impl_->GetVariant()->SizeInBytes();
}

Result<std::string> Variant::ToJson(const std::string& zone_id) const {
    return impl_->GetVariant()->ToJson(zone_id);
}

Result<std::optional<Literal>> Variant::VariantGet(const std::string& path,
                                                   struct ArrowSchema* target_type,
                                                   const VariantCastArgs& cast_args) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Field> target_field,
                                      arrow::ImportField(target_type));
    return VariantGetExecutor::Get(impl_->GetVariant(), path, target_field->type(), cast_args);
}

Result<std::unique_ptr<struct ArrowArray>> Variant::VariantGetArrow(
    const std::string& path, struct ArrowSchema* target_field,
    const VariantCastArgs& cast_args) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Field> field,
                                      arrow::ImportField(target_field));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Array> array,
        VariantGetExecutor::GetAsArrow(impl_->GetVariant(), path, field, cast_args,
                                       impl_->GetPool(), impl_->GetArrowMemoryPool()));
    auto result = std::make_unique<struct ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, result.get()));
    return result;
}

Result<std::optional<std::string>> Variant::VariantGetJson(const std::string& path,
                                                           const std::string& zone_id) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> extracted,
                           VariantGetExecutor::ExtractByPath(impl_->GetVariant(), path));
    if (extracted == nullptr) {
        return std::optional<std::string>(std::nullopt);
    }
    PAIMON_ASSIGN_OR_RAISE(std::string json, extracted->ToJson(zone_id));
    return std::optional<std::string>(std::move(json));
}

Result<std::unique_ptr<struct ArrowSchema>> Variant::ArrowField(
    const std::string& field_name, bool nullable,
    std::unordered_map<std::string, std::string> metadata) {
    auto variant_field = VariantTypeUtils::ToArrowField(field_name, nullable, std::move(metadata));
    auto field = std::make_unique<struct ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportField(*variant_field, field.get()));
    return field;
}

class VariantAccessBuilder::Impl {
 public:
    arrow::FieldVector fields;
};

VariantAccessBuilder::VariantAccessBuilder() : impl_(std::make_unique<Impl>()) {}
VariantAccessBuilder::~VariantAccessBuilder() = default;

Status VariantAccessBuilder::AddField(struct ArrowSchema* target_type, const std::string& path,
                                      bool fail_on_error, const std::string& zone_id) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Field> target,
                                      arrow::ImportField(target_type));
    // Validate the path eagerly so mistakes fail at build time, not at read time.
    PAIMON_RETURN_NOT_OK(VariantPathSegment::Parse(path));
    // Keep the target field's own metadata (e.g. the variant extension marker of a
    // `Variant::ArrowField` target, which drives the deep re-encode cast) and add the access
    // description to it.
    std::vector<std::string> keys = {DataField::DESCRIPTION};
    std::vector<std::string> values = {
        VariantAccessUtils::BuildVariantMetadata(path, fail_on_error, zone_id)};
    if (target->metadata() != nullptr) {
        for (int64_t i = 0; i < target->metadata()->size(); ++i) {
            if (target->metadata()->key(i) == DataField::DESCRIPTION) {
                continue;
            }
            keys.push_back(target->metadata()->key(i));
            values.push_back(target->metadata()->value(i));
        }
    }
    impl_->fields.push_back(arrow::field(std::to_string(impl_->fields.size()), target->type(),
                                         /*nullable=*/true,
                                         arrow::KeyValueMetadata::Make(keys, values)));
    return Status::OK();
}

Result<std::unique_ptr<struct ArrowSchema>> VariantAccessBuilder::Build(
    const std::string& field_name) const {
    if (impl_->fields.empty()) {
        return Status::Invalid("a variant-access projection needs at least one field");
    }
    auto access_field =
        arrow::field(field_name, arrow::struct_(impl_->fields), /*nullable=*/true,
                     arrow::KeyValueMetadata::Make({VariantDefs::kExtensionTypeKey},
                                                   {VariantDefs::kExtensionTypeValue}));
    auto field = std::make_unique<struct ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportField(*access_field, field.get()));
    return field;
}

}  // namespace paimon
