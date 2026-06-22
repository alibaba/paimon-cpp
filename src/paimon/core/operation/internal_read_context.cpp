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

#include "paimon/core/operation/internal_read_context.h"

#include <optional>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/status.h"

namespace paimon {

Result<std::shared_ptr<arrow::Field>> InternalReadContext::AlignReadFieldWithTableFieldIds(
    const std::shared_ptr<arrow::Field>& read_field,
    const std::shared_ptr<arrow::Field>& table_field) {
    static const std::vector<std::string> kReadMetadataWhitelist = {DataField::MAP_SELECTED_KEYS};

    if (read_field->type()->id() != table_field->type()->id()) {
        return Status::Invalid(fmt::format(
            "Read schema field '{}' type {} does not match table field type {}", read_field->name(),
            read_field->type()->ToString(), table_field->type()->ToString()));
    }

    auto type_id = read_field->type()->id();
    if (type_id == arrow::Type::STRUCT) {
        auto read_struct = std::static_pointer_cast<arrow::StructType>(read_field->type());
        auto table_struct = std::static_pointer_cast<arrow::StructType>(table_field->type());
        arrow::FieldVector rebased_children;
        rebased_children.reserve(read_struct->num_fields());
        for (const auto& read_child : read_struct->fields()) {
            auto table_child =
                NestedProjectionUtils::FindFieldByName(table_struct->fields(), read_child->name());
            if (!table_child) {
                return Status::Invalid(
                    fmt::format("Read schema nested field '{}' does not exist in table field '{}'",
                                read_child->name(), table_field->name()));
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> rebased_child,
                                   AlignReadFieldWithTableFieldIds(read_child, table_child));
            rebased_children.push_back(rebased_child);
        }
        auto rebased_type = arrow::struct_(rebased_children);
        auto aligned_field = table_field->WithType(rebased_type);
        return DataField::MergeFieldMetadataByWhitelist(aligned_field, read_field,
                                                        kReadMetadataWhitelist);
    }

    if (type_id == arrow::Type::LIST) {
        auto read_list = std::static_pointer_cast<arrow::ListType>(read_field->type());
        auto table_list = std::static_pointer_cast<arrow::ListType>(table_field->type());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Field> rebased_value_field,
            AlignReadFieldWithTableFieldIds(read_list->value_field(), table_list->value_field()));
        auto rebased_type = arrow::list(rebased_value_field);
        auto aligned_field = table_field->WithType(rebased_type);
        return DataField::MergeFieldMetadataByWhitelist(aligned_field, read_field,
                                                        kReadMetadataWhitelist);
    }

    if (type_id == arrow::Type::MAP) {
        auto read_map = std::static_pointer_cast<arrow::MapType>(read_field->type());
        auto table_map = std::static_pointer_cast<arrow::MapType>(table_field->type());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Field> rebased_key_field,
            AlignReadFieldWithTableFieldIds(read_map->key_field(), table_map->key_field()));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Field> rebased_item_field,
            AlignReadFieldWithTableFieldIds(read_map->item_field(), table_map->item_field()));
        auto rebased_type = arrow::map(rebased_key_field->type(), rebased_item_field);
        auto aligned_field = table_field->WithType(rebased_type);
        return DataField::MergeFieldMetadataByWhitelist(aligned_field, read_field,
                                                        kReadMetadataWhitelist);
    }

    if (!read_field->type()->Equals(table_field->type())) {
        return Status::Invalid(fmt::format(
            "Read schema field '{}' type {} does not match table field type {}", read_field->name(),
            read_field->type()->ToString(), table_field->type()->ToString()));
    }

    auto aligned_field = table_field->WithType(read_field->type());
    return DataField::MergeFieldMetadataByWhitelist(aligned_field, read_field,
                                                    kReadMetadataWhitelist);
}

std::optional<DataField> InternalReadContext::TryResolveSpecialFieldById(
    int32_t field_id, const CoreOptions& core_options) {
    if (field_id == SpecialFields::ValueKind().Id()) {
        return SpecialFields::ValueKind();
    }
    if (field_id == SpecialFields::RowId().Id()) {
        if (core_options.RowTrackingEnabled()) {
            return SpecialFields::RowId();
        }
        return std::nullopt;
    }
    if (field_id == SpecialFields::SequenceNumber().Id()) {
        if (core_options.RowTrackingEnabled() || core_options.KeyValueSequenceNumberEnabled()) {
            return SpecialFields::SequenceNumber();
        }
        return std::nullopt;
    }
    if (field_id == SpecialFields::IndexScore().Id()) {
        if (core_options.DataEvolutionEnabled()) {
            return SpecialFields::IndexScore();
        }
        return std::nullopt;
    }
    return std::nullopt;
}

std::optional<DataField> InternalReadContext::TryResolveSpecialFieldByName(
    const std::string& name, const CoreOptions& core_options) {
    if (name == SpecialFields::ValueKind().Name()) {
        return SpecialFields::ValueKind();
    }
    if (name == SpecialFields::RowId().Name()) {
        if (core_options.RowTrackingEnabled()) {
            return SpecialFields::RowId();
        }
        return std::nullopt;
    }
    if (name == SpecialFields::SequenceNumber().Name()) {
        if (core_options.RowTrackingEnabled() || core_options.KeyValueSequenceNumberEnabled()) {
            return SpecialFields::SequenceNumber();
        }
        return std::nullopt;
    }
    if (name == SpecialFields::IndexScore().Name()) {
        if (core_options.DataEvolutionEnabled()) {
            return SpecialFields::IndexScore();
        }
        return std::nullopt;
    }
    return std::nullopt;
}

Result<std::unique_ptr<InternalReadContext>> InternalReadContext::Create(
    const std::shared_ptr<ReadContext>& context, const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& options) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options, context->GetSpecificFileSystem(),
                                                context->GetFileSystemSchemeToIdentifierMap()));
    core_options.WithCache(context->GetCache());
    // prepare read schema
    // Priority: projected_arrow_schema > read_field_ids > read_field_names
    std::vector<DataField> read_data_fields;
    if (context->HasReadSchema()) {
        // Nested column pruning path: user provided a read C ArrowSchema
        // where STRUCT types may contain only a subset of sub-fields.
        // ImportSchema consumes the C schema — that's fine, it's one-shot usage.
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                          arrow::ImportSchema(context->GetReadSchema()));
        read_data_fields.reserve(read_schema->num_fields());
        // Align special-field validation with read_field_ids/read_field_names branches.
        for (const auto& read_field : read_schema->fields()) {
            if (auto resolved_special_field =
                    TryResolveSpecialFieldByName(read_field->name(), core_options)) {
                read_data_fields.push_back(*resolved_special_field);
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField table_field,
                                   table_schema->GetField(read_field->name()));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Field> aligned_field,
                AlignReadFieldWithTableFieldIds(read_field, table_field.ArrowField()));
            read_data_fields.emplace_back(table_field.Id(), aligned_field,
                                          table_field.Description());
        }
    } else if (!context->GetReadFieldIds().empty()) {
        read_data_fields.reserve(context->GetReadFieldIds().size());
        for (const auto& field_id : context->GetReadFieldIds()) {
            if (auto resolved_special_field = TryResolveSpecialFieldById(field_id, core_options)) {
                read_data_fields.push_back(*resolved_special_field);
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(field_id));
            read_data_fields.push_back(field);
        }
    } else if (!context->GetReadFieldNames().empty()) {
        read_data_fields.reserve(context->GetReadFieldNames().size());
        for (const auto& name : context->GetReadFieldNames()) {
            if (auto resolved_special_field = TryResolveSpecialFieldByName(name, core_options)) {
                read_data_fields.push_back(*resolved_special_field);
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(name));
            read_data_fields.push_back(field);
        }
    } else {
        // if field names not set, read all fields
        read_data_fields = table_schema->Fields();
    }
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_data_fields);
    // validate read schema to avoid redundant fields
    PAIMON_RETURN_NOT_OK(ArrowSchemaValidator::ValidateSchemaWithFieldId(*read_schema));
    // validate predicate
    if (context->GetPredicate()) {
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
            *read_schema, context->GetPredicate(), /*validate_field_idx=*/true));
        PAIMON_RETURN_NOT_OK(
            PredicateValidator::ValidatePredicateWithLiterals(context->GetPredicate()));
    }

    return std::unique_ptr<InternalReadContext>(
        new InternalReadContext(context, table_schema, read_schema, core_options));
}

InternalReadContext::InternalReadContext(const std::shared_ptr<ReadContext>& read_context,
                                         const std::shared_ptr<TableSchema>& table_schema,
                                         const std::shared_ptr<arrow::Schema>& read_schema,
                                         const CoreOptions& options)
    : read_context_(read_context),
      table_schema_(table_schema),
      read_schema_(read_schema),
      options_(options) {}

Result<std::shared_ptr<InternalReadContext>> InternalReadContext::CreateWithSchema(
    const std::shared_ptr<InternalReadContext>& original,
    const std::shared_ptr<arrow::Schema>& new_read_schema) {
    // Create a new InternalReadContext sharing all properties except read_schema.
    // The new read_schema is the minimal column set for COUNT(*).
    return std::shared_ptr<InternalReadContext>(new InternalReadContext(
        original->read_context_, original->table_schema_, new_read_schema, original->options_));
}

}  // namespace paimon
