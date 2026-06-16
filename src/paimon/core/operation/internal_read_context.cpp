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

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/status.h"

namespace paimon {

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
        // Nested column pruning path: user provided a projected C ArrowSchema
        // where STRUCT types may contain only a subset of sub-fields.
        // ImportSchema consumes the C schema — that's fine, it's one-shot usage.
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> projected_schema,
                                          arrow::ImportSchema(context->GetReadSchema()));
        PAIMON_ASSIGN_OR_RAISE(read_data_fields,
                               DataField::ConvertArrowSchemaToDataFields(projected_schema));
        // Align special-field validation with read_field_ids/read_field_names branches.
        for (auto& field : read_data_fields) {
            if (auto resolved_special_field =
                    TryResolveSpecialFieldById(field.Id(), core_options)) {
                field = *resolved_special_field;
                continue;
            }
            if (SpecialFields::IsSpecialFieldName(field.Name())) {
                if (auto resolved_special_field =
                        TryResolveSpecialFieldByName(field.Name(), core_options)) {
                    field = *resolved_special_field;
                    continue;
                }
            }
            PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] DataField unused,
                                   table_schema->GetField(field.Id()));
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
