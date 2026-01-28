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

#include <utility>

#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
Result<std::unique_ptr<InternalReadContext>> InternalReadContext::Create(
    const std::shared_ptr<ReadContext>& context, const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& options) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options, context->GetSpecificFileSystem(),
                                                context->GetFileSystemSchemeToIdentifierMap()));
    // prepare read schema
    std::vector<DataField> read_data_fields;
    if (!context->GetReadFieldIds().empty()) {
        read_data_fields.reserve(context->GetReadFieldIds().size());
        for (const auto& field_id : context->GetReadFieldIds()) {
            // if enable row tracking or data evolution, check special fields
            if (core_options.RowTrackingEnabled() && field_id == SpecialFields::RowId().Id()) {
                read_data_fields.push_back(SpecialFields::RowId());
                continue;
            }
            if (core_options.RowTrackingEnabled() &&
                field_id == SpecialFields::SequenceNumber().Id()) {
                read_data_fields.push_back(SpecialFields::SequenceNumber());
                continue;
            }
            if (core_options.DataEvolutionEnabled() &&
                field_id == SpecialFields::IndexScore().Id()) {
                read_data_fields.push_back(SpecialFields::IndexScore());
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(field_id));
            read_data_fields.push_back(field);
        }
    } else if (!context->GetReadSchema().empty()) {
        read_data_fields.reserve(context->GetReadSchema().size());
        for (const auto& name : context->GetReadSchema()) {
            // if enable row tracking or data evolution, check special fields
            if (core_options.RowTrackingEnabled() && name == SpecialFields::RowId().Name()) {
                read_data_fields.push_back(SpecialFields::RowId());
                continue;
            }
            if (core_options.RowTrackingEnabled() &&
                name == SpecialFields::SequenceNumber().Name()) {
                read_data_fields.push_back(SpecialFields::SequenceNumber());
                continue;
            }
            if (core_options.DataEvolutionEnabled() && name == SpecialFields::IndexScore().Name()) {
                read_data_fields.push_back(SpecialFields::IndexScore());
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

}  // namespace paimon
