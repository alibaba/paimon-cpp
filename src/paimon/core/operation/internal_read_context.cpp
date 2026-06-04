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

#include "paimon/common/predicate/compound_predicate_impl.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/predicate/function.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
namespace {
// Build a map from a field's position in `table_schema` (the latest table schema, the
// index space upstream predicates are typically constructed against) to its position
// in `read_data_fields` (the projected read schema). Field identity is the stable
// field id, so the mapping survives column renames within the same schema. Read-only
// special fields (RowId / SequenceNumber / etc.) have no analogue in the table
// schema and are skipped — user-supplied predicates do not reference them.
std::map<int32_t, int32_t> BuildLatestToReadIdxMapping(
    const TableSchema& table_schema, const std::vector<DataField>& read_data_fields) {
    std::map<int32_t, int32_t> id_to_latest_idx;
    const auto& table_fields = table_schema.Fields();
    for (size_t latest_idx = 0; latest_idx < table_fields.size(); latest_idx++) {
        id_to_latest_idx[table_fields[latest_idx].Id()] = static_cast<int32_t>(latest_idx);
    }
    std::map<int32_t, int32_t> mapping;
    for (size_t read_idx = 0; read_idx < read_data_fields.size(); read_idx++) {
        auto iter = id_to_latest_idx.find(read_data_fields[read_idx].Id());
        if (iter != id_to_latest_idx.end()) {
            mapping[iter->second] = static_cast<int32_t>(read_idx);
        }
    }
    return mapping;
}

// Project `predicate` onto the read schema, rewriting each leaf's field index via
// `latest_to_read_idx` (predicate's source index space → read schema position).
//
// Inclusive semantics, matching paimon Java's PredicateProjectionConverter:
//   - Leaf whose field is not in the read schema: dropped (returns nullopt).
//   - AND: drop non-projectable children, keep the rest. Safe because if `A AND B`
//     holds for a row, A holds too; the projected predicate is a superset
//     (necessary, not sufficient).
//   - OR: every child must be projectable. If any child is dropped, drop the
//     whole OR — otherwise a row that only satisfied the dropped branch would
//     falsely pass.
//   - Predicates without a field index (e.g. full-text / vector search) flow
//     through unchanged.
Result<std::optional<std::shared_ptr<Predicate>>> ProjectPredicate(
    const std::map<int32_t, int32_t>& latest_to_read_idx,
    const std::shared_ptr<Predicate>& predicate) {
    if (auto leaf = std::dynamic_pointer_cast<LeafPredicateImpl>(predicate)) {
        auto iter = latest_to_read_idx.find(leaf->FieldIndex());
        if (iter == latest_to_read_idx.end()) {
            return std::optional<std::shared_ptr<Predicate>>{};
        }
        if (iter->second == leaf->FieldIndex()) {
            return std::optional<std::shared_ptr<Predicate>>{predicate};
        }
        return std::optional<std::shared_ptr<Predicate>>{
            std::static_pointer_cast<Predicate>(leaf->NewLeafPredicate(iter->second))};
    }
    if (auto compound = std::dynamic_pointer_cast<CompoundPredicateImpl>(predicate)) {
        const bool is_and = compound->GetFunction().GetType() == Function::Type::AND;
        std::vector<std::shared_ptr<Predicate>> projected_children;
        projected_children.reserve(compound->Children().size());
        bool any_changed = false;
        for (const auto& child : compound->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<Predicate>> projected_child,
                                   ProjectPredicate(latest_to_read_idx, child));
            if (!projected_child.has_value()) {
                if (!is_and) {
                    return std::optional<std::shared_ptr<Predicate>>{};
                }
                any_changed = true;
                continue;
            }
            if (projected_child.value() != child) {
                any_changed = true;
            }
            projected_children.push_back(std::move(projected_child.value()));
        }
        if (projected_children.empty()) {
            return std::optional<std::shared_ptr<Predicate>>{};
        }
        if (projected_children.size() == 1) {
            return std::optional<std::shared_ptr<Predicate>>{std::move(projected_children[0])};
        }
        if (!any_changed) {
            return std::optional<std::shared_ptr<Predicate>>{predicate};
        }
        return std::optional<std::shared_ptr<Predicate>>{std::static_pointer_cast<Predicate>(
            compound->NewCompoundPredicate(projected_children))};
    }
    return std::optional<std::shared_ptr<Predicate>>{predicate};
}

Result<std::shared_ptr<Predicate>> ProjectAndValidatePredicate(
    const arrow::Schema& read_schema, const std::map<int32_t, int32_t>& latest_to_read_idx,
    const std::shared_ptr<Predicate>& predicate) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<Predicate>> projected,
                           ProjectPredicate(latest_to_read_idx, predicate));
    if (!projected.has_value()) {
        return std::shared_ptr<Predicate>{};
    }
    PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
        read_schema, projected.value(), /*validate_field_idx=*/true));
    PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithLiterals(projected.value()));
    return projected.value();
}
}  // namespace

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
            if ((core_options.RowTrackingEnabled() ||
                 core_options.KeyValueSequenceNumberEnabled()) &&
                field_id == SpecialFields::SequenceNumber().Id()) {
                read_data_fields.push_back(SpecialFields::SequenceNumber());
                continue;
            }
            if (field_id == SpecialFields::ValueKind().Id()) {
                read_data_fields.push_back(SpecialFields::ValueKind());
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
            if ((core_options.RowTrackingEnabled() ||
                 core_options.KeyValueSequenceNumberEnabled()) &&
                name == SpecialFields::SequenceNumber().Name()) {
                read_data_fields.push_back(SpecialFields::SequenceNumber());
                continue;
            }
            if (name == SpecialFields::ValueKind().Name()) {
                read_data_fields.push_back(SpecialFields::ValueKind());
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
    // Project the upstream predicate onto `read_schema`. Predicates carry field indices
    // pointing into the latest table schema; rewrite them to positions in `read_schema_`
    // so downstream readers can apply them directly.
    std::shared_ptr<Predicate> projected_predicate;
    if (context->GetPredicate()) {
        auto latest_to_read_idx = BuildLatestToReadIdxMapping(*table_schema, read_data_fields);
        PAIMON_ASSIGN_OR_RAISE(
            projected_predicate,
            ProjectAndValidatePredicate(*read_schema, latest_to_read_idx, context->GetPredicate()));
    }

    return std::unique_ptr<InternalReadContext>(new InternalReadContext(
        context, table_schema, read_schema, core_options, std::move(projected_predicate)));
}

InternalReadContext::InternalReadContext(const std::shared_ptr<ReadContext>& read_context,
                                         const std::shared_ptr<TableSchema>& table_schema,
                                         const std::shared_ptr<arrow::Schema>& read_schema,
                                         const CoreOptions& options,
                                         std::shared_ptr<Predicate> projected_predicate)
    : read_context_(read_context),
      table_schema_(table_schema),
      read_schema_(read_schema),
      options_(options),
      projected_predicate_(std::move(projected_predicate)) {}

Result<std::shared_ptr<InternalReadContext>> InternalReadContext::CreateWithSchema(
    const std::shared_ptr<InternalReadContext>& original,
    const std::shared_ptr<arrow::Schema>& new_read_schema) {
    // The wrapped read_context still holds the caller-supplied predicate (in the latest
    // table schema's index space). Re-project it against `new_read_schema` directly,
    // rather than re-projecting the already-projected predicate sitting on `original`.
    std::shared_ptr<Predicate> projected_predicate;
    if (original->read_context_->GetPredicate()) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> new_read_data_fields,
                               DataField::ConvertArrowSchemaToDataFields(new_read_schema));
        auto latest_to_read_idx =
            BuildLatestToReadIdxMapping(*original->table_schema_, new_read_data_fields);
        PAIMON_ASSIGN_OR_RAISE(projected_predicate, ProjectAndValidatePredicate(
                                                        *new_read_schema, latest_to_read_idx,
                                                        original->read_context_->GetPredicate()));
    }
    return std::shared_ptr<InternalReadContext>(
        new InternalReadContext(original->read_context_, original->table_schema_, new_read_schema,
                                original->options_, std::move(projected_predicate)));
}

}  // namespace paimon
