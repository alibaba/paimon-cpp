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

#include "paimon/data/shredding/map_shared_shredding_schema_utils.h"

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "fmt/format.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

Result<std::unique_ptr<::ArrowSchema>> MapSharedShreddingSchemaUtils::LogicalToPhysicalSchema(
    std::unique_ptr<::ArrowSchema> logical_schema,
    const std::map<std::string, int32_t>& field_to_num_columns) {
    if (!logical_schema) {
        return Status::Invalid("logical schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_logical_schema,
                                      arrow::ImportSchema(logical_schema.get()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> physical_schema,
                           MapSharedShreddingUtils::LogicalToPhysicalSchema(arrow_logical_schema,
                                                                            field_to_num_columns));
    auto c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*physical_schema, c_schema.get()));
    return c_schema;
}

Result<std::unique_ptr<::ArrowSchema>> MapSharedShreddingSchemaUtils::AttachMetadataToSchema(
    std::unique_ptr<::ArrowSchema> physical_schema,
    const std::map<std::string, MapSharedShreddingFieldMeta>& field_name_to_meta,
    const std::string& compression) {
    if (!physical_schema) {
        return Status::Invalid("physical schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_physical_schema,
                                      arrow::ImportSchema(physical_schema.get()));

    arrow::FieldVector updated_fields = arrow_physical_schema->fields();
    for (const auto& [field_name, field_meta] : field_name_to_meta) {
        int32_t field_index = arrow_physical_schema->GetFieldIndex(field_name);
        if (field_index < 0) {
            return Status::Invalid(fmt::format(
                "Shared-shredding field '{}' not found in physical schema.", field_name));
        }

        const auto& field = arrow_physical_schema->field(field_index);
        auto metadata = field->metadata() ? field->metadata()->Copy()
                                          : std::make_shared<arrow::KeyValueMetadata>();
        PAIMON_RETURN_NOT_OK(
            MapSharedShreddingUtils::SerializeMetadata(field_meta, compression, metadata.get()));
        updated_fields[field_index] = field->WithMetadata(metadata);
    }

    auto updated_schema =
        arrow::schema(std::move(updated_fields), arrow_physical_schema->metadata());
    auto c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*updated_schema, c_schema.get()));
    return c_schema;
}

Result<MapSharedShreddingFieldMeta> MapSharedShreddingSchemaUtils::ExtractMetadataFromField(
    std::unique_ptr<::ArrowSchema> physical_schema, const std::string& field_name,
    const std::string& compression) {
    if (!physical_schema) {
        return Status::Invalid("physical schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_physical_schema,
                                      arrow::ImportSchema(physical_schema.get()));
    const auto& field = arrow_physical_schema->GetFieldByName(field_name);
    if (!field) {
        return Status::Invalid(
            fmt::format("Shared-shredding field '{}' not found in physical schema.", field_name));
    }

    auto metadata =
        field->metadata() ? field->metadata()->Copy() : std::shared_ptr<arrow::KeyValueMetadata>();
    return MapSharedShreddingUtils::DeserializeMetadata(metadata, compression);
}

}  // namespace paimon
