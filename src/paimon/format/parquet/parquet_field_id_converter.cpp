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

#include "paimon/format/parquet/parquet_field_id_converter.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon::parquet {

using arrow::Field;
using arrow::KeyValueMetadata;

const char ParquetFieldIdConverter::PARQUET_FIELD_ID[] = "PARQUET:field_id";

Result<std::shared_ptr<arrow::Schema>> ParquetFieldIdConverter::AddParquetIdsFromPaimonIds(
    const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<std::shared_ptr<Field>> new_fields;
    for (const auto& field : schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<Field> new_field,
            ProcessField(field, ParquetFieldIdConverter::IdConvertType::PAIMON_TO_PARQUET_ID));
        new_fields.push_back(new_field);
    }
    return arrow::schema(new_fields, schema->metadata());
}

Result<std::shared_ptr<arrow::Schema>> ParquetFieldIdConverter::GetPaimonIdsFromParquetIds(
    const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<std::shared_ptr<Field>> new_fields;
    for (const auto& field : schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<Field> new_field,
            ProcessField(field, ParquetFieldIdConverter::IdConvertType::PARQUET_TO_PAIMON_ID));
        new_fields.push_back(new_field);
    }
    return arrow::schema(new_fields, schema->metadata());
}

arrow::Result<std::shared_ptr<const KeyValueMetadata>> ParquetFieldIdConverter::CopyId(
    const std::shared_ptr<const KeyValueMetadata>& metadata,
    ParquetFieldIdConverter::IdConvertType convert_type) {
    auto copy =
        [&](const std::string& from,
            const std::string& to) -> arrow::Result<std::shared_ptr<const KeyValueMetadata>> {
        if (!metadata || !metadata->Contains(from)) {
            return metadata;
        }
        ARROW_ASSIGN_OR_RAISE(auto paimon_id, metadata->Get(from));
        std::vector<std::string> keys = {to};
        std::vector<std::string> values = {paimon_id};
        auto new_meta = KeyValueMetadata::Make(keys, values);
        return metadata->Merge(*new_meta);
    };

    if (convert_type == ParquetFieldIdConverter::IdConvertType::PAIMON_TO_PARQUET_ID) {
        // in write process
        return copy(DataField::FIELD_ID, PARQUET_FIELD_ID);
    } else if (convert_type == ParquetFieldIdConverter::IdConvertType::PARQUET_TO_PAIMON_ID) {
        // in read process
        return copy(PARQUET_FIELD_ID, DataField::FIELD_ID);
    }
    return arrow::Status::Invalid("only support PAIMON_TO_PARQUET_ID and PARQUET_TO_PAIMON_ID");
}

arrow::Result<std::shared_ptr<Field>> ParquetFieldIdConverter::ProcessField(
    const std::shared_ptr<Field>& field, ParquetFieldIdConverter::IdConvertType convert_type) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const KeyValueMetadata> updated_metadata,
                          CopyId(field->metadata(), convert_type));
    auto type = field->type();
    if (type->id() == arrow::Type::STRUCT) {
        auto struct_type = std::static_pointer_cast<arrow::StructType>(type);
        std::vector<std::shared_ptr<Field>> new_fields;
        for (const auto& child : struct_type->fields()) {
            ARROW_ASSIGN_OR_RAISE(auto new_child, ProcessField(child, convert_type));
            new_fields.push_back(new_child);
        }
        auto new_type = arrow::struct_(new_fields);
        return field->WithType(new_type)->WithMergedMetadata(updated_metadata);
    } else if (type->id() == arrow::Type::LIST) {
        auto list_type = std::static_pointer_cast<arrow::ListType>(type);
        ARROW_ASSIGN_OR_RAISE(auto new_value_field,
                              ProcessField(list_type->value_field(), convert_type));
        auto new_type = arrow::list(new_value_field);
        return field->WithType(new_type)->WithMergedMetadata(updated_metadata);
    } else if (type->id() == arrow::Type::MAP) {
        auto map_type = std::static_pointer_cast<arrow::MapType>(type);
        ARROW_ASSIGN_OR_RAISE(auto new_key_field,
                              ProcessField(map_type->key_field(), convert_type));
        ARROW_ASSIGN_OR_RAISE(auto new_item_field,
                              ProcessField(map_type->item_field(), convert_type));
        auto new_type = arrow::map(new_key_field->type(), new_item_field);
        return field->WithType(new_type)->WithMergedMetadata(updated_metadata);
    }

    return field->WithMergedMetadata(updated_metadata);
}

}  // namespace paimon::parquet
