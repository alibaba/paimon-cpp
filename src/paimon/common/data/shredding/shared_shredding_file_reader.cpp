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

#include "paimon/common/data/shredding/shared_shredding_file_reader.h"

#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/util/key_value_metadata.h"
#include "fmt/format.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/casting/casting_utils.h"

namespace paimon {
namespace {
// TODO(lisizhuo.lsz): rm kSelectedKeysMetadataKey after zhanyu pr
constexpr const char* kSelectedKeysMetadataKey = "paimon.map.selected-keys";

Result<std::optional<std::vector<std::string>>> GetSelectedKeys(
    const std::shared_ptr<arrow::Field>& field) {
    if (!field->HasMetadata() || !field->metadata()) {
        return std::optional<std::vector<std::string>>();
    }
    int32_t index = field->metadata()->FindKey(kSelectedKeysMetadataKey);
    if (index < 0) {
        return std::optional<std::vector<std::string>>();
    }
    std::string selected_keys = field->metadata()->value(index);
    // paimon will not ignore empty for '' is a valid key
    return std::optional<std::vector<std::string>>(
        StringUtils::Split(selected_keys, ",", /*ignore_empty=*/false));
}

}  // namespace

Result<std::unique_ptr<SharedShreddingFileReader>> SharedShreddingFileReader::Create(
    std::unique_ptr<FileBatchReader>&& reader, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> file_schema, reader->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_arrow_schema,
                                      arrow::ImportSchema(file_schema.get()));

    std::map<std::string, MapSharedShreddingFieldMeta> shared_shredding_name_to_meta;
    for (const auto& field : file_arrow_schema->fields()) {
        std::shared_ptr<arrow::KeyValueMetadata> metadata =
            std::const_pointer_cast<arrow::KeyValueMetadata>(field->metadata());
        if (MapSharedShreddingUtils::HasShreddingMetadata(metadata)) {
            PAIMON_ASSIGN_OR_RAISE(
                MapSharedShreddingFieldMeta meta,
                MapSharedShreddingUtils::DeserializeMetadata(
                    metadata, MapSharedShreddingDefine::kDefaultDictCompression));
            shared_shredding_name_to_meta[field->name()] = std::move(meta);
        }
    }
    return std::unique_ptr<SharedShreddingFileReader>(
        new SharedShreddingFileReader(std::move(reader), shared_shredding_name_to_meta, pool));
}

SharedShreddingFileReader::SharedShreddingFileReader(
    std::unique_ptr<FileBatchReader>&& reader,
    const std::map<std::string, MapSharedShreddingFieldMeta>& shared_shredding_name_to_meta,
    const std::shared_ptr<MemoryPool>& pool)
    : arrow_pool_(GetArrowPool(pool)),
      reader_(std::move(reader)),
      shared_shredding_name_to_meta_(shared_shredding_name_to_meta) {}

Result<std::unique_ptr<::ArrowSchema>> SharedShreddingFileReader::GetFileSchema() const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> physical_schema,
                           reader_->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> physical_arrow_schema,
                                      arrow::ImportSchema(physical_schema.get()));

    arrow::FieldVector logical_fields = physical_arrow_schema->fields();
    for (int32_t i = 0; i < physical_arrow_schema->num_fields(); ++i) {
        const auto& field = physical_arrow_schema->field(i);
        std::shared_ptr<arrow::KeyValueMetadata> metadata =
            std::const_pointer_cast<arrow::KeyValueMetadata>(field->metadata());
        if (!MapSharedShreddingUtils::HasShreddingMetadata(metadata)) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(logical_fields[i], ToLogicalMapField(field));
    }

    auto logical_schema = arrow::schema(std::move(logical_fields));
    auto c_logical_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*logical_schema, c_logical_schema.get()));
    return c_logical_schema;
}

Result<std::shared_ptr<arrow::Field>> SharedShreddingFileReader::ToLogicalMapField(
    const std::shared_ptr<arrow::Field>& physical_field) {
    auto physical_type = std::dynamic_pointer_cast<arrow::StructType>(physical_field->type());
    if (!physical_type) {
        return Status::Invalid(fmt::format("shared-shredding field {} is not a physical struct",
                                           physical_field->name()));
    }

    std::shared_ptr<arrow::DataType> value_type;
    bool value_nullable = true;
    for (const auto& child : physical_type->fields()) {
        if (child->name() == MapSharedShreddingDefine::kFieldMapping ||
            child->name() == MapSharedShreddingDefine::kOverflow) {
            continue;
        }
        value_type = child->type();
        value_nullable = child->nullable();
        break;
    }
    if (!value_type) {
        return Status::Invalid(fmt::format("cannot infer shared-shredding value type for field {}",
                                           physical_field->name()));
    }
    return arrow::field(
        physical_field->name(),
        arrow::map(arrow::utf8(), arrow::field("value", value_type, value_nullable)),
        physical_field->nullable());
}

Status SharedShreddingFileReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!read_schema) {
        return Status::Invalid("invalid read schema in SharedShreddingFileReader, cannot be null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> logical_read_schema,
                                      arrow::ImportSchema(read_schema));
    std::vector<std::string> shared_shredding_names;
    for (const auto& field : logical_read_schema->fields()) {
        if (shared_shredding_name_to_meta_.find(field->name()) !=
            shared_shredding_name_to_meta_.end()) {
            shared_shredding_names.push_back(field->name());
        }
    }
    if (shared_shredding_names.empty()) {
        // suppose not fall into SharedShreddingFileReader
        return Status::Invalid("do not exist shared shredding columns in read schema");
    }
    shared_shredding_name_to_selected_keys_.clear();
    shared_shredding_name_to_map_type_.clear();
    arrow::FieldVector resolved_fields = logical_read_schema->fields();
    for (const auto& name : shared_shredding_names) {
        const auto& field = logical_read_schema->GetFieldByName(name);
        if (!field) {
            return Status::Invalid(
                fmt::format("cannot find shared-shredding field in read schema"));
        }
        auto meta_iter = shared_shredding_name_to_meta_.find(field->name());
        if (meta_iter == shared_shredding_name_to_meta_.end()) {
            return Status::Invalid(
                fmt::format("cannot find shared-shredding metadata for field {}", field->name()));
        }
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::vector<std::string>> selected_keys_opt,
                               GetSelectedKeys(field));
        std::vector<std::string> selected_keys;
        if (!selected_keys_opt) {
            // select all keys
            selected_keys.reserve(meta_iter->second.name_to_id.size());
            for (const auto& [key_name, _] : meta_iter->second.name_to_id) {
                selected_keys.push_back(key_name);
            }
        } else {
            std::set<std::string> seen_keys;
            for (const auto& selected_key : selected_keys_opt.value()) {
                if (!seen_keys.insert(selected_key).second) {
                    return Status::Invalid(
                        fmt::format("duplicate key [{}] in paimon.map.selected-keys for field {}",
                                    selected_key, field->name()));
                }
                selected_keys.push_back(selected_key);
            }
        }
        shared_shredding_name_to_selected_keys_[field->name()] = selected_keys;

        auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(field->type());
        shared_shredding_name_to_map_type_[field->name()] = map_type;
        std::set<int32_t> selected_physical_column_ids;
        bool include_overflow = false;
        for (const auto& selected_key : selected_keys) {
            auto name_iter = meta_iter->second.name_to_id.find(selected_key);
            if (name_iter == meta_iter->second.name_to_id.end()) {
                continue;
            }
            auto column_iter = meta_iter->second.field_to_columns.find(name_iter->second);
            if (column_iter == meta_iter->second.field_to_columns.end()) {
                continue;
            }
            const std::vector<int32_t>& physical_column_ids = column_iter->second;
            selected_physical_column_ids.insert(physical_column_ids.begin(),
                                                physical_column_ids.end());
            PAIMON_ASSIGN_OR_RAISE(bool is_overflow_field, MapSharedShreddingUtils::IsOverflowField(
                                                               meta_iter->second, selected_key));
            include_overflow = include_overflow || is_overflow_field;
        }
        std::shared_ptr<arrow::DataType> resolved_type =
            MapSharedShreddingUtils::BuildSpecificPhysicalStructType(
                map_type->item_type(), selected_physical_column_ids,
                map_type->item_field()->nullable(), include_overflow);
        resolved_fields[logical_read_schema->GetFieldIndex(name)] =
            arrow::field(field->name(), resolved_type, field->nullable());
    }

    auto resolved_schema = arrow::schema(std::move(resolved_fields));
    std::unique_ptr<ArrowSchema> c_resolved_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*resolved_schema, c_resolved_schema.get()));
    return reader_->SetReadSchema(c_resolved_schema.get(), predicate, selection_bitmap);
}

Result<BatchReader::ReadBatch> SharedShreddingFileReader::NextBatch() {
    return Status::Invalid(
        "paimon inner reader SharedShreddingFileReader should use NextBatchWithBitmap");
}

Result<BatchReader::ReadBatchWithBitmap> SharedShreddingFileReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           reader_->NextBatchWithBitmap());
    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
        return batch_with_bitmap;
    }

    auto& [batch, bitmap] = batch_with_bitmap;
    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
    if (!struct_array) {
        return Status::Invalid("cannot cast batch to StructArray in SharedShreddingFileReader");
    }

    arrow::ArrayVector resolved_arrays = struct_array->fields();
    arrow::FieldVector resolved_fields = struct_array->struct_type()->fields();
    for (int32_t field_idx = 0; field_idx < struct_array->num_fields(); ++field_idx) {
        const auto& physical_field = struct_array->struct_type()->field(field_idx);
        auto iter = shared_shredding_name_to_selected_keys_.find(physical_field->name());
        if (iter == shared_shredding_name_to_selected_keys_.end()) {
            continue;
        }
        auto physical_struct_array =
            std::dynamic_pointer_cast<arrow::StructArray>(struct_array->field(field_idx));
        if (!physical_struct_array) {
            return Status::Invalid(fmt::format(
                "cannot cast physical shredding field {} to StructArray", physical_field->name()));
        }

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> logical_map_array,
                               RebuildLogicalMapArray(physical_field, physical_struct_array));
        resolved_arrays[field_idx] = logical_map_array;
        resolved_fields[field_idx] = arrow::field(physical_field->name(), logical_map_array->type(),
                                                  physical_field->nullable());
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> new_struct_array,
                                      arrow::StructArray::Make(resolved_arrays, resolved_fields));
    auto new_c_array = std::make_unique<ArrowArray>();
    auto new_c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*new_struct_array, new_c_array.get(), new_c_schema.get()));
    batch = std::make_pair(std::move(new_c_array), std::move(new_c_schema));
    return batch_with_bitmap;
}

Result<std::shared_ptr<arrow::Array>> SharedShreddingFileReader::RebuildLogicalMapArray(
    const std::shared_ptr<arrow::Field>& physical_field,
    const std::shared_ptr<arrow::StructArray>& physical_struct_array) const {
    std::string shredding_field_name = physical_field->name();
    auto meta_iter = shared_shredding_name_to_meta_.find(shredding_field_name);
    if (meta_iter == shared_shredding_name_to_meta_.end()) {
        return Status::Invalid(fmt::format("cannot find shared-shredding metadata for field {}",
                                           shredding_field_name));
    }
    auto selected_iter = shared_shredding_name_to_selected_keys_.find(shredding_field_name);
    if (selected_iter == shared_shredding_name_to_selected_keys_.end()) {
        return Status::Invalid(
            fmt::format("cannot find selected keys for field {}", shredding_field_name));
    }
    auto map_type_iter = shared_shredding_name_to_map_type_.find(shredding_field_name);
    if (map_type_iter == shared_shredding_name_to_map_type_.end()) {
        return Status::Invalid(
            fmt::format("cannot find logical map type for field {}", shredding_field_name));
    }
    const MapSharedShreddingFieldMeta& meta = meta_iter->second;
    const std::vector<std::string>& selected_keys = selected_iter->second;
    const auto& map_type = map_type_iter->second;

    auto field_mapping_array = std::dynamic_pointer_cast<arrow::ListArray>(
        physical_struct_array->GetFieldByName(MapSharedShreddingDefine::kFieldMapping));
    if (!field_mapping_array) {
        return Status::Invalid(
            fmt::format("cannot find __field_mapping for field {}", shredding_field_name));
    }
    auto field_mapping_values =
        std::dynamic_pointer_cast<arrow::Int32Array>(field_mapping_array->values());
    if (!field_mapping_values) {
        return Status::Invalid("__field_mapping values is not an Int32Array");
    }

    auto selected_key_ids = ResolveSelectedKeyIds(meta, selected_keys);

    std::map<std::string, std::shared_ptr<arrow::Array>> physical_column_name_to_array;
    std::shared_ptr<arrow::MapArray> overflow_array;
    CollectPhysicalColumns(physical_struct_array, &physical_column_name_to_array, &overflow_array);
    for (auto& [_, physical_column_array] : physical_column_name_to_array) {
        if (physical_column_array->type_id() == arrow::Type::DICTIONARY) {
            PAIMON_ASSIGN_OR_RAISE(
                physical_column_array,
                CastingUtils::Cast(physical_column_array, map_type->item_type(),
                                   arrow::compute::CastOptions::Safe(), arrow_pool_.get()));
        }
    }

    std::shared_ptr<arrow::Int32Array> overflow_keys;
    std::shared_ptr<arrow::Array> overflow_items;
    if (overflow_array) {
        overflow_keys =
            arrow::internal::checked_pointer_cast<arrow::Int32Array>(overflow_array->keys());
        overflow_items = overflow_array->items();
        if (!overflow_keys || !overflow_items) {
            return Status::Invalid("__overflow map has invalid key or item array");
        }
        if (overflow_items->type_id() == arrow::Type::DICTIONARY) {
            PAIMON_ASSIGN_OR_RAISE(
                overflow_items,
                CastingUtils::Cast(overflow_items, map_type->item_type(),
                                   arrow::compute::CastOptions::Safe(), arrow_pool_.get()));
        }
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> map_builder_base,
                                      arrow::MakeBuilder(map_type, arrow_pool_.get()));
    auto* map_builder = dynamic_cast<arrow::MapBuilder*>(map_builder_base.get());
    if (!map_builder) {
        return Status::Invalid(
            fmt::format("cannot create MapBuilder for field {}", shredding_field_name));
    }
    auto* key_builder = dynamic_cast<arrow::StringBuilder*>(map_builder->key_builder());
    if (!key_builder) {
        return Status::Invalid(fmt::format("map key builder is not a StringBuilder for field {}",
                                           shredding_field_name));
    }
    arrow::ArrayBuilder* item_builder = map_builder->item_builder();
    if (!item_builder) {
        return Status::Invalid(
            fmt::format("map item builder is null for field {}", shredding_field_name));
    }

    int64_t row_count = physical_struct_array->length();
    int64_t max_item_count = row_count * static_cast<int64_t>(selected_key_ids.size());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->Reserve(row_count));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Reserve(max_item_count));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(item_builder->Reserve(max_item_count));

    for (int64_t row = 0; row < row_count; ++row) {
        if (physical_struct_array->IsNull(row)) {
            // null struct -> null map
            PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->AppendNull());
            continue;
        }
        if (field_mapping_array->IsNull(row)) {
            return Status::Invalid(fmt::format(
                "__field_mapping cannot be null in non-null shared-shredding row for field {}",
                shredding_field_name));
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->Append());
        int32_t mapping_offset = field_mapping_array->value_offset(row);
        int32_t mapping_length = field_mapping_array->value_length(row);
        // follow the sequence in paimon.map.selected-keys
        for (const auto& [selected_key, selected_field_id] : selected_key_ids) {
            bool found = false;
            for (int32_t pos = 0; pos < mapping_length; ++pos) {
                int32_t mapping_index = mapping_offset + pos;
                if (field_mapping_values->IsNull(mapping_index)) {
                    return Status::Invalid("__field_mapping element cannot be null");
                }
                if (field_mapping_values->Value(mapping_index) != selected_field_id) {
                    continue;
                }
                std::string physical_column_name =
                    MapSharedShreddingDefine::PhysicalColumnName(pos);
                auto physical_column_iter =
                    physical_column_name_to_array.find(physical_column_name);
                if (physical_column_iter == physical_column_name_to_array.end()) {
                    return Status::Invalid(
                        fmt::format("cannot find selected physical column {} for field {}",
                                    physical_column_name, shredding_field_name));
                }
                PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Append(selected_key));
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    item_builder->AppendArraySlice(*physical_column_iter->second->data(), row, 1));
                found = true;
                break;
            }
            if (found || !overflow_array) {
                continue;
            }
            int32_t overflow_offset = overflow_array->value_offset(row);
            int32_t overflow_length = overflow_array->value_length(row);
            for (int32_t pos = 0; pos < overflow_length; ++pos) {
                int32_t overflow_index = overflow_offset + pos;
                if (!overflow_keys->IsNull(overflow_index) &&
                    overflow_keys->Value(overflow_index) == selected_field_id) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Append(selected_key));
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(
                        item_builder->AppendArraySlice(*overflow_items->data(), overflow_index, 1));
                    break;
                }
            }
        }
    }
    std::shared_ptr<arrow::MapArray> map_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->Finish(&map_array));
    return map_array;
}

std::vector<std::pair<std::string, int32_t>> SharedShreddingFileReader::ResolveSelectedKeyIds(
    const MapSharedShreddingFieldMeta& meta, const std::vector<std::string>& selected_keys) {
    std::vector<std::pair<std::string, int32_t>> selected_key_ids;
    selected_key_ids.reserve(selected_keys.size());
    for (const auto& selected_key : selected_keys) {
        auto id_iter = meta.name_to_id.find(selected_key);
        if (id_iter == meta.name_to_id.end()) {
            continue;
        }
        selected_key_ids.emplace_back(selected_key, id_iter->second);
    }
    return selected_key_ids;
}

void SharedShreddingFileReader::CollectPhysicalColumns(
    const std::shared_ptr<arrow::StructArray>& physical_struct_array,
    std::map<std::string, std::shared_ptr<arrow::Array>>* physical_column_name_to_array,
    std::shared_ptr<arrow::MapArray>* overflow_array) {
    const auto& struct_type = physical_struct_array->struct_type();
    for (int32_t i = 0; i < struct_type->num_fields(); ++i) {
        const auto& sub_field = struct_type->field(i);
        if (sub_field->name() == MapSharedShreddingDefine::kFieldMapping) {
            continue;
        }
        if (sub_field->name() == MapSharedShreddingDefine::kOverflow) {
            *overflow_array = arrow::internal::checked_pointer_cast<arrow::MapArray>(
                physical_struct_array->field(i));
            continue;
        }
        (*physical_column_name_to_array)[sub_field->name()] = physical_struct_array->field(i);
    }
}

std::shared_ptr<Metrics> SharedShreddingFileReader::GetReaderMetrics() const {
    return reader_->GetReaderMetrics();
}

void SharedShreddingFileReader::Close() {
    reader_->Close();
}

Result<uint64_t> SharedShreddingFileReader::GetPreviousBatchFirstRowNumber() const {
    return reader_->GetPreviousBatchFirstRowNumber();
}

Result<uint64_t> SharedShreddingFileReader::GetNumberOfRows() const {
    return reader_->GetNumberOfRows();
}

bool SharedShreddingFileReader::SupportPreciseBitmapSelection() const {
    return reader_->SupportPreciseBitmapSelection();
}

}  // namespace paimon
