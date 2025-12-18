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

#include "paimon/core/utils/manifest_meta_reader.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/util.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;

ManifestMetaReader::ManifestMetaReader(std::unique_ptr<BatchReader>&& reader,
                                       const std::shared_ptr<arrow::DataType>& target_type,
                                       const std::shared_ptr<MemoryPool>& pool)
    : reader_(std::move(reader)), target_type_(target_type), pool_(GetArrowPool(pool)) {}

Result<BatchReader::ReadBatch> ManifestMetaReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatch src_result, reader_->NextBatch());
    auto& c_array = src_result.first;
    auto& c_schema = src_result.second;
    if (!c_array) {
        return BatchReader::MakeEofBatch();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> target_array,
                           AlignArrayWithSchema(arrow_array, target_type_, pool_.get()));
    std::unique_ptr<ArrowArray> target_c_arrow_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> target_c_schema = std::make_unique<ArrowSchema>();

    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*target_array, target_c_arrow_array.get(), target_c_schema.get()));
    return std::make_pair(std::move(target_c_arrow_array), std::move(target_c_schema));
}

Result<std::shared_ptr<arrow::Array>> ManifestMetaReader::AlignArrayWithSchema(
    const std::shared_ptr<arrow::Array>& src_array,
    const std::shared_ptr<arrow::DataType>& target_type, arrow::MemoryPool* pool) {
    const auto src_kind = src_array->type()->id();
    switch (src_kind) {
        case arrow::Type::type::LIST: {
            auto list_src_array =
                arrow::internal::checked_pointer_cast<arrow::ListArray>(src_array);
            auto list_target_type =
                arrow::internal::checked_pointer_cast<arrow::ListType>(target_type);
            if (!list_target_type) {
                return Status::Invalid(
                    "Complete non exist field failed, target type cannot cast to a list data "
                    "type");
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> converted,
                                   AlignArrayWithSchema(list_src_array->values(),
                                                        list_target_type->value_type(), pool));
            auto new_list_type = std::make_shared<arrow::ListType>(converted->type());
            return std::make_shared<arrow::ListArray>(
                new_list_type, list_src_array->length(), list_src_array->value_offsets(), converted,
                list_src_array->null_bitmap(), list_src_array->null_count(),
                list_src_array->offset());
        }
        case arrow::Type::type::MAP: {
            auto map_src_array = arrow::internal::checked_pointer_cast<arrow::MapArray>(src_array);
            auto map_target_type =
                arrow::internal::checked_pointer_cast<arrow::MapType>(target_type);
            if (!map_target_type) {
                return Status::Invalid(
                    "Complete non exist field failed, target type cannot cast to a map data "
                    "type");
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Array> key_converted,
                AlignArrayWithSchema(map_src_array->keys(), map_target_type->key_type(), pool));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Array> value_converted,
                AlignArrayWithSchema(map_src_array->items(), map_target_type->item_type(), pool));
            auto new_map_type =
                std::make_shared<arrow::MapType>(key_converted->type(), value_converted->type());
            return std::make_shared<arrow::MapArray>(
                new_map_type, map_src_array->length(), map_src_array->value_offsets(),
                key_converted, value_converted, map_src_array->null_bitmap(),
                map_src_array->null_count(), map_src_array->offset());
        }
        case arrow::Type::type::STRUCT: {
            auto struct_src_array =
                arrow::internal::checked_pointer_cast<arrow::StructArray>(src_array);
            auto struct_target_type =
                arrow::internal::checked_pointer_cast<arrow::StructType>(target_type);
            if (!struct_target_type) {
                return Status::Invalid(
                    "Complete non exist field failed, target type cannot cast to a struct data "
                    "type");
            }
            std::vector<std::string> field_names;
            arrow::ArrayVector converted_array;
            field_names.reserve(target_type->num_fields());
            converted_array.reserve(target_type->num_fields());
            for (int32_t i = 0; i < struct_target_type->num_fields(); i++) {
                const auto& target_field = struct_target_type->field(i);
                const std::string& target_name = target_field->name();
                field_names.push_back(target_name);
                auto src_field = struct_src_array->GetFieldByName(target_name);
                if (src_field) {
                    PAIMON_ASSIGN_OR_RAISE(
                        std::shared_ptr<arrow::Array> converted,
                        AlignArrayWithSchema(src_field, target_field->type(), pool));
                    converted_array.push_back(converted);
                } else {
                    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                        std::shared_ptr<arrow::Array> null_array,
                        arrow::MakeArrayOfNull(target_field->type(), struct_src_array->length(),
                                               pool));
                    converted_array.push_back(null_array);
                }
            }
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> arrow_array,
                arrow::StructArray::Make(
                    converted_array, field_names, struct_src_array->null_bitmap(),
                    struct_src_array->null_count(), struct_src_array->offset()));
            return arrow_array;
        }
        case arrow::Type::type::INT32:
            // cast for avro format, avro store int8,int16,int32 as int32, so need to cast to
            // correct type
            if (src_kind != target_type->id()) {
                arrow::compute::CastOptions cast_options;
                cast_options.allow_int_overflow = false;
                auto int32_array =
                    arrow::internal::checked_pointer_cast<arrow::Int32Array>(src_array);
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> result,
                    arrow::compute::Cast(*int32_array, target_type, cast_options));
                return result;
            }
        default:
            return src_array;
    }
}

}  // namespace paimon
