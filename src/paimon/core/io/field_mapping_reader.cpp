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
#include "paimon/core/io/field_mapping_reader.h"

#include <cassert>
#include <cstddef>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/util.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/scalar.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/casting/cast_executor.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/memory/bytes.h"
#include "paimon/reader/batch_reader.h"

namespace paimon {
class MemoryPool;

FieldMappingReader::FieldMappingReader(int32_t field_count, std::unique_ptr<BatchReader>&& reader,
                                       const BinaryRow& partition,
                                       std::unique_ptr<FieldMapping>&& mapping,
                                       const std::shared_ptr<MemoryPool>& pool)
    : field_count_(field_count),
      arrow_pool_(GetArrowPool(pool)),
      reader_(std::move(reader)),
      partition_(partition),
      partition_info_(mapping->partition_info),
      non_partition_info_(mapping->non_partition_info),
      non_exist_field_info_(mapping->non_exist_field_info) {
    if (non_exist_field_info_ != std::nullopt || partition_info_ != std::nullopt) {
        need_mapping_ = true;
    }

    for (int32_t i = 0;
         i < static_cast<int32_t>(non_partition_info_.idx_in_target_read_schema.size()); i++) {
        if (i != non_partition_info_.idx_in_target_read_schema[i]) {
            need_mapping_ = true;
        }
        if (non_partition_info_.cast_executors[i] != nullptr) {
            need_casting_ = true;
        }
    }
}

Result<std::shared_ptr<arrow::Array>> FieldMappingReader::CastNonPartitionArrayIfNeed(
    const std::shared_ptr<arrow::Array>& src_array) const {
    if (!need_casting_) {
        return src_array;
    }
    auto* struct_array = arrow::internal::checked_cast<arrow::StructArray*>(src_array.get());
    int32_t field_count = struct_array->num_fields();
    assert(static_cast<size_t>(field_count) == non_partition_info_.cast_executors.size());
    arrow::ArrayVector casted_array;
    std::vector<std::string> casted_field_names;
    casted_array.reserve(field_count);
    casted_field_names.reserve(field_count);
    for (int32_t i = 0; i < field_count; i++) {
        if (non_partition_info_.cast_executors[i] != nullptr) {
            auto single_column_array = struct_array->field(i);
            // if src array is dict, cast to string first
            auto dict_array =
                std::dynamic_pointer_cast<arrow::DictionaryArray>(single_column_array);
            if (dict_array) {
                PAIMON_ASSIGN_OR_RAISE(
                    single_column_array,
                    CastingUtils::Cast(dict_array, /*target_type=*/arrow::utf8(),
                                       arrow::compute::CastOptions::Safe(), arrow_pool_.get()));
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Array> casted,
                non_partition_info_.cast_executors[i]->Cast(
                    single_column_array, non_partition_info_.non_partition_read_schema[i].Type(),
                    arrow_pool_.get()));
            casted_array.push_back(casted);
            casted_field_names.push_back(non_partition_info_.non_partition_read_schema[i].Name());
        } else {
            // read and data type may both be string type, but after adapter transform, type may be
            // dictionary, need reconstruct struct type
            casted_array.push_back(struct_array->field(i));
            casted_field_names.push_back(non_partition_info_.non_partition_data_schema[i].Name());
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::StructArray::Make(casted_array, casted_field_names));
    return arrow_array;
}

Result<BatchReader::ReadBatchWithBitmap> FieldMappingReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap non_partition_result_with_bitmap,
                           reader_->NextBatchWithBitmap());
    if (!need_mapping_ && !need_casting_) {
        return non_partition_result_with_bitmap;
    }
    if (BatchReader::IsEofBatch(non_partition_result_with_bitmap)) {
        // read finish
        partition_array_.reset();
        non_exist_array_.reset();
        return non_partition_result_with_bitmap;
    }
    auto& [non_partition_result, bitmap] = non_partition_result_with_bitmap;
    auto& [c_array, c_schema] = non_partition_result;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> non_partition_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));

    arrow::ArrayVector target_array(field_count_);
    std::vector<std::string> target_field_names(field_count_);
    // mapping non-partition array
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> casted_non_partition_array,
                           CastNonPartitionArrayIfNeed(non_partition_array));
    MappingFields(casted_non_partition_array, non_partition_info_.non_partition_read_schema,
                  non_partition_info_.idx_in_target_read_schema, &target_array,
                  &target_field_names);

    // mapping partition array
    if (partition_info_ != std::nullopt) {
        if (!partition_array_ || partition_array_->length() < non_partition_array->length()) {
            PAIMON_ASSIGN_OR_RAISE(partition_array_,
                                   GeneratePartitionArray(non_partition_array->length()));
        }
        auto trim_partition_array = partition_array_->Slice(0, non_partition_array->length());
        MappingFields(trim_partition_array, partition_info_.value().partition_read_schema,
                      partition_info_.value().idx_in_target_read_schema, &target_array,
                      &target_field_names);
    }
    // mapping non-exist array
    if (non_exist_field_info_ != std::nullopt) {
        if (!non_exist_array_ || non_exist_array_->length() < non_partition_array->length()) {
            PAIMON_ASSIGN_OR_RAISE(non_exist_array_,
                                   GenerateNonExistArray(non_partition_array->length()));
        }
        auto trim_non_exist_array = non_exist_array_->Slice(0, non_partition_array->length());
        MappingFields(trim_non_exist_array, non_exist_field_info_.value().non_exist_read_schema,
                      non_exist_field_info_.value().idx_in_target_read_schema, &target_array,
                      &target_field_names);
    }

    // construct target array
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::StructArray::Make(target_array, target_field_names));
    std::unique_ptr<ArrowArray> target_c_arrow_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> target_c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*arrow_array, target_c_arrow_array.get(), target_c_schema.get()));
    auto target_batch = std::make_pair(std::move(target_c_arrow_array), std::move(target_c_schema));
    return std::make_pair(std::move(target_batch), std::move(bitmap));
}

Result<std::shared_ptr<arrow::Array>> FieldMappingReader::GenerateSinglePartitionArray(
    int32_t idx, int32_t batch_size) const {
    const auto& type = partition_info_.value().partition_read_schema[idx].Type();
    if (partition_.IsNullAt(partition_info_.value().idx_in_partition[idx])) {
        // for null partition value
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> null_array,
            arrow::MakeArrayOfNull(type, batch_size, arrow_pool_.get()));
        return null_array;
    }
    auto type_id = type->id();
    std::shared_ptr<arrow::Scalar> scalar;
    switch (type_id) {
        case arrow::Type::type::BOOL: {
            bool value = partition_.GetBoolean(partition_info_.value().idx_in_partition[idx]);
            scalar = std::make_shared<arrow::BooleanScalar>(value);
            break;
        }
        case arrow::Type::type::INT8: {
            int8_t value = partition_.GetByte(partition_info_.value().idx_in_partition[idx]);
            scalar = std::make_shared<arrow::Int8Scalar>(value);
            break;
        }
        case arrow::Type::type::INT16: {
            int16_t value = partition_.GetShort(partition_info_.value().idx_in_partition[idx]);
            scalar = std::make_shared<arrow::Int16Scalar>(value);
            break;
        }
        case arrow::Type::type::INT32: {
            int32_t value = partition_.GetInt(partition_info_.value().idx_in_partition[idx]);
            scalar = std::make_shared<arrow::Int32Scalar>(value);
            break;
        }
        case arrow::Type::type::INT64: {
            int64_t value = partition_.GetLong(partition_info_.value().idx_in_partition[idx]);
            scalar = std::make_shared<arrow::Int64Scalar>(value);
            break;
        }
        case arrow::Type::type::FLOAT: {
            float value = partition_.GetFloat(partition_info_.value().idx_in_partition[idx]);
            scalar = std::make_shared<arrow::FloatScalar>(value);
            break;
        }
        case arrow::Type::type::DOUBLE: {
            double value = partition_.GetDouble(partition_info_.value().idx_in_partition[idx]);
            scalar = std::make_shared<arrow::DoubleScalar>(value);
            break;
        }
        case arrow::Type::type::STRING: {
            BinaryString value =
                partition_.GetString(partition_info_.value().idx_in_partition[idx]);
            scalar = std::make_shared<arrow::StringScalar>(value.ToString());
            break;
        }
        case arrow::Type::type::BINARY: {
            auto value = partition_.GetBinary(partition_info_.value().idx_in_partition[idx]);
            std::string value_str(value->data(), value->size());
            scalar = std::make_shared<arrow::BinaryScalar>(value_str);
            break;
        }
        case arrow::Type::type::DATE32: {
            int32_t value = partition_.GetDate(partition_info_.value().idx_in_partition[idx]);
            scalar = std::make_shared<arrow::Date32Scalar>(value);
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("Not support arrow type {} for partition",
                            partition_info_.value().partition_read_schema[idx].Type()->ToString()));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> arrow_array,
        arrow::MakeArrayFromScalar(*scalar, batch_size, arrow_pool_.get()));
    return arrow_array;
}

Result<std::shared_ptr<arrow::Array>> FieldMappingReader::GeneratePartitionArray(
    int32_t batch_size) const {
    arrow::ArrayVector partition_array;
    std::vector<std::string> partition_field_names;
    partition_array.reserve(partition_info_.value().partition_read_schema.size());
    partition_field_names.reserve(partition_info_.value().partition_read_schema.size());
    for (size_t i = 0; i < partition_info_.value().partition_read_schema.size(); i++) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> single_partition_array,
                               GenerateSinglePartitionArray(i, batch_size));
        partition_array.push_back(single_partition_array);
        partition_field_names.push_back(partition_info_.value().partition_read_schema[i].Name());
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> arrow_array,
        arrow::StructArray::Make(partition_array, partition_field_names));
    return arrow_array;
}

Result<std::shared_ptr<arrow::Array>> FieldMappingReader::GenerateNonExistArray(
    int32_t batch_size) const {
    arrow::ArrayVector non_exist_array;
    std::vector<std::string> non_exist_field_names;
    non_exist_array.reserve(non_exist_field_info_.value().non_exist_read_schema.size());
    non_exist_field_names.reserve(non_exist_field_info_.value().non_exist_read_schema.size());
    for (const auto& non_exist_field : non_exist_field_info_.value().non_exist_read_schema) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> null_array,
            arrow::MakeArrayOfNull(non_exist_field.Type(), batch_size, arrow_pool_.get()));
        non_exist_array.push_back(null_array);
        non_exist_field_names.push_back(non_exist_field.Name());
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> arrow_array,
        arrow::StructArray::Make(non_exist_array, non_exist_field_names));
    return arrow_array;
}

void FieldMappingReader::MappingFields(const std::shared_ptr<arrow::Array>& data_array,
                                       const std::vector<DataField>& read_fields_of_data_array,
                                       const std::vector<int32_t>& idx_in_target_schema,
                                       arrow::ArrayVector* target_array,
                                       std::vector<std::string>* target_field_names) {
    auto* struct_array = arrow::internal::checked_cast<arrow::StructArray*>(data_array.get());
    assert(struct_array);
    assert(struct_array->fields().size() == idx_in_target_schema.size());
    for (size_t i = 0; i < idx_in_target_schema.size(); i++) {
        // target type may be string type, but after adapter transform, type may be dictionary,
        // need reconstruct struct type
        (*target_array)[idx_in_target_schema[i]] = struct_array->field(i);
        (*target_field_names)[idx_in_target_schema[i]] = read_fields_of_data_array[i].Name();
    }
}

}  // namespace paimon
