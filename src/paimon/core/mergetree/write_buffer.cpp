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

#include "paimon/core/mergetree/write_buffer.h"

#include <cassert>
#include <utility>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/io/key_value_in_memory_record_reader.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/data/decimal.h"

namespace paimon {

WriteBuffer::WriteBuffer(
    const std::shared_ptr<arrow::DataType>& value_type,
    const std::vector<std::string>& trimmed_primary_keys,
    const std::vector<std::string>& user_defined_sequence_fields,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
    const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      value_type_(value_type),
      trimmed_primary_keys_(trimmed_primary_keys),
      user_defined_sequence_fields_(user_defined_sequence_fields),
      key_comparator_(key_comparator),
      merge_function_wrapper_(merge_function_wrapper) {}

Status WriteBuffer::Write(std::unique_ptr<RecordBatch>&& moved_batch) {
    if (ArrowArrayIsReleased(moved_batch->GetData())) {
        return Status::Invalid("invalid batch: data is released");
    }
    std::unique_ptr<RecordBatch> batch = std::move(moved_batch);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(batch->GetData(), value_type_));
    auto value_struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
    if (value_struct_array == nullptr) {
        return Status::Invalid("invalid RecordBatch: cannot cast to StructArray");
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t memory_in_bytes, EstimateMemoryUse(value_struct_array));
    current_memory_in_bytes_ += memory_in_bytes;

    batch_vec_.push_back(std::move(value_struct_array));
    row_kinds_vec_.push_back(batch->GetRowKind());
    return Status::OK();
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> WriteBuffer::DrainToReaders(
    int64_t* last_sequence_number) {
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
    if (batch_vec_.empty()) {
        return readers;
    }

    readers.reserve(batch_vec_.size());
    for (size_t i = 0; i < batch_vec_.size(); ++i) {
        int64_t sequence_number = *last_sequence_number;
        *last_sequence_number += batch_vec_[i]->length();
        auto in_memory_reader = std::make_unique<KeyValueInMemoryRecordReader>(
            sequence_number, std::move(batch_vec_[i]), std::move(row_kinds_vec_[i]),
            trimmed_primary_keys_, user_defined_sequence_fields_, key_comparator_,
            merge_function_wrapper_, pool_);
        readers.push_back(std::move(in_memory_reader));
    }

    Clear();
    return readers;
}

void WriteBuffer::Clear() {
    batch_vec_.clear();
    row_kinds_vec_.clear();
    current_memory_in_bytes_ = 0;
}

// TODO(jinli.zjw): Consider making the memory estimation more accurate.
// https://github.com/alibaba/paimon-cpp/pull/206#discussion_r3021325389
Result<int64_t> WriteBuffer::EstimateMemoryUse(const std::shared_ptr<arrow::Array>& array) {
    arrow::Type::type type = array->type()->id();
    int64_t null_bits_size_in_bytes = (array->length() + 7) / 8;
    switch (type) {
        case arrow::Type::type::BOOL:
            return null_bits_size_in_bytes + array->length() * sizeof(bool);
        case arrow::Type::type::INT8:
            return null_bits_size_in_bytes + array->length() * sizeof(int8_t);
        case arrow::Type::type::INT16:
            return null_bits_size_in_bytes + array->length() * sizeof(int16_t);
        case arrow::Type::type::INT32:
            return null_bits_size_in_bytes + array->length() * sizeof(int32_t);
        case arrow::Type::type::DATE32:
            return null_bits_size_in_bytes + array->length() * sizeof(int32_t);
        case arrow::Type::type::INT64:
            return null_bits_size_in_bytes + array->length() * sizeof(int64_t);
        case arrow::Type::type::FLOAT:
            return null_bits_size_in_bytes + array->length() * sizeof(float);
        case arrow::Type::type::DOUBLE:
            return null_bits_size_in_bytes + array->length() * sizeof(double);
        case arrow::Type::type::TIMESTAMP:
            return null_bits_size_in_bytes + array->length() * sizeof(int64_t);
        case arrow::Type::type::DECIMAL:
            return null_bits_size_in_bytes + array->length() * sizeof(Decimal::int128_t);
        case arrow::Type::type::STRING:
        case arrow::Type::type::BINARY: {
            auto binary_array =
                arrow::internal::checked_cast<const arrow::BinaryArray*>(array.get());
            assert(binary_array);
            int64_t value_length = binary_array->total_values_length();
            int64_t offset_length = array->length() * sizeof(int32_t);
            return null_bits_size_in_bytes + value_length + offset_length;
        }
        case arrow::Type::type::LIST: {
            auto list_array = arrow::internal::checked_cast<const arrow::ListArray*>(array.get());
            assert(list_array);
            PAIMON_ASSIGN_OR_RAISE(int64_t value_mem, EstimateMemoryUse(list_array->values()));
            return null_bits_size_in_bytes + value_mem;
        }
        case arrow::Type::type::MAP: {
            auto map_array = arrow::internal::checked_cast<const arrow::MapArray*>(array.get());
            assert(map_array);
            PAIMON_ASSIGN_OR_RAISE(int64_t key_mem, EstimateMemoryUse(map_array->keys()));
            PAIMON_ASSIGN_OR_RAISE(int64_t item_mem, EstimateMemoryUse(map_array->items()));
            return null_bits_size_in_bytes + key_mem + item_mem;
        }
        case arrow::Type::type::STRUCT: {
            auto struct_array =
                arrow::internal::checked_cast<const arrow::StructArray*>(array.get());
            assert(struct_array);
            int64_t struct_mem = 0;
            for (const auto& field : struct_array->fields()) {
                PAIMON_ASSIGN_OR_RAISE(int64_t field_mem, EstimateMemoryUse(field));
                struct_mem += field_mem;
            }
            return null_bits_size_in_bytes + struct_mem;
        }
        default:
            return Status::Invalid(fmt::format("Do not support type {} in EstimateMemoryUse",
                                               array->type()->ToString()));
    }
}

}  // namespace paimon
