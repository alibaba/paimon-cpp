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

#include "paimon/core/mergetree/in_memory_sort_buffer.h"

#include <cassert>
#include <utility>

#include "arrow/api.h"
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

InMemorySortBuffer::InMemorySortBuffer(int64_t last_sequence_number,
                                       const std::shared_ptr<arrow::DataType>& value_type,
                                       const std::vector<std::string>& trimmed_primary_keys,
                                       const std::vector<std::string>& user_defined_sequence_fields,
                                       bool sequence_fields_ascending,
                                       const std::shared_ptr<FieldsComparator>& key_comparator,
                                       uint64_t write_buffer_size,
                                       const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      value_type_(value_type),
      trimmed_primary_keys_(trimmed_primary_keys),
      user_defined_sequence_fields_(user_defined_sequence_fields),
      sequence_fields_ascending_(sequence_fields_ascending),
      key_comparator_(key_comparator),
      write_buffer_size_(write_buffer_size),
      next_sequence_number_(last_sequence_number + 1) {}

void InMemorySortBuffer::Clear() {
    buffered_batches_.clear();
    current_memory_in_bytes_ = 0;
}

uint64_t InMemorySortBuffer::GetMemorySize() const {
    return current_memory_in_bytes_;
}

Result<bool> InMemorySortBuffer::FlushMemory() {
    return false;
}

Result<bool> InMemorySortBuffer::Write(std::unique_ptr<RecordBatch>&& moved_batch) {
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
    current_memory_in_bytes_ += static_cast<uint64_t>(memory_in_bytes);

    BufferedWriteBatch buffered_batch;
    buffered_batch.first_sequence_number = next_sequence_number_;
    buffered_batch.struct_array = std::move(value_struct_array);
    buffered_batch.row_kinds = batch->GetRowKind();
    next_sequence_number_ += buffered_batch.struct_array->length();
    buffered_batches_.push_back(std::move(buffered_batch));
    return current_memory_in_bytes_ < write_buffer_size_;
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> InMemorySortBuffer::CreateReaders() {
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
    if (buffered_batches_.empty()) {
        return readers;
    }

    readers.reserve(buffered_batches_.size());
    for (auto& buffered_batch : buffered_batches_) {
        auto in_memory_reader = std::make_unique<KeyValueInMemoryRecordReader>(
            buffered_batch.first_sequence_number, buffered_batch.struct_array,
            buffered_batch.row_kinds, trimmed_primary_keys_, user_defined_sequence_fields_,
            sequence_fields_ascending_, key_comparator_, pool_);
        readers.push_back(std::move(in_memory_reader));
    }
    return readers;
}

bool InMemorySortBuffer::HasData() const {
    return !buffered_batches_.empty();
}

// TODO(jinli.zjw): Consider making the memory estimation more accurate.
// https://github.com/alibaba/paimon-cpp/pull/206#discussion_r3021325389
Result<int64_t> InMemorySortBuffer::EstimateMemoryUse(const std::shared_ptr<arrow::Array>& array) {
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
