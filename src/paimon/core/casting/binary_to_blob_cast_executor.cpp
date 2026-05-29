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

#include "paimon/core/casting/binary_to_blob_cast_executor.h"

#include <cstdint>
#include <memory>

#include "arrow/array/array_binary.h"
#include "arrow/buffer.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/status.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> BinaryToBlobCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    return Status::Invalid(
        fmt::format("BinaryToBlobCastExecutor does not support literal cast from {} to {}",
                    static_cast<int>(literal.GetType()), target_type->ToString()));
}

Result<std::shared_ptr<arrow::Array>> BinaryToBlobCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    if (array->type_id() != arrow::Type::BINARY) {
        return Status::Invalid(
            fmt::format("BinaryToBlobCastExecutor only supports binary input, got {}",
                        array->type()->ToString()));
    }
    if (target_type->id() != arrow::Type::LARGE_BINARY) {
        return Status::Invalid(
            fmt::format("BinaryToBlobCastExecutor only supports large_binary target, got {}",
                        target_type->ToString()));
    }

    auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
    if (binary_array->offset() != 0) {
        return Status::Invalid("BinaryToBlobCastExecutor only supports arrays with zero offset");
    }

    const int64_t length = binary_array->length();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Buffer> large_offsets_buffer,
        arrow::AllocateBuffer((length + 1) * static_cast<int64_t>(sizeof(int64_t)), pool));
    auto* large_offsets = reinterpret_cast<int64_t*>(large_offsets_buffer->mutable_data());
    for (int64_t row_index = 0; row_index <= length; row_index++) {
        large_offsets[row_index] = binary_array->value_offset(row_index);
    }

    std::shared_ptr<arrow::Buffer> null_bitmap = binary_array->null_bitmap();
    if (binary_array->null_count() == 0) {
        null_bitmap.reset();
    }

    auto value_data = binary_array->value_data();
    auto array_data =
        arrow::ArrayData::Make(target_type, length, {null_bitmap, large_offsets_buffer, value_data},
                               binary_array->null_count());
    return arrow::MakeArray(array_data);
}

}  // namespace paimon
