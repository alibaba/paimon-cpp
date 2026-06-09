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

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/util/bit_util.h"

namespace arrow {
class StructArray;
}  // namespace arrow

namespace paimon {
class MemoryPool;

/// Pre-cached column metadata for fast access without virtual function calls or checked_cast.
struct CachedColumnMeta {
    /// Null bitmap pointer. nullptr means no nulls (all valid).
    const uint8_t* null_bitmap = nullptr;
    /// Arrow array offset (for sliced arrays).
    int64_t array_offset = 0;
    /// For fixed-width types: raw pointer to values buffer (buffer[1]).
    /// For variable-length types (STRING/BINARY): raw pointer to data buffer (buffer[2]).
    const uint8_t* values_data = nullptr;
    /// For variable-length types (STRING/BINARY): raw pointer to offsets buffer (buffer[1]).
    const int32_t* offsets = nullptr;

    /// Fast null check: directly reads validity bitmap bit.
    inline bool IsNull(int64_t row_id) const {
        return null_bitmap != nullptr &&
               !arrow::bit_util::GetBit(null_bitmap, array_offset + row_id);
    }

    /// Fast fixed-width value access (INT8/INT16/INT32/INT64/FLOAT/DOUBLE/DATE32/TIMESTAMP).
    template <typename T>
    inline T GetFixed(int64_t row_id) const {
        return reinterpret_cast<const T*>(values_data)[array_offset + row_id];
    }

    /// Fast boolean value access (bit-packed in values buffer).
    inline bool GetBool(int64_t row_id) const {
        return arrow::bit_util::GetBit(values_data, array_offset + row_id);
    }

    /// Fast string_view access for non-dictionary STRING/BINARY columns.
    inline std::string_view GetVarLenView(int64_t row_id) const {
        int64_t idx = array_offset + row_id;
        int32_t start = offsets[idx];
        int32_t length = offsets[idx + 1] - start;
        return {reinterpret_cast<const char*>(values_data) + start, static_cast<size_t>(length)};
    }
};

struct ColumnarBatchContext {
    ColumnarBatchContext(const arrow::ArrayVector& array_vec_in,
                         const std::shared_ptr<MemoryPool>& pool_in)
        : pool(pool_in), array_vec(array_vec_in) {
        BuildCachedMeta();
    }

    std::shared_ptr<MemoryPool> pool;
    arrow::ArrayVector array_vec;
    /// Pre-cached metadata per column for fast access.
    std::vector<CachedColumnMeta> cached_meta;

 private:
    void BuildCachedMeta() {
        cached_meta.resize(array_vec.size());
        for (size_t i = 0; i < array_vec.size(); i++) {
            const auto* array = array_vec[i].get();
            auto& meta = cached_meta[i];
            meta.array_offset = array->offset();

            // Cache null bitmap
            if (array->null_count() != 0) {
                meta.null_bitmap = array->null_bitmap_data();
            }

            // Cache data pointers based on type
            const auto& array_data = array->data();
            switch (array->type_id()) {
                case arrow::Type::BOOL:
                case arrow::Type::INT8:
                case arrow::Type::INT16:
                case arrow::Type::INT32:
                case arrow::Type::DATE32:
                case arrow::Type::INT64:
                case arrow::Type::FLOAT:
                case arrow::Type::DOUBLE: {
                    // Fixed-width: values in buffer[1]
                    if (array_data->buffers.size() > 1 && array_data->buffers[1]) {
                        meta.values_data = array_data->buffers[1]->data();
                    }
                    break;
                }
                case arrow::Type::STRING:
                case arrow::Type::BINARY: {
                    // Variable-length: offsets in buffer[1], data in buffer[2]
                    if (array_data->buffers.size() > 2) {
                        if (array_data->buffers[1]) {
                            meta.offsets =
                                reinterpret_cast<const int32_t*>(array_data->buffers[1]->data());
                        }
                        if (array_data->buffers[2]) {
                            meta.values_data = array_data->buffers[2]->data();
                        }
                    }
                    break;
                }
                default:
                    // TIMESTAMP, DECIMAL, DICTIONARY, LIST, MAP, STRUCT — not cached, use array_vec
                    // fallback
                    break;
            }
        }
    }
};
}  // namespace paimon
