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

#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_map.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_writer.h"

namespace paimon {
/// Utils for generate `BinaryRow`, `BinaryArray`, `BinaryMap` from `InternalRow`, `InternalArray`,
/// and `InternalMap`.
class BinarySerializerUtils {
 public:
    BinarySerializerUtils() = delete;
    ~BinarySerializerUtils() = delete;

    static Result<std::shared_ptr<BinaryArray>> WriteBinaryArray(
        const std::shared_ptr<InternalArray>& value, const std::shared_ptr<arrow::DataType>& type,
        MemoryPool* pool);

    static Result<std::shared_ptr<BinaryMap>> WriteBinaryMap(
        const std::shared_ptr<InternalMap>& value, const std::shared_ptr<arrow::DataType>& type,
        MemoryPool* pool);

    static Result<std::shared_ptr<BinaryRow>> WriteBinaryRow(
        const std::shared_ptr<InternalRow>& value, const std::shared_ptr<arrow::DataType>& type,
        MemoryPool* pool);

 private:
    static Status WriteBinaryData(const std::shared_ptr<arrow::DataType>& type,
                                  const DataGetters* getter, int32_t pos, BinaryWriter* writer,
                                  MemoryPool* pool);
};
}  // namespace paimon
