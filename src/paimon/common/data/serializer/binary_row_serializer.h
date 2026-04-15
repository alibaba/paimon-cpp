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

#pragma once

#include <cstdint>
#include <memory>

#include "paimon/common/data/binary_row.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class DataInputStream;
class MemorySegmentOutputStream;
class MemoryPool;

/// Serializer for `BinaryRow`.
class BinaryRowSerializer {
 public:
    BinaryRowSerializer(int32_t num_fields, const std::shared_ptr<MemoryPool>& pool)
        : num_fields_(num_fields),
          fixed_length_part_size_(BinaryRow::CalculateFixPartSizeInBytes(num_fields)),
          pool_(pool) {}

    int32_t GetArity() const {
        return num_fields_;
    }

    static constexpr int32_t LENGTH_SIZE_IN_BYTES = 4;

    int32_t GetFixedLengthPartSize() const {
        return fixed_length_part_size_;
    }

    /// @return Fixed part length to serialize one row.
    int32_t GetSerializedRowFixedPartLength() const {
        return GetFixedLengthPartSize() + LENGTH_SIZE_IN_BYTES;
    }

    Result<BinaryRow> Deserialize(DataInputStream* source) const;

    Status Serialize(const BinaryRow& row, MemorySegmentOutputStream* target) const;

 private:
    Status SerializeWithoutLength(const BinaryRow& record, MemorySegmentOutputStream* target) const;

    int32_t num_fields_;
    int32_t fixed_length_part_size_;
    std::shared_ptr<MemoryPool> pool_;
};
}  // namespace paimon
