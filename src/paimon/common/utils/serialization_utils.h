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
#include <cstring>
#include <memory>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/io/byte_order.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/macros.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"

namespace paimon {

/// Utils for serialization.
class SerializationUtils {
 public:
    SerializationUtils() = delete;
    ~SerializationUtils() = delete;

    /// Serialize `BinaryRow`, the difference between this and `BinaryRowSerializer` is
    /// that arity is also serialized here, so the deserialization is schemaless.
    static std::shared_ptr<Bytes> SerializeBinaryRow(const BinaryRow& row, MemoryPool* pool) {
        int32_t size_in_row = row.GetSizeInBytes();
        auto bytes = Bytes::AllocateBytes(size_in_row + sizeof(int32_t), pool);
        int32_t arity = row.GetFieldCount();
        if (SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN) {
            arity = EndianSwapValue(arity);
        }
        memcpy(bytes->data(), &arity, sizeof(int32_t));
        MemorySegmentUtils::CopyToBytes({row.GetSegment()}, row.GetOffset(), bytes.get(),
                                        sizeof(int32_t), size_in_row);
        return bytes;
    }

    static Status SerializeBinaryRow(const BinaryRow& row, MemorySegmentOutputStream* out) {
        if (row.GetSizeInBytes() < 0) {
            return Status::Invalid(
                fmt::format("bytes size {} is less than 0", row.GetSizeInBytes()));
        }
        out->WriteValue<int32_t>(4 + row.GetSizeInBytes());
        out->WriteValue<int32_t>(row.GetFieldCount());
        return MemorySegmentUtils::CopyToStream({row.GetSegment()}, row.GetOffset(),
                                                row.GetSizeInBytes(), out);
    }

    /// Schemaless deserialization for `BinaryRow`.
    static Result<BinaryRow> DeserializeBinaryRow(const std::shared_ptr<Bytes>& bytes) {
        if (PAIMON_UNLIKELY(bytes->size() < 4)) {
            return Status::Invalid(fmt::format("bytes size {} is less than 4", bytes->size()));
        }
        int32_t arity = *(reinterpret_cast<int32_t*>(bytes->data()));
        if (SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN) {
            arity = EndianSwapValue(arity);
        }
        if (PAIMON_UNLIKELY(arity < 0)) {
            return Status::Invalid("arity is less than 0");
        }
        BinaryRow row(arity);
        row.PointTo(MemorySegment::Wrap(bytes), 4, bytes->size() - 4);
        return row;
    }

    /// Schemaless deserialization for `BinaryRow` from a `DataInputStream`.
    static Result<BinaryRow> DeserializeBinaryRow(DataInputStream* input, MemoryPool* pool) {
        int32_t read_length = -1;
        PAIMON_ASSIGN_OR_RAISE(read_length, input->ReadValue<int32_t>());
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(read_length, pool);
        PAIMON_RETURN_NOT_OK(input->ReadBytes(bytes.get()));
        return DeserializeBinaryRow(bytes);
    }
};

}  // namespace paimon
