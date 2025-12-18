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

#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/data/serializer/binary_row_serializer.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/io/data_input_stream.h"

struct ArrowArray;

namespace paimon {

/// A serializer to serialize object by `BinaryRowSerializer`.
template <typename T>
class ObjectSerializer {
 public:
    ObjectSerializer(const std::shared_ptr<arrow::DataType>& data_type,
                     const std::shared_ptr<MemoryPool>& pool)
        : pool_(pool), data_type_(data_type), row_serializer_(data_type_->num_fields(), pool) {}

    virtual ~ObjectSerializer() = default;

    /// Convert a `T` to `BinaryRow`.
    virtual Result<BinaryRow> ToRow(const T& record) const = 0;

    /// Convert an `InternalRow` to `T`.
    virtual Result<T> FromRow(const InternalRow& row_data) const = 0;

    /// Get the number of fields.
    int32_t NumFields() const {
        return data_type_->num_fields();
    }

    const std::shared_ptr<arrow::DataType>& GetDataType() const {
        return data_type_;
    }
    /// Serializes the given record to the given target output stream.
    ///
    /// @param record The record to serialize.
    /// @param target The output stream to write the serialized data to.
    /// @return status returned, if the serialization encountered an I/O related error. Typically
    /// raised by the output stream, which may have an underlying I/O channel to which it
    /// delegates.
    Status Serialize(const T& record, MemorySegmentOutputStream* target) const {
        PAIMON_ASSIGN_OR_RAISE(BinaryRow row, ToRow(record));
        return row_serializer_.Serialize(row, target);
    }

    /// De-serializes a record from the given source input stream.
    ///
    /// @param source The input stream from which to read the data.
    /// @return The deserialized element.
    Result<T> Deserialize(DataInputStream* source) const {
        PAIMON_ASSIGN_OR_RAISE(BinaryRow row, row_serializer_.Deserialize(source));
        return FromRow(row);
    }

    /// Serializes the given record list to the given target output stream.
    Status SerializeList(const std::vector<T>& records, MemorySegmentOutputStream* target) {
        target->WriteValue<int32_t>(records.size());
        for (const T& record : records) {
            PAIMON_RETURN_NOT_OK(Serialize(record, target));
        }
        return Status::OK();
    }

    /// De-serializes a record list from the given source input view.
    Result<std::vector<T>> DeserializeList(DataInputStream* source) const {
        int32_t size = 0;
        PAIMON_ASSIGN_OR_RAISE(size, source->ReadValue<int32_t>());
        std::vector<T> records;
        records.reserve(size);
        for (int32_t i = 0; i < size; i++) {
            PAIMON_ASSIGN_OR_RAISE(T meta, Deserialize(source));
            records.emplace_back(std::move(meta));
        }
        return records;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::DataType> data_type_;
    BinaryRowSerializer row_serializer_;
};

}  // namespace paimon
