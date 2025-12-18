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

#include "paimon/core/utils/object_serializer.h"
#include "paimon/core/utils/offset_row.h"

namespace paimon {
/// An `ObjectSerializer` for versioned serialization.
template <typename T>
class PAIMON_EXPORT VersionedObjectSerializer : public ObjectSerializer<T> {
 public:
    explicit VersionedObjectSerializer(const std::shared_ptr<MemoryPool>& pool)
        : ObjectSerializer<T>(VersionType(T::DataType()), pool) {}

    static std::shared_ptr<arrow::DataType> VersionType(
        const std::shared_ptr<arrow::DataType>& data_type) {
        auto struct_type = arrow::internal::checked_pointer_cast<arrow::StructType>(data_type);
        if (!struct_type) {
            assert(false);
            return nullptr;
        }
        return struct_type
            ->AddField(0, arrow::field("_VERSION", arrow::int32(), /*nullable=*/false))
            .ValueOr(nullptr);
    }

    /// Gets the version with which this serializer serializes.
    ///
    /// @return The version of the serialization schema.
    virtual int32_t GetVersion() const = 0;

    virtual Result<T> ConvertFrom(int32_t version, const InternalRow& row) const = 0;

    Result<T> FromRow(const InternalRow& row) const override {
        int32_t version = row.GetInt(0);
        OffsetRow offset_row(row, /*arity=*/row.GetFieldCount() - 1, /*offset=*/1);
        return ConvertFrom(version, offset_row);
    }

    Result<BinaryRow> ToRow(const T& record) const override {
        return Status::NotImplemented("ToRow for VersionedObjectSerializer is not implemented");
    }
};
}  // namespace paimon
