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

#include <memory>

#include "arrow/type_fwd.h"
#include "paimon/common/data/data_define.h"
#include "paimon/result.h"

namespace paimon {

class Bytes;
class DataGetters;
class MemoryPool;

/// Helpers for extracting and comparing field aggregation values.
class FieldAggregateUtils {
 public:
    FieldAggregateUtils() = delete;
    ~FieldAggregateUtils() = delete;

    /// Return a binary value which owns its buffer. A merged row hands out binary fields as views
    /// into buffers it releases once the field is overwritten.
    ///
    /// @param value Binary value, either owning or a view.
    /// @param pool Pool the copy is allocated from when one is needed.
    /// @return The value itself when it already owns its buffer, otherwise an owning copy.
    static VariantType OwnedBinary(const VariantType& value, MemoryPool* pool);

    /// Extract a value from a typed field.
    ///
    /// @param getters Source containing the field.
    /// @param pos Field position in the source.
    /// @param type Logical type of the field.
    /// @return The extracted value, or an error Status.
    static Result<VariantType> GetValue(const DataGetters& getters, int32_t pos,
                                        const std::shared_ptr<arrow::DataType>& type);

    /// Compare two values using their logical type.
    ///
    /// @param lhs Left value.
    /// @param rhs Right value.
    /// @param type Logical type of both values.
    /// @return Whether the values are equal, or an error Status.
    static Result<bool> Equals(const VariantType& lhs, const VariantType& rhs,
                               const std::shared_ptr<arrow::DataType>& type);
};

}  // namespace paimon
