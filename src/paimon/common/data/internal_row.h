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
#include <functional>
#include <memory>
#include <string>

#include "paimon/common/data/data_define.h"
#include "paimon/common/data/data_getters.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
/// Base interface for an internal data structure representing data of `RowType`.
/// The mappings from SQL data types to the internal data structures are listed in the
/// following table:
/// +--------------------------------+-----------------------------------------+
/// | SQL Data Types                 | Internal Data Structures                |
/// +--------------------------------+-----------------------------------------+
/// | BOOLEAN                        | boolean                                 |
/// +--------------------------------+-----------------------------------------+
/// | CHAR / VARCHAR / STRING        | `BinaryString`                    |
/// +--------------------------------+-----------------------------------------+
/// | BINARY / VARBINARY / BYTES     | byte[]                                  |
/// +--------------------------------+-----------------------------------------+
/// | DECIMAL                        | `Decimal`                         |
/// +--------------------------------+-----------------------------------------+
/// | TINYINT                        | byte                                    |
/// +--------------------------------+-----------------------------------------+
/// | SMALLINT                       | short                                   |
/// +--------------------------------+-----------------------------------------+
/// | INT                            | int                                     |
/// +--------------------------------+-----------------------------------------+
/// | BIGINT                         | long                                    |
/// +--------------------------------+-----------------------------------------+
/// | FLOAT                          | float                                   |
/// +--------------------------------+-----------------------------------------+
/// | DOUBLE                         | double                                  |
/// +--------------------------------+-----------------------------------------+
/// | DATE                           | int (number of days since epoch)        |
/// +--------------------------------+-----------------------------------------+
/// | TIME                           | int (number of milliseconds of the day) |
/// +--------------------------------+-----------------------------------------+
/// | TIMESTAMP                      | `Timestamp`                       |
/// +--------------------------------+-----------------------------------------+
/// | TIMESTAMP WITH LOCAL TIME ZONE | `Timestamp`                       |
/// +--------------------------------+-----------------------------------------+
/// | ROW                            | `InternalRow`                     |
/// +--------------------------------+-----------------------------------------+
/// | ARRAY                          | `InternalArray`                   |
/// +--------------------------------+-----------------------------------------+
/// | MAP / MULTISET                 | `InternalMap`                     |
/// +--------------------------------+-----------------------------------------+
/// Nullability is always handled by the container data structure.

class PAIMON_EXPORT InternalRow : public DataGetters {
 public:
    ~InternalRow() override = default;

    /// @return the number of fields in this row.
    /// The number does not include `RowKind`. It is kept separately.
    virtual int32_t GetFieldCount() const = 0;

    /// @return the kind of change that this row describes in a changelog.
    virtual Result<const RowKind*> GetRowKind() const = 0;

    /// Sets the kind of change that this row describes in a changelog.
    virtual void SetRowKind(const RowKind* kind) = 0;

    virtual std::string ToString() const = 0;

    using FieldGetterFunc = std::function<VariantType(const InternalRow&)>;

    static Result<FieldGetterFunc> CreateFieldGetter(
        int32_t field_idx, const std::shared_ptr<arrow::DataType>& field_type, bool use_view);
};

}  // namespace paimon
