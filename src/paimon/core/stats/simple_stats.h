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
#include <sstream>
#include <string>

#include "arrow/api.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/result.h"

namespace arrow {
class ArrayBuilder;
}  // namespace arrow

namespace paimon {
class InternalRow;
class MemoryPool;

/// The statistics for columns, supports the following stats.
///
/// <ul>
/// <li>min_values: the minimum values of the columns
/// <li>max_values: the maximum values of the columns
/// <li>null_counts: the number of nulls of the columns
/// </ul>
///
/// All statistics are stored in the form of a Binary, which can significantly reduce its memory
/// consumption, but the cost is that the column type needs to be known when getting.
class SimpleStats {
 public:
    SimpleStats(const BinaryRow& min_values, const BinaryRow& max_values,
                const BinaryArray& null_counts)
        : min_values_(min_values), max_values_(max_values), null_counts_(null_counts) {}

    /// Empty stats for 0 column number.
    static const SimpleStats& EmptyStats();

    const BinaryRow& MinValues() const {
        return min_values_;
    }

    const BinaryRow& MaxValues() const {
        return max_values_;
    }

    const BinaryArray& NullCounts() const {
        return null_counts_;
    }

    BinaryRow ToRow() const;

    static Result<SimpleStats> FromRow(const InternalRow* row, MemoryPool* pool);

    std::string ToString() const {
        std::stringstream ss;
        ss << std::hex << static_cast<uint32_t>(HashCode());
        return "SimpleStats@" + ss.str();
    }

    int32_t HashCode() const;

    bool operator==(const SimpleStats& other) const;

    static const std::shared_ptr<arrow::DataType>& DataType();

 private:
    BinaryRow min_values_;
    BinaryRow max_values_;
    BinaryArray null_counts_;
};
}  // namespace paimon
