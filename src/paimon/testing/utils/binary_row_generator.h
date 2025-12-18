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
#include <string>
#include <variant>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/stats/simple_stats.h"

namespace paimon::test {

struct TimestampType {
    TimestampType(const Timestamp& _timestamp, int32_t _precision)
        : timestamp(_timestamp), precision(_precision) {}

    Timestamp timestamp = Timestamp(0, 0);
    int32_t precision = Timestamp::DEFAULT_PRECISION;
};

class BinaryRowGenerator {
 public:
    BinaryRowGenerator() = delete;
    ~BinaryRowGenerator() = delete;

    // monostate indicates null
    using ValueType = std::vector<
        std::variant<bool, int8_t, int16_t, int32_t, int64_t, float, double, std::string,
                     std::shared_ptr<Bytes>, TimestampType, Decimal, NullType>>;

    static std::unique_ptr<InternalRow> GenerateRowPtr(const RowKind* kind, const ValueType& values,
                                                       MemoryPool* pool) {
        auto row = std::make_unique<BinaryRow>(values.size());
        BinaryRowWriter writer(row.get(), 0, pool);
        for (size_t i = 0; i < values.size(); i++) {
            std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, std::monostate>) {
                        // @note for timestamp and decimal, if precision is non-compact, the
                        // hash of returned row maybe diffenent with java
                        writer.SetNullAt(i);
                    } else if constexpr (std::is_same_v<T, int8_t>) {
                        writer.WriteByte(i, arg);
                    } else if constexpr (std::is_same_v<T, int16_t>) {
                        writer.WriteShort(i, arg);
                    } else if constexpr (std::is_same_v<T, int32_t>) {
                        writer.WriteInt(i, arg);
                    } else if constexpr (std::is_same_v<T, int64_t>) {
                        writer.WriteLong(i, arg);
                    } else if constexpr (std::is_same_v<T, float>) {
                        writer.WriteFloat(i, arg);
                    } else if constexpr (std::is_same_v<T, double>) {
                        writer.WriteDouble(i, arg);
                    } else if constexpr (std::is_same_v<T, bool>) {
                        writer.WriteBoolean(i, arg);
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        writer.WriteString(i, BinaryString::FromString(arg, pool));
                    } else if constexpr (std::is_same_v<T, std::shared_ptr<Bytes>>) {
                        writer.WriteBinary(i, *arg);
                    } else if constexpr (std::is_same_v<T, TimestampType>) {
                        writer.WriteTimestamp(i, arg.timestamp, arg.precision);
                    } else if constexpr (std::is_same_v<T, Decimal>) {
                        writer.WriteDecimal(i, arg, arg.Precision());
                    } else {
                        EXPECT_FALSE(true);
                    }
                },
                values[i]);
        }
        writer.Complete();
        row->SetRowKind(kind);
        return row;
    }

    static std::unique_ptr<InternalRow> GenerateRowPtr(const ValueType& values, MemoryPool* pool) {
        return GenerateRowPtr(RowKind::Insert(), values, pool);
    }

    static BinaryRow GenerateRow(const RowKind* kind, const ValueType& values, MemoryPool* pool) {
        auto row = GenerateRowPtr(kind, values, pool);
        auto binary_row = dynamic_cast<BinaryRow*>(row.get());
        assert(binary_row);
        return *binary_row;
    }

    static BinaryRow GenerateRow(const ValueType& values, MemoryPool* pool) {
        return GenerateRow(RowKind::Insert(), values, pool);
    }

    static SimpleStats GenerateStats(const ValueType& min, const ValueType& max,
                                     const std::vector<int64_t>& null, MemoryPool* pool) {
        auto min_row = GenerateRowPtr(min, pool);
        auto binary_min_row = dynamic_cast<BinaryRow*>(min_row.get());
        assert(binary_min_row);
        auto max_row = GenerateRowPtr(max, pool);
        auto binary_max_row = dynamic_cast<BinaryRow*>(max_row.get());
        assert(binary_max_row);
        return SimpleStats(*binary_min_row, *binary_max_row, FromLongArrayWithNull(null, pool));
    }

    static BinaryArray FromLongArrayWithNull(const std::vector<int64_t>& arr, MemoryPool* pool) {
        BinaryArray array;
        BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), sizeof(int64_t), pool);
        for (size_t i = 0; i < arr.size(); i++) {
            int64_t v = arr[i];
            if (v == -1) {
                writer.SetNullValue<int64_t>(i);
            } else {
                writer.WriteLong(i, v);
            }
        }
        writer.Complete();
        return array;
    }
};
}  // namespace paimon::test
