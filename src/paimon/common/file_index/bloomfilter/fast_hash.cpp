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

#include "paimon/common/file_index/bloomfilter/fast_hash.h"

#include <cassert>
#include <cstring>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/status.h"
#include "xxhash.h"  // NOLINT(build/include_subdir)

namespace paimon {
Result<FastHash::HashFunction> FastHash::GetHashFunction(
    const std::shared_ptr<arrow::DataType>& arrow_type) {
    PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                           FieldTypeUtils::ConvertToFieldType(arrow_type->id()));
    switch (field_type) {
        case FieldType::TINYINT:
            return HashFunction([](const Literal& literal) -> int64_t {
                return GetLongHash(static_cast<int64_t>(literal.GetValue<int8_t>()));
            });
        case FieldType::SMALLINT:
            return HashFunction([](const Literal& literal) -> int64_t {
                return GetLongHash(static_cast<int64_t>(literal.GetValue<int16_t>()));
            });
        case FieldType::DATE:
        case FieldType::INT:
            return HashFunction([](const Literal& literal) -> int64_t {
                return GetLongHash(static_cast<int64_t>(literal.GetValue<int32_t>()));
            });
        case FieldType::BIGINT:
            return HashFunction([](const Literal& literal) -> int64_t {
                return GetLongHash(static_cast<int64_t>(literal.GetValue<int64_t>()));
            });
        case FieldType::FLOAT:
            return HashFunction([](const Literal& literal) -> int64_t {
                auto raw_value = literal.GetValue<float>();
                int32_t bits = 0;
                std::memcpy(&bits, &raw_value, sizeof(raw_value));
                return GetLongHash(bits);
            });
        case FieldType::DOUBLE:
            return HashFunction([](const Literal& literal) -> int64_t {
                auto raw_value = literal.GetValue<double>();
                int64_t bits;
                std::memcpy(&bits, &raw_value, sizeof(raw_value));
                return GetLongHash(bits);
            });
        case FieldType::TIMESTAMP: {
            auto ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(arrow_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(ts_type);
            assert(precision >= 0);
            return HashFunction([precision](const Literal& literal) -> int64_t {
                int64_t value = 0;
                if (precision <= Timestamp::MILLIS_PRECISION) {
                    value = literal.GetValue<Timestamp>().GetMillisecond();
                } else {
                    value = literal.GetValue<Timestamp>().ToMicrosecond();
                }
                return GetLongHash(value);
            });
        }
        case FieldType::STRING:
        case FieldType::BINARY:
            return HashFunction([](const Literal& literal) -> int64_t {
                auto value = literal.GetValue<std::string>();
                return Hash64(value.data(), value.size());
            });
        default:
            return Status::Invalid(fmt::format("bloom filter index does not support {}",
                                               FieldTypeUtils::FieldTypeToString(field_type)));
    }
}

int64_t FastHash::GetLongHash(int64_t key) {
    key = (~key) + (key << 21);  // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8);  // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4);  // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
}

int64_t FastHash::Hash64(const char* data, size_t length) {
    return XXH64(data, length, /*seed=*/0);
}

}  // namespace paimon
