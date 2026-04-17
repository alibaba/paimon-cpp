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

#include "paimon/core/bucket/hive_bucket_function.h"

#include <cassert>
#include <cmath>
#include <cstring>
#include <limits>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/bucket/hive_hasher.h"
#include "paimon/status.h"

namespace paimon {
HiveBucketFunction::HiveBucketFunction(const std::vector<HiveFieldInfo>& field_infos)
    : field_infos_(field_infos) {}

Result<std::unique_ptr<HiveBucketFunction>> HiveBucketFunction::Create(
    const std::vector<FieldType>& field_types) {
    std::vector<HiveFieldInfo> field_infos;
    field_infos.reserve(field_types.size());
    for (const auto& type : field_types) {
        field_infos.emplace_back(type);
    }
    return Create(field_infos);
}

Result<std::unique_ptr<HiveBucketFunction>> HiveBucketFunction::Create(
    const std::vector<HiveFieldInfo>& field_infos) {
    if (field_infos.empty()) {
        return Status::Invalid("HiveBucketFunction requires at least one field");
    }
    for (const auto& info : field_infos) {
        switch (info.type) {
            case FieldType::BOOLEAN:
            case FieldType::TINYINT:
            case FieldType::SMALLINT:
            case FieldType::INT:
            case FieldType::BIGINT:
            case FieldType::FLOAT:
            case FieldType::DOUBLE:
            case FieldType::STRING:
            case FieldType::BINARY:
            case FieldType::DECIMAL:
            case FieldType::DATE:
                break;
            default:
                return Status::Invalid(fmt::format("Unsupported type as Hive bucket key type: {}",
                                                   FieldTypeUtils::FieldTypeToString(info.type)));
        }
    }
    return std::unique_ptr<HiveBucketFunction>(new HiveBucketFunction(field_infos));
}

int32_t HiveBucketFunction::Bucket(const BinaryRow& row, int32_t num_buckets) const {
    static constexpr int32_t SEED = 0;
    int32_t hash = SEED;
    for (int32_t i = 0; i < row.GetFieldCount(); i++) {
        hash = (31 * hash) + ComputeHash(row, i);
    }
    return Mod(hash & std::numeric_limits<int32_t>::max(), num_buckets);
}

int32_t HiveBucketFunction::ComputeHash(const BinaryRow& row, int32_t field_index) const {
    if (row.IsNullAt(field_index)) {
        return 0;
    }

    const auto& info = field_infos_[field_index];
    switch (info.type) {
        case FieldType::BOOLEAN:
            return HiveHasher::HashInt(row.GetBoolean(field_index) ? 1 : 0);
        case FieldType::TINYINT:
            return HiveHasher::HashInt(static_cast<int32_t>(row.GetByte(field_index)));
        case FieldType::SMALLINT:
            return HiveHasher::HashInt(static_cast<int32_t>(row.GetShort(field_index)));
        case FieldType::INT:
        case FieldType::DATE:
            return HiveHasher::HashInt(row.GetInt(field_index));
        case FieldType::BIGINT:
            return HiveHasher::HashLong(row.GetLong(field_index));
        case FieldType::FLOAT: {
            float float_value = row.GetFloat(field_index);
            int32_t bits;
            if (float_value == -0.0f) {
                bits = 0;
            } else {
                std::memcpy(&bits, &float_value, sizeof(bits));
            }
            return HiveHasher::HashInt(bits);
        }
        case FieldType::DOUBLE: {
            double double_value = row.GetDouble(field_index);
            int64_t bits;
            if (double_value == -0.0) {
                bits = 0L;
            } else {
                std::memcpy(&bits, &double_value, sizeof(bits));
            }
            return HiveHasher::HashLong(bits);
        }
        case FieldType::STRING: {
            std::string_view sv = row.GetStringView(field_index);
            return HiveHasher::HashBytes(sv.data(), static_cast<int32_t>(sv.size()));
        }
        case FieldType::BINARY: {
            std::string_view sv = row.GetStringView(field_index);
            return HiveHasher::HashBytes(sv.data(), static_cast<int32_t>(sv.size()));
        }
        case FieldType::DECIMAL: {
            Decimal decimal = row.GetDecimal(field_index, info.precision, info.scale);
            return HiveHasher::HashDecimal(decimal);
        }
        default:
            // This should never happen since Create() validates the types.
            assert(false);
            return 0;
    }
}

int32_t HiveBucketFunction::Mod(int32_t value, int32_t divisor) {
    int32_t remainder = value % divisor;
    if (remainder < 0) {
        return (remainder + divisor) % divisor;
    }
    return remainder;
}

}  // namespace paimon
