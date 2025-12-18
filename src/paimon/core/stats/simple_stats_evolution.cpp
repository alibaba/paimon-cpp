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

#include "paimon/core/stats/simple_stats_evolution.h"

#include <cassert>
#include <cstddef>
#include <string_view>

#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/projected_array.h"
#include "paimon/common/utils/projected_row.h"
#include "paimon/core/casting/casted_row.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/status.h"

namespace paimon {
class Bytes;
class CastExecutor;
class InternalMap;
class InternalRow;
class MemoryPool;

SimpleStatsEvolution::SimpleStatsEvolution(const std::vector<DataField>& data_fields,
                                           const std::vector<DataField>& table_fields,
                                           bool need_mapping,
                                           const std::shared_ptr<MemoryPool>& pool)
    : need_mapping_(need_mapping), data_fields_(data_fields), table_fields_(table_fields) {
    empty_values_ = std::make_shared<GenericRow>(data_fields.size());
    empty_null_counts_ = std::make_shared<BinaryArray>();
    BinaryArrayWriter array_writer(empty_null_counts_.get(), data_fields.size(),
                                   /*element_size=*/sizeof(int64_t), pool.get());
    for (size_t i = 0; i < data_fields.size(); ++i) {
        array_writer.SetNullAt(i);
    }
    array_writer.Complete();

    for (size_t i = 0; i < data_fields.size(); i++) {
        const auto& data_field = data_fields[i];
        id_to_data_fields_.emplace(data_field.Id(), std::make_pair(i, data_field));
    }
    std::map<std::string, DataField> name_to_table_fields;
    for (const auto& field : table_fields) {
        name_to_table_fields_.emplace(field.Name(), field);
    }
}

class NullCountsEvoArray : public InternalArray {
 public:
    NullCountsEvoArray(const std::shared_ptr<InternalArray>& array,
                       const std::vector<int32_t>& mapping, int64_t not_found_value)
        : array_(array), mapping_(mapping), not_found_value_(not_found_value) {
        assert(array_);
    }
    int32_t Size() const override {
        return mapping_.size();
    }
    bool IsNullAt(int32_t pos) const override {
        assert(static_cast<size_t>(pos) < mapping_.size());
        if (mapping_[pos] < 0) {
            return false;
        }
        return array_->IsNullAt(mapping_[pos]);
    }
    int64_t GetLong(int32_t pos) const override {
        if (mapping_[pos] < 0) {
            return not_found_value_;
        }
        return array_->GetLong(mapping_[pos]);
    }

 private:
    // ============================= Unsupported Methods ================================
    bool GetBoolean(int32_t pos) const override;
    char GetByte(int32_t pos) const override;
    int16_t GetShort(int32_t pos) const override;
    int32_t GetInt(int32_t pos) const override;
    int32_t GetDate(int32_t pos) const override;
    float GetFloat(int32_t pos) const override;
    double GetDouble(int32_t pos) const override;
    BinaryString GetString(int32_t pos) const override;
    std::string_view GetStringView(int32_t pos) const override;
    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override;
    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override;
    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override;
    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override;
    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override;
    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override;
    Result<std::vector<char>> ToBooleanArray() const override;
    Result<std::vector<char>> ToByteArray() const override;
    Result<std::vector<int16_t>> ToShortArray() const override;
    Result<std::vector<int32_t>> ToIntArray() const override;
    Result<std::vector<int64_t>> ToLongArray() const override;
    Result<std::vector<float>> ToFloatArray() const override;
    Result<std::vector<double>> ToDoubleArray() const override;

 private:
    std::shared_ptr<InternalArray> array_;
    std::vector<int32_t> mapping_;
    int64_t not_found_value_;
};

Result<SimpleStatsEvolution::EvolutionStats> SimpleStatsEvolution::Evolution(
    const SimpleStats& stats, int64_t row_count,
    const std::optional<std::vector<std::string>>& dense_fields) {
    SimpleStatsEvolution::EvolutionStats evolution_stats(
        std::make_shared<BinaryRow>(stats.MinValues()),
        std::make_shared<BinaryRow>(stats.MaxValues()),
        std::make_shared<BinaryArray>(stats.NullCounts()));
    auto& min_values = evolution_stats.min_values;
    auto& max_values = evolution_stats.max_values;
    auto& null_counts = evolution_stats.null_counts;
    if (dense_fields != std::nullopt && dense_fields.value().empty()) {
        // optimize for empty dense fields
        min_values = empty_values_;
        max_values = empty_values_;
        null_counts = empty_null_counts_;
    } else if (dense_fields != std::nullopt) {
        // create dense index mapping
        std::vector<int32_t> data_idx_to_dense_idx;
        std::optional<std::vector<int32_t>> cached_dense_idx =
            dense_fields_mapping_.Find(dense_fields.value());
        if (!cached_dense_idx) {
            std::map<std::string, int32_t> field_name_to_dense_idx =
                ObjectUtils::CreateIdentifierToIndexMap(dense_fields.value());
            data_idx_to_dense_idx.resize(data_fields_.size(), -1);
            for (size_t i = 0; i < data_fields_.size(); ++i) {
                auto iter = field_name_to_dense_idx.find(data_fields_[i].Name());
                if (iter != field_name_to_dense_idx.end()) {
                    data_idx_to_dense_idx[i] = iter->second;
                }
            }
            dense_fields_mapping_.Insert(dense_fields.value(), data_idx_to_dense_idx);
        } else {
            data_idx_to_dense_idx = std::move(cached_dense_idx).value();
        }

        min_values = std::make_shared<ProjectedRow>(min_values, data_idx_to_dense_idx);
        max_values = std::make_shared<ProjectedRow>(max_values, data_idx_to_dense_idx);
        null_counts = std::make_shared<ProjectedArray>(null_counts, data_idx_to_dense_idx);
    }

    if (!need_mapping_) {
        return evolution_stats;
    }
    return MappingStats(row_count, evolution_stats);
}

Result<SimpleStatsEvolution::EvolutionStats> SimpleStatsEvolution::MappingStats(
    int64_t row_count, const SimpleStatsEvolution::EvolutionStats& src_stats) const {
    SimpleStatsEvolution::EvolutionStats evolution_stats = src_stats;
    auto& min_values = evolution_stats.min_values;
    auto& max_values = evolution_stats.max_values;
    auto& null_counts = evolution_stats.null_counts;

    std::vector<int32_t> table_idx_to_data_idx(table_fields_.size(), -1);
    std::vector<DataField> projected_data_fields;
    projected_data_fields.reserve(table_fields_.size());
    std::map<int32_t, int32_t> field_id_to_data_idx = ObjectUtils::CreateIdentifierToIndexMap(
        data_fields_, [](const DataField& field) -> int32_t { return field.Id(); });
    for (size_t i = 0; i < table_fields_.size(); ++i) {
        auto iter = field_id_to_data_idx.find(table_fields_[i].Id());
        if (iter != field_id_to_data_idx.end()) {
            table_idx_to_data_idx[i] = iter->second;
            projected_data_fields.push_back(data_fields_[iter->second]);
        } else {
            projected_data_fields.push_back(table_fields_[i]);
        }
    }
    min_values = std::make_shared<ProjectedRow>(min_values, table_idx_to_data_idx);
    max_values = std::make_shared<ProjectedRow>(max_values, table_idx_to_data_idx);
    null_counts =
        std::make_shared<NullCountsEvoArray>(null_counts, table_idx_to_data_idx, row_count);

    // cast mapping
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::shared_ptr<CastExecutor>> cast_executors,
        FieldMappingBuilder::CreateDataCastExecutors(table_fields_, projected_data_fields));

    bool need_cast = false;
    for (const auto& executor : cast_executors) {
        if (executor) {
            need_cast = true;
            break;
        }
    }
    if (need_cast) {
        PAIMON_ASSIGN_OR_RAISE(min_values, CastedRow::Create(cast_executors, projected_data_fields,
                                                             table_fields_, min_values));
        PAIMON_ASSIGN_OR_RAISE(max_values, CastedRow::Create(cast_executors, projected_data_fields,
                                                             table_fields_, max_values));
    }
    return evolution_stats;
}

// ============================= Unsupported Methods ================================
bool NullCountsEvoArray::GetBoolean(int32_t pos) const {
    assert(false);
    return array_->GetBoolean(mapping_[pos]);
}

char NullCountsEvoArray::GetByte(int32_t pos) const {
    assert(false);
    return array_->GetByte(mapping_[pos]);
}

int16_t NullCountsEvoArray::GetShort(int32_t pos) const {
    assert(false);
    return array_->GetShort(mapping_[pos]);
}

int32_t NullCountsEvoArray::GetInt(int32_t pos) const {
    assert(false);
    return array_->GetInt(mapping_[pos]);
}

int32_t NullCountsEvoArray::GetDate(int32_t pos) const {
    return NullCountsEvoArray::GetInt(pos);
}

float NullCountsEvoArray::GetFloat(int32_t pos) const {
    assert(false);
    return array_->GetFloat(mapping_[pos]);
}

double NullCountsEvoArray::GetDouble(int32_t pos) const {
    assert(false);
    return array_->GetDouble(mapping_[pos]);
}

BinaryString NullCountsEvoArray::GetString(int32_t pos) const {
    assert(false);
    return array_->GetString(mapping_[pos]);
}

std::string_view NullCountsEvoArray::GetStringView(int32_t pos) const {
    assert(false);
    return array_->GetStringView(mapping_[pos]);
}

Decimal NullCountsEvoArray::GetDecimal(int32_t pos, int32_t precision, int32_t scale) const {
    assert(false);
    return array_->GetDecimal(mapping_[pos], precision, scale);
}

Timestamp NullCountsEvoArray::GetTimestamp(int32_t pos, int32_t precision) const {
    assert(false);
    return array_->GetTimestamp(mapping_[pos], precision);
}

std::shared_ptr<Bytes> NullCountsEvoArray::GetBinary(int32_t pos) const {
    assert(false);
    return array_->GetBinary(mapping_[pos]);
}

std::shared_ptr<InternalArray> NullCountsEvoArray::GetArray(int32_t pos) const {
    assert(false);
    return array_->GetArray(mapping_[pos]);
}

std::shared_ptr<InternalMap> NullCountsEvoArray::GetMap(int32_t pos) const {
    assert(false);
    return array_->GetMap(mapping_[pos]);
}

std::shared_ptr<InternalRow> NullCountsEvoArray::GetRow(int32_t pos, int32_t num_fields) const {
    assert(false);
    return array_->GetRow(mapping_[pos], num_fields);
}

Result<std::vector<char>> NullCountsEvoArray::ToBooleanArray() const {
    return Status::Invalid("NullCountsEvoArray do not support convert to boolean array");
}
Result<std::vector<char>> NullCountsEvoArray::ToByteArray() const {
    return Status::Invalid("NullCountsEvoArray do not support convert to byte array");
}
Result<std::vector<int16_t>> NullCountsEvoArray::ToShortArray() const {
    return Status::Invalid("NullCountsEvoArray do not support convert to short array");
}
Result<std::vector<int32_t>> NullCountsEvoArray::ToIntArray() const {
    return Status::Invalid("NullCountsEvoArray do not support convert to int array");
}
Result<std::vector<int64_t>> NullCountsEvoArray::ToLongArray() const {
    return Status::Invalid("NullCountsEvoArray do not support convert to long array");
}
Result<std::vector<float>> NullCountsEvoArray::ToFloatArray() const {
    return Status::Invalid("NullCountsEvoArray do not support convert to float array");
}
Result<std::vector<double>> NullCountsEvoArray::ToDoubleArray() const {
    return Status::Invalid("NullCountsEvoArray do not support convert to double array");
}

}  // namespace paimon
