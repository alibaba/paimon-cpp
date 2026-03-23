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

#include "paimon/common/utils/binary_row_partition_computer.h"

#include <algorithm>
#include <cstddef>

#include "arrow/type.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/macros.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;

BinaryRowPartitionComputer::BinaryRowPartitionComputer(
    const std::vector<std::string>& partition_keys, const std::shared_ptr<arrow::Schema>& schema,
    const std::string& default_part_value,
    const std::vector<PartitionConverter>& partition_converters,
    const std::shared_ptr<MemoryPool>& memory_pool)
    : memory_pool_(memory_pool),
      partition_keys_(partition_keys),
      schema_(schema),
      default_part_value_(default_part_value),
      partition_converters_(partition_converters) {}

Result<std::unique_ptr<BinaryRowPartitionComputer>> BinaryRowPartitionComputer::Create(
    const std::vector<std::string>& partition_keys, const std::shared_ptr<arrow::Schema>& schema,
    const std::string& default_part_value, bool legacy_partition_name_enabled,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (PAIMON_UNLIKELY(schema == nullptr)) {
        return Status::Invalid(
            "create binary row partition computer failed, schema is null pointer.");
    }
    if (PAIMON_UNLIKELY(memory_pool == nullptr)) {
        return Status::Invalid(
            "create binary row partition computer failed, memory pool is null pointer.");
    }
    std::vector<PartitionConverter> partition_converters;
    for (const auto& partition_key : partition_keys) {
        PAIMON_ASSIGN_OR_RAISE(arrow::Type::type type_id,
                               GetTypeFromArrowSchema(schema, partition_key));
        PAIMON_ASSIGN_OR_RAISE(
            DataConverterUtils::StrToBinaryRowConverter converter,
            DataConverterUtils::CreateDataToBinaryRowConverter(type_id, memory_pool.get()));
        PAIMON_ASSIGN_OR_RAISE(DataConverterUtils::BinaryRowFieldToStrConverter reconverter,
                               DataConverterUtils::CreateBinaryRowFieldToStringConverter(
                                   type_id, legacy_partition_name_enabled));
        partition_converters.emplace_back(partition_key, std::move(converter),
                                          std::move(reconverter));
    }
    return std::unique_ptr<BinaryRowPartitionComputer>(new BinaryRowPartitionComputer(
        partition_keys, schema, default_part_value, partition_converters, memory_pool));
}

Result<BinaryRow> BinaryRowPartitionComputer::ToBinaryRow(
    const std::map<std::string, std::string>& partition) const {
    BinaryRow binary_row(partition_converters_.size());
    BinaryRowWriter writer(&binary_row, /*initial_size=*/0, memory_pool_.get());
    for (size_t field_idx = 0; field_idx < partition_converters_.size(); field_idx++) {
        const auto& partition_extractor = partition_converters_[field_idx];
        const auto& partition_key = partition_extractor.partition_key;
        const auto& to_binary_row = partition_extractor.converter;
        auto input_iter = partition.find(partition_key);
        if (input_iter == partition.end()) {
            return Status::Invalid(
                fmt::format("can not find partition key '{}' in input partition '{}'",
                            partition_key, partition));
        }
        const auto& value_str = input_iter->second;
        if (value_str == default_part_value_) {
            // TODO(yonghao.fyh): when support decimal/ timestamp in partition, use
            // WriteTimestamp(null) for non compact precision
            writer.SetNullAt(field_idx);
        } else {
            PAIMON_RETURN_NOT_OK(to_binary_row(value_str, field_idx, &writer));
        }
    }
    writer.Complete();
    return binary_row;
}

Result<std::vector<std::pair<std::string, std::string>>>
BinaryRowPartitionComputer::GeneratePartitionVector(const BinaryRow& partition) const {
    if (static_cast<size_t>(partition.GetFieldCount()) != partition_converters_.size()) {
        return Status::Invalid(fmt::format(
            "partition binary row field count {} not match with partition converter size {}",
            partition.GetFieldCount(), partition_converters_.size()));
    }
    std::vector<std::pair<std::string, std::string>> result;
    for (size_t field_idx = 0; field_idx < partition_converters_.size(); field_idx++) {
        const auto& partition_extractor = partition_converters_[field_idx];
        const auto& partition_key = partition_extractor.partition_key;
        const auto& to_str = partition_extractor.reconverter;
        if (partition.IsNullAt(field_idx)) {
            result.emplace_back(partition_key, default_part_value_);
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::string partition_field_str, to_str(partition, field_idx));
            if (StringUtils::IsNullOrWhitespaceOnly(partition_field_str)) {
                partition_field_str = default_part_value_;
            }
            result.emplace_back(partition_key, partition_field_str);
        }
    }
    return result;
}

Result<arrow::Type::type> BinaryRowPartitionComputer::GetTypeFromArrowSchema(
    const std::shared_ptr<arrow::Schema>& schema, const std::string& field_name) {
    auto field = schema->GetFieldByName(field_name);
    if (field == nullptr) {
        return Status::Invalid(
            fmt::format("field {} not in schema {}", field_name, schema->ToString()));
    }
    return field->type()->id();
}

Result<std::string> BinaryRowPartitionComputer::PartToSimpleString(
    const std::shared_ptr<arrow::Schema>& partition_type, const BinaryRow& partition,
    const std::string& delimiter, int32_t max_length) {
    std::vector<DataConverterUtils::BinaryRowFieldToStrConverter> partition_converters;
    partition_converters.reserve(partition_type->num_fields());
    for (const auto& field : partition_type->fields()) {
        PAIMON_ASSIGN_OR_RAISE(DataConverterUtils::BinaryRowFieldToStrConverter converter,
                               DataConverterUtils::CreateBinaryRowFieldToStringConverter(
                                   field->type()->id(), /*legacy_partition_name_enabled=*/true));
        partition_converters.emplace_back(converter);
    }
    std::vector<std::string> partition_vec;
    partition_vec.reserve(partition_converters.size());
    for (size_t field_idx = 0; field_idx < partition_converters.size(); field_idx++) {
        const auto& to_str = partition_converters[field_idx];
        if (partition.IsNullAt(field_idx)) {
            partition_vec.push_back("null");
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::string partition_field_str, to_str(partition, field_idx));
            partition_vec.push_back(partition_field_str);
        }
    }
    return fmt::format("{}", fmt::join(partition_vec, delimiter)).substr(0, max_length);
}
}  // namespace paimon
