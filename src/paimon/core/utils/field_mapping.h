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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/casting/cast_executor.h"
#include "paimon/core/partition/partition_info.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"

namespace paimon {
struct FieldMapping {
    FieldMapping(const std::optional<PartitionInfo>& _partition_info,
                 const NonPartitionInfo& _non_partition_info,
                 const std::optional<NonExistFieldInfo>& _non_exist_field_info)
        : partition_info(_partition_info),
          non_partition_info(_non_partition_info),
          non_exist_field_info(_non_exist_field_info) {}

    static Result<std::shared_ptr<arrow::Schema>> GetPartitionSchema(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::string>& partition_keys);

    std::optional<PartitionInfo> partition_info = std::nullopt;
    NonPartitionInfo non_partition_info;
    std::optional<NonExistFieldInfo> non_exist_field_info = std::nullopt;
};
class LeafPredicateImpl;

class FieldMappingBuilder {
 public:
    static Result<std::unique_ptr<FieldMappingBuilder>> Create(
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::vector<std::string>& partition_keys,
        const std::shared_ptr<Predicate>& predicate);

    Result<std::unique_ptr<FieldMapping>> CreateFieldMapping(
        const std::vector<DataField>& data_fields) const;
    Result<std::unique_ptr<FieldMapping>> CreateFieldMapping(
        const std::shared_ptr<arrow::Schema>& data_schema) const;

    int32_t GetReadFieldCount() const {
        return read_fields_.size();
    }

    static Result<std::vector<std::shared_ptr<CastExecutor>>> CreateDataCastExecutors(
        const std::vector<DataField>& read_fields, const std::vector<DataField>& data_fields);

    static Result<std::optional<std::shared_ptr<Predicate>>> ReconstructPredicateWithDataFields(
        const std::shared_ptr<Predicate>& predicate,
        const std::map<std::string, DataField>& field_name_to_read_fields,
        const std::map<int32_t, std::pair<int32_t, DataField>>& field_id_to_data_fields);

 private:
    FieldMappingBuilder(const std::vector<DataField>& read_fields,
                        const std::vector<std::string>& partition_keys,
                        const std::shared_ptr<Predicate>& predicate)
        : read_fields_(read_fields), partition_keys_(partition_keys), predicate_(predicate) {}

    std::optional<NonExistFieldInfo> CreateNonExistFieldInfo(
        const std::vector<DataField>& data_fields) const;
    ExistFieldInfo CreateExistFieldInfo(const std::vector<DataField>& data_fields) const;

    Result<NonPartitionInfo> CreateNonPartitionInfo(
        const std::vector<DataField>& data_fields, const ExistFieldInfo& exist_field_info,
        const std::map<std::string, int32_t>& partition_keys) const;
    Result<std::shared_ptr<Predicate>> CreateDataFilters(
        const std::vector<DataField>& data_fields) const;

    static Result<std::optional<std::shared_ptr<Predicate>>> ReconstructLeafPredicate(
        const std::shared_ptr<LeafPredicateImpl>& leaf_predicate,
        const std::map<std::string, DataField>& field_name_to_read_fields,
        const std::map<int32_t, std::pair<int32_t, DataField>>& field_id_to_data_fields);

    Result<std::optional<PartitionInfo>> CreatePartitionInfo(
        const ExistFieldInfo& exist_field_info,
        const std::map<std::string, int32_t>& partition_keys) const;

 private:
    std::vector<DataField> read_fields_;
    std::vector<std::string> partition_keys_;
    std::shared_ptr<Predicate> predicate_;
};

}  // namespace paimon
