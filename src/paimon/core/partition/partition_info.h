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
#include <vector>

#include "arrow/type.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/casting/cast_executor.h"

namespace paimon {
// ExistFieldInfo = PartitionInfo + NonPartitionInfo
// ExistFieldInfo equals the intersection of data scheme and read schema
// ExistFieldInfo + NonExistFieldInfo = read schema
struct PartitionInfo {
    // the intersection of partition schema & read schema
    std::vector<DataField> partition_read_schema;

    // indicates the idx in read schema (not data schema)
    std::vector<int32_t> idx_in_target_read_schema;

    // indicates the idx in partition schema (in BinaryRow)
    std::vector<int32_t> idx_in_partition;

    // partition predicate
    std::shared_ptr<Predicate> partition_filter;
};

struct NonPartitionInfo {
    // the intersection of non-partition schema and read schema
    std::vector<DataField> non_partition_read_schema;
    std::vector<DataField> non_partition_data_schema;

    // indicates the idx in read schema (not in data schema)
    std::vector<int32_t> idx_in_target_read_schema;

    // non-partition predicate
    std::shared_ptr<Predicate> non_partition_filter;
    std::vector<std::shared_ptr<CastExecutor>> cast_executors;
};

struct NonExistFieldInfo {
    // the fields in read schema but not in data schema
    std::vector<DataField> non_exist_read_schema;

    // indicates the idx in read schema (not data schema)
    std::vector<int32_t> idx_in_target_read_schema;
};

struct ExistFieldInfo {
    // the fields in both read schema and data schema
    std::vector<DataField> exist_read_schema;
    std::vector<DataField> exist_data_schema;

    // indicates the idx in read schema (not data schema)
    std::vector<int32_t> idx_in_target_read_schema;
};

}  // namespace paimon
