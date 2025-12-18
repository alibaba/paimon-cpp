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
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/utils/data_converter_utils.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace paimon {

class BinaryRow;
class MemoryPool;

// TODO(yonghao.fyh): rethink naming of converter/reconverter
struct PartitionConverter {
    PartitionConverter(const std::string& _partition_key,
                       typename DataConverterUtils::StrToBinaryRowConverter&& _converter,
                       typename DataConverterUtils::BinaryRowFieldToStrConverter&& _reconverter)
        : partition_key(_partition_key), converter(_converter), reconverter(_reconverter) {}
    std::string partition_key;
    typename DataConverterUtils::StrToBinaryRowConverter converter;
    typename DataConverterUtils::BinaryRowFieldToStrConverter reconverter;
};

/// PartitionComputer for `BinaryRow`.
class BinaryRowPartitionComputer {
 public:
    static Result<std::unique_ptr<BinaryRowPartitionComputer>> Create(
        const std::vector<std::string>& partition_keys,
        const std::shared_ptr<arrow::Schema>& schema, const std::string& default_part_value,
        bool legacy_partition_name_enabled, const std::shared_ptr<MemoryPool>& memory_pool);

    Result<BinaryRow> ToBinaryRow(const std::map<std::string, std::string>& partition) const;
    Result<std::vector<std::pair<std::string, std::string>>> GeneratePartitionVector(
        const BinaryRow& partition) const;
    const std::vector<std::string>& GetPartitionKeys() const {
        return partition_keys_;
    }

 private:
    BinaryRowPartitionComputer(const std::vector<std::string>& partition_keys,
                               const std::shared_ptr<arrow::Schema>& schema,
                               const std::string& default_part_value,
                               const std::vector<PartitionConverter>& partition_converters,
                               const std::shared_ptr<MemoryPool>& memory_pool);

    static Result<arrow::Type::type> GetTypeFromArrowSchema(
        const std::shared_ptr<arrow::Schema>& schema, const std::string& field_name);

    std::shared_ptr<MemoryPool> memory_pool_;
    std::vector<std::string> partition_keys_;
    std::shared_ptr<arrow::Schema> schema_;
    std::string default_part_value_;
    std::vector<PartitionConverter> partition_converters_;
};

}  // namespace paimon
