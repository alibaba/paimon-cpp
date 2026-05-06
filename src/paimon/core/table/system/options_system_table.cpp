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

#include "paimon/core/table/system/options_system_table.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/system_table_scan.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
namespace {

std::shared_ptr<arrow::Schema> OptionsSchema() {
    return arrow::schema({arrow::field("key", arrow::utf8(), /*nullable=*/false),
                          arrow::field("value", arrow::utf8(), /*nullable=*/false)});
}

class OptionsBatchReader : public BatchReader {
 public:
    OptionsBatchReader(std::map<std::string, std::string> options,
                       const std::shared_ptr<MemoryPool>& pool)
        : arrow_pool_(GetArrowPool(pool)), options_(std::move(options)) {}

    Result<ReadBatch> NextBatch() override {
        if (emitted_) {
            return BatchReader::MakeEofBatch();
        }
        emitted_ = true;

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> key_array_builder,
                                          arrow::MakeBuilder(arrow::utf8(), arrow_pool_.get()));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> value_array_builder,
                                          arrow::MakeBuilder(arrow::utf8(), arrow_pool_.get()));
        auto* key_builder = dynamic_cast<arrow::StringBuilder*>(key_array_builder.get());
        auto* value_builder = dynamic_cast<arrow::StringBuilder*>(value_array_builder.get());
        if (key_builder == nullptr || value_builder == nullptr) {
            return Status::Invalid("cannot create string builders for options system table");
        }
        for (const auto& [key, value] : options_) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Append(key));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Append(value));
        }
        std::shared_ptr<arrow::Array> key_array;
        std::shared_ptr<arrow::Array> value_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Finish(&key_array));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Finish(&value_array));
        auto struct_array = std::make_shared<arrow::StructArray>(
            arrow::struct_(OptionsSchema()->fields()), key_array->length(),
            std::vector<std::shared_ptr<arrow::Array>>{key_array, value_array});

        auto c_array = std::make_unique<::ArrowArray>();
        auto c_schema = std::make_unique<::ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*struct_array, c_array.get(), c_schema.get()));
        return std::make_pair(std::move(c_array), std::move(c_schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return std::make_shared<MetricsImpl>();
    }

    void Close() override {
        emitted_ = true;
    }

 private:
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    std::map<std::string, std::string> options_;
    bool emitted_ = false;
};

}  // namespace

OptionsSystemTable::OptionsSystemTable(std::string table_path,
                                       std::shared_ptr<TableSchema> table_schema)
    : table_path_(std::move(table_path)), table_schema_(std::move(table_schema)) {}

std::string OptionsSystemTable::Name() const {
    return kName;
}

std::shared_ptr<arrow::Schema> OptionsSystemTable::ArrowSchema() const {
    return OptionsSchema();
}

Result<std::unique_ptr<TableScan>> OptionsSystemTable::NewScan() const {
    return std::make_unique<SystemTableScan>(table_path_);
}

Result<std::unique_ptr<BatchReader>> OptionsSystemTable::CreateBatchReader(
    const std::vector<std::shared_ptr<Split>>& splits,
    const std::shared_ptr<MemoryPool>& pool) const {
    if (splits.size() != 1) {
        return Status::Invalid("options system table expects a single split");
    }
    for (const auto& split : splits) {
        if (!std::dynamic_pointer_cast<SystemTableSplit>(split)) {
            return Status::Invalid("unsupported split for options system table");
        }
    }
    return std::make_unique<OptionsBatchReader>(table_schema_->Options(), pool);
}

}  // namespace paimon
