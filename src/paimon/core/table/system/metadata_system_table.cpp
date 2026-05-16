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

#include "paimon/core/table/system/metadata_system_table.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/table/system/system_table_scan.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/table/source/table_read.h"

namespace paimon {
namespace {

class MetadataBatchReader : public BatchReader {
 public:
    MetadataBatchReader(const MetadataSystemTable& table, const std::shared_ptr<MemoryPool>& pool)
        : table_(table), arrow_pool_(GetArrowPool(pool)) {}

    Result<ReadBatch> NextBatch() override {
        if (emitted_) {
            return BatchReader::MakeEofBatch();
        }
        emitted_ = true;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> record_batch,
                               table_.BuildRecordBatch(arrow_pool_.get()));
        std::shared_ptr<arrow::StructArray> struct_array =
            std::make_shared<arrow::StructArray>(arrow::struct_(record_batch->schema()->fields()),
                                                 record_batch->num_rows(), record_batch->columns());
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
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
    const MetadataSystemTable& table_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    bool emitted_ = false;
};

class MetadataTableRead : public TableRead {
 public:
    MetadataTableRead(std::shared_ptr<const MetadataSystemTable> table,
                      const std::shared_ptr<MemoryPool>& memory_pool)
        : TableRead(memory_pool), table_(std::move(table)) {}

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& splits) override {
        if (splits.size() != 1) {
            return Status::Invalid(table_->Name(), " system table expects a single split");
        }
        for (const auto& split : splits) {
            if (!std::dynamic_pointer_cast<SystemTableSplit>(split)) {
                return Status::Invalid("unsupported split for ", table_->Name(), " system table");
            }
        }
        return std::make_unique<MetadataBatchReader>(*table_, GetMemoryPool());
    }

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<Split>& split) override {
        std::vector<std::shared_ptr<Split>> splits = {split};
        return CreateReader(splits);
    }

 private:
    std::shared_ptr<const MetadataSystemTable> table_;
};

}  // namespace

MetadataSystemTable::MetadataSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                         std::string branch)
    : fs_(std::move(fs)),
      table_path_(std::move(table_path)),
      branch_(BranchManager::NormalizeBranch(branch)) {}

Result<std::unique_ptr<TableScan>> MetadataSystemTable::NewScan(
    const std::shared_ptr<ScanContext>& /*context*/) const {
    return std::make_unique<SystemTableScan>(table_path_);
}

Result<std::unique_ptr<TableRead>> MetadataSystemTable::NewRead(
    const std::shared_ptr<ReadContext>& context) const {
    return std::make_unique<MetadataTableRead>(
        std::static_pointer_cast<const MetadataSystemTable>(shared_from_this()),
        context->GetMemoryPool());
}

}  // namespace paimon
