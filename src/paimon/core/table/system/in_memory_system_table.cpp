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

#include "paimon/core/table/system/in_memory_system_table.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/core/io/generic_row_to_arrow_array_converter.h"
#include "paimon/core/table/system/system_table_scan.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/table/source/table_read.h"

namespace paimon {
namespace {

class InMemorySystemTableBatchReader : public BatchReader {
 public:
    InMemorySystemTableBatchReader(std::shared_ptr<const InMemorySystemTable> table,
                                   const std::shared_ptr<MemoryPool>& pool)
        : table_(std::move(table)), arrow_pool_(GetArrowPool(pool)) {}

    Result<ReadBatch> NextBatch() override {
        if (emitted_) {
            return BatchReader::MakeEofBatch();
        }
        emitted_ = true;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, table_->ArrowSchema());
        PAIMON_ASSIGN_OR_RAISE(std::vector<GenericRow> rows, table_->BuildRows());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GenericRowToArrowArrayConverter> converter,
                               GenericRowToArrowArrayConverter::Create(schema, arrow_pool_.get()));
        return converter->NextBatch(rows);
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return std::make_shared<MetricsImpl>();
    }

    void Close() override {
        emitted_ = true;
    }

 private:
    std::shared_ptr<const InMemorySystemTable> table_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    bool emitted_ = false;
};

class InMemorySystemTableRead : public TableRead {
 public:
    InMemorySystemTableRead(std::shared_ptr<const InMemorySystemTable> table,
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
        return std::make_unique<InMemorySystemTableBatchReader>(table_, GetMemoryPool());
    }

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<Split>& split) override {
        std::vector<std::shared_ptr<Split>> splits = {split};
        return CreateReader(splits);
    }

 private:
    std::shared_ptr<const InMemorySystemTable> table_;
};

}  // namespace

InMemorySystemTable::InMemorySystemTable(std::string table_path)
    : table_path_(std::move(table_path)) {}

Result<std::unique_ptr<TableScan>> InMemorySystemTable::NewScan(
    const std::shared_ptr<ScanContext>& /*context*/) const {
    return std::make_unique<SystemTableScan>(table_path_);
}

Result<std::unique_ptr<TableRead>> InMemorySystemTable::NewRead(
    const std::shared_ptr<ReadContext>& context) const {
    return std::make_unique<InMemorySystemTableRead>(
        std::static_pointer_cast<const InMemorySystemTable>(shared_from_this()),
        context->GetMemoryPool());
}

}  // namespace paimon
