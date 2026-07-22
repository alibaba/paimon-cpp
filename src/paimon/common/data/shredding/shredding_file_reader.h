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

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>

#include "arrow/api.h"
#include "paimon/common/data/shredding/shredding_read_plan.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {

/// A file batch reader wrapper that pushes per-column physical (shredded, possibly pruned)
/// subtrees down to the inner reader and assembles the physical batches back into the logical
/// columns as planned by `ShreddingColumnReadPlan`s.
class ShreddingFileReader : public FileBatchReader {
 public:
    ShreddingFileReader(std::unique_ptr<FileBatchReader>&& reader,
                        std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>>&& plans,
                        const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<ReadBatch> NextBatch() override;

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

    void Close() override;

    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;

    Result<uint64_t> GetNumberOfRows() const override;

    bool SupportPreciseBitmapSelection() const override;

 private:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<FileBatchReader> reader_;
    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> plans_;
};

}  // namespace paimon
