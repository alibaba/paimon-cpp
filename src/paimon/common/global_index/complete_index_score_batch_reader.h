/*
 * Copyright 2025-present Alibaba Inc.
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
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {
class MemoryPool;
class Metrics;
/// A batch reader that enriches the output Arrow array with index score information.
/// It assumes the input data already contains the `_INDEX_SCORE` column,
/// and ensures this score is properly updated in the returned batches.
///
/// @pre The read schema must include the `_INDEX_SCORE` field.
class CompleteIndexScoreBatchReader : public BatchReader {
 public:
    CompleteIndexScoreBatchReader(std::unique_ptr<BatchReader>&& reader,
                                  const std::vector<float>& scores,
                                  const std::shared_ptr<MemoryPool>& pool);

    Result<ReadBatch> NextBatch() override;

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    void Close() override {
        reader_->Close();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

 private:
    void UpdateScoreFieldIndex(const arrow::StructType* struct_type);

 private:
    size_t score_cursor_ = 0;
    int32_t index_score_field_idx_ = -1;
    std::vector<std::string> field_names_with_score_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<BatchReader> reader_;
    std::vector<float> scores_;
};
}  // namespace paimon
