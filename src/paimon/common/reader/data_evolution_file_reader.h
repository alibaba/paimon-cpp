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

#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/metrics.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {
/// This is a union reader which contains multiple inner readers.
///
/// This reader, assembling multiple reader into one big and great reader. The row it produces
/// also come from the readers it contains.
///
/// For example, the expected schema for this reader is : int, int, string, int, string, int.(Total
/// 6 fields) It contains three inner readers, we call them reader0, reader1 and reader2.
///
/// The rowOffsets and fieldOffsets are all 6 elements long the same as
/// output schema. RowOffsets is used to indicate which inner reader the field comes from, and
/// fieldOffsets is used to indicate the offset of the field in the inner reader.
///
/// For example, if rowOffsets is {0, 2, 0, 1, 2, 1} and fieldOffsets is {0, 0, 1, 1, 1, 0}, it
/// means:
/// - The first field comes from reader0, and it is at offset 0 in reader0.
/// - The second field comes from reader2, and it is at offset 0 in reader2.
/// - The third field comes from reader0, and it is at offset 1 in reader0.
/// - The fourth field comes from reader1, and it is at offset 1 in reader1.
/// - The fifth field comes from reader2, and it is at offset 1 in reader2.
/// - The sixth field comes from reader1, and it is at offset 0 in reader1.
///
/// These three readers work together, package out final and complete rows.
class DataEvolutionFileReader : public BatchReader {
 public:
    static Result<std::unique_ptr<DataEvolutionFileReader>> Create(
        std::vector<std::unique_ptr<BatchReader>>&& readers,
        const std::shared_ptr<arrow::Schema>& read_schema, int32_t read_batch_size,
        const std::vector<int32_t>& reader_offsets, const std::vector<int32_t>& field_offsets,
        const std::shared_ptr<MemoryPool>& pool);

    Result<ReadBatch> NextBatch() override {
        return Status::Invalid(
            "paimon inner reader DataEvolutionFileReader should use NextBatchWithBitmap");
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    void Close() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

 private:
    DataEvolutionFileReader(std::vector<std::unique_ptr<BatchReader>>&& readers,
                            const std::shared_ptr<arrow::Schema>& read_schema,
                            int32_t read_batch_size, const std::vector<int32_t>& reader_offsets,
                            const std::vector<int32_t>& field_offsets,
                            const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : arrow_pool_(arrow_pool),
          readers_(std::move(readers)),
          read_schema_(read_schema),
          read_batch_size_(read_batch_size),
          reader_offsets_(reader_offsets),
          field_offsets_(field_offsets),
          cached_array_vec_(readers_.size()),
          non_exist_array_vec_(read_schema->num_fields(), nullptr) {}

    int64_t CalculateCachedArrayLength(size_t reader_idx) const;

    Result<std::shared_ptr<arrow::Array>> NextBatchForSingleReader(size_t reader_idx);

    Result<std::shared_ptr<arrow::Array>> GetOrCreateNonExistArray(int32_t field_idx,
                                                                   int64_t array_length);

 private:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::vector<std::unique_ptr<BatchReader>> readers_;
    std::shared_ptr<arrow::Schema> read_schema_;
    int32_t read_batch_size_;
    std::vector<int32_t> reader_offsets_;
    std::vector<int32_t> field_offsets_;
    std::vector<arrow::ArrayVector> cached_array_vec_;
    arrow::ArrayVector non_exist_array_vec_;
};
}  // namespace paimon
