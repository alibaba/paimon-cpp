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

#include "avro/DataFile.hh"
#include "paimon/format/avro/avro_record_converter.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"

namespace paimon::avro {

class AvroFileBatchReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<AvroFileBatchReader>> Create(
        std::unique_ptr<::avro::DataFileReader<::avro::GenericDatum>>&& reader, int32_t batch_size,
        const std::shared_ptr<MemoryPool>& pool);

    ~AvroFileBatchReader() override;

    Result<BatchReader::ReadBatch> NextBatch() override;

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    uint64_t GetPreviousBatchFirstRowNumber() const override {
        assert(false);
        return -1;
    }

    uint64_t GetNumberOfRows() const override {
        assert(false);
        return -1;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        assert(false);
        return nullptr;
    }

    void Close() override {
        DoClose();
    }

    bool SupportPreciseBitmapSelection() const override {
        return false;
    }

 private:
    void DoClose();

    AvroFileBatchReader(std::unique_ptr<::avro::DataFileReader<::avro::GenericDatum>>&& reader,
                        std::unique_ptr<AvroRecordConverter>&& record_converter,
                        int32_t batch_size);

    std::unique_ptr<::avro::DataFileReader<::avro::GenericDatum>> reader_;
    std::unique_ptr<AvroRecordConverter> record_converter_;
    const int32_t batch_size_;
    bool close_ = false;
};

}  // namespace paimon::avro
