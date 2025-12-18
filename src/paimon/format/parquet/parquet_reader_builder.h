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

#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/format/parquet/parquet_file_batch_reader.h"
#include "paimon/format/parquet/parquet_input_stream_impl.h"
#include "paimon/format/reader_builder.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"

namespace paimon::parquet {

class ParquetReaderBuilder : public ReaderBuilder {
 public:
    ParquetReaderBuilder(const std::map<std::string, std::string>& options, int32_t batch_size)
        : batch_size_(batch_size), pool_(GetDefaultPool()), options_(options) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& path) const override {
        PAIMON_ASSIGN_OR_RAISE(uint64_t file_length, path->Length());
        std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(pool_);
        auto input_stream = std::make_unique<ParquetInputStreamImpl>(path, arrow_pool, file_length);
        return ParquetFileBatchReader::Create(std::move(input_stream), arrow_pool, options_,
                                              batch_size_);
    }

    Result<std::unique_ptr<FileBatchReader>> Build(const std::string& path) const override {
        return Status::Invalid("do not support build reader with path in parquet format");
    }

 private:
    int32_t batch_size_ = -1;
    std::shared_ptr<MemoryPool> pool_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon::parquet
