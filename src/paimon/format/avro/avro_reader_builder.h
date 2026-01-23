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

#include "avro/DataFile.hh"
#include "paimon/format/avro/avro_file_batch_reader.h"
#include "paimon/format/avro/avro_input_stream_impl.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::avro {

class AvroReaderBuilder : public ReaderBuilder {
 public:
    AvroReaderBuilder(const std::map<std::string, std::string>& options, int32_t batch_size)
        : batch_size_(batch_size), pool_(GetDefaultPool()), options_(options) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& path) const override {
        return AvroFileBatchReader::Create(path, batch_size_, pool_);
    }

    Result<std::unique_ptr<FileBatchReader>> Build(const std::string& path) const override {
        return Status::Invalid("do not support build reader with path in avro format");
    }

 private:
    const int32_t batch_size_;
    std::shared_ptr<MemoryPool> pool_;
    const std::map<std::string, std::string> options_;
};

}  // namespace paimon::avro
