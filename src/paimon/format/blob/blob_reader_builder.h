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

#include "paimon/common/utils/options_utils.h"
#include "paimon/format/blob/blob_file_batch_reader.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::blob {

class BlobReaderBuilder : public ReaderBuilder {
 public:
    BlobReaderBuilder(int32_t batch_size, const std::map<std::string, std::string>& options)
        : batch_size_(batch_size), pool_(GetDefaultPool()), options_(options) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& input_stream) const override {
        PAIMON_ASSIGN_OR_RAISE(
            bool blob_as_descriptor,
            OptionsUtils::GetValueFromMap<bool>(options_, Options::BLOB_AS_DESCRIPTOR, false));
        return BlobFileBatchReader::Create(input_stream, batch_size_, blob_as_descriptor, pool_);
    }

    Result<std::unique_ptr<FileBatchReader>> Build(const std::string& path) const override {
        return Status::Invalid("do not support build reader with path in blob format");
    }

 private:
    int32_t batch_size_;
    std::shared_ptr<MemoryPool> pool_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon::blob
