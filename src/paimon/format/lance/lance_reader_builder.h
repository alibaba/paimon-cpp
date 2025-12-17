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
#include "paimon/format/lance/lance_file_batch_reader.h"
#include "paimon/format/lance/lance_format_defs.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"

namespace paimon::lance {
class LanceReaderBuilder : public ReaderBuilder {
 public:
    LanceReaderBuilder(const std::map<std::string, std::string>& options, int32_t batch_size)
        : batch_size_(batch_size), pool_(GetDefaultPool()), options_(options) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(const std::string& path) const override {
        PAIMON_ASSIGN_OR_RAISE(uint32_t batch_readahead, OptionsUtils::GetValueFromMap<uint32_t>(
                                                             options_, LANCE_READAHEAD_BATCH_COUNT,
                                                             DEFAULT_LANCE_READAHEAD_BATCH_COUNT));
        return LanceFileBatchReader::Create(path, batch_size_, batch_readahead);
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& path) const override {
        return Status::Invalid("no support input stream in lance format");
    }

 private:
    int32_t batch_size_ = -1;
    std::shared_ptr<MemoryPool> pool_;
    std::map<std::string, std::string> options_;
};
}  // namespace paimon::lance
