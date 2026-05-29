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

#include <cassert>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "paimon/format/blob/blob_format_writer.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
}  // namespace arrow
namespace paimon {
class FileSystem;
class OutputStream;
}  // namespace paimon

namespace paimon::blob {

class BlobWriterBuilder : public SpecificFSWriterBuilder {
 public:
    BlobWriterBuilder(const std::shared_ptr<arrow::DataType>& data_type,
                      const std::map<std::string, std::string>& options)
        : pool_(GetDefaultPool()), data_type_(data_type), options_(options) {
        assert(data_type_);
    }

    WriterBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    SpecificFSWriterBuilder* WithFileSystem(const std::shared_ptr<FileSystem>& fs) override {
        fs_ = fs;
        return this;
    }

    /// Sets a write consumer that will be called after each blob row is written.
    BlobWriterBuilder* WithWriteConsumer(BlobFormatWriter::WriteConsumer consumer) {
        write_consumer_ = std::move(consumer);
        return this;
    }

    Result<std::unique_ptr<FormatWriter>> Build(const std::shared_ptr<OutputStream>& out,
                                                const std::string& compression) override {
        assert(out);
        if (fs_ == nullptr) {
            return Status::Invalid("File system is nullptr. Please call WithFileSystem() first.");
        }
        return BlobFormatWriter::Create(out, data_type_, write_consumer_, fs_, pool_);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::DataType> data_type_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<FileSystem> fs_;
    BlobFormatWriter::WriteConsumer write_consumer_;
};

}  // namespace paimon::blob
