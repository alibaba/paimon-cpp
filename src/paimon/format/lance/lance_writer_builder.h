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

#include "paimon/format/lance/lance_format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::lance {

class LanceWriterBuilder : public DirectWriterBuilder {
 public:
    LanceWriterBuilder(const std::shared_ptr<arrow::Schema>& schema,
                       const std::map<std::string, std::string>& options)
        : pool_(GetDefaultPool()), schema_(schema), options_(options) {
        assert(schema);
    }

    WriterBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    Result<std::unique_ptr<FormatWriter>> Build(const std::shared_ptr<OutputStream>& out,
                                                const std::string& compression) override {
        return Status::Invalid("no support OutputStream in lance format");
    }

    Result<std::unique_ptr<FormatWriter>> BuildFromPath(const std::string& path) override {
        return LanceFormatWriter::Create(path, schema_);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
};
}  // namespace paimon::lance
