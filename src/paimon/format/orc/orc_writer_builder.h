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

#include "paimon/format/orc/orc_format_writer.h"
#include "paimon/format/orc/orc_output_stream_impl.h"
#include "paimon/format/writer_builder.h"

namespace paimon::orc {

class OrcWriterBuilder : public WriterBuilder {
 public:
    OrcWriterBuilder(const std::shared_ptr<arrow::Schema>& schema, int32_t batch_size,
                     const std::map<std::string, std::string>& options)
        : batch_size_(batch_size), pool_(GetDefaultPool()), schema_(schema), options_(options) {
        assert(schema);
    }

    WriterBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    Result<std::unique_ptr<FormatWriter>> Build(const std::shared_ptr<OutputStream>& out,
                                                const std::string& compression) override {
        assert(out);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OrcOutputStreamImpl> output_stream,
                               OrcOutputStreamImpl::Create(out));
        // each format writer has its own memory pool while shares other options
        return OrcFormatWriter::Create(std::move(output_stream), *schema_, options_, compression,
                                       batch_size_, pool_);
    }

 private:
    int32_t batch_size_ = -1;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
};
}  // namespace paimon::orc
