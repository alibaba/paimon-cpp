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
#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "arrow/util/type_fwd.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "parquet/properties.h"
#include "parquet/type_fwd.h"

namespace arrow {
class Schema;
}  // namespace arrow
namespace paimon {
class OutputStream;
}  // namespace paimon

namespace paimon::parquet {

class ParquetWriterBuilder : public WriterBuilder {
 public:
    ParquetWriterBuilder(const std::shared_ptr<arrow::Schema>& schema, int32_t batch_size,
                         const std::map<std::string, std::string>& options)
        : batch_size_(batch_size),
          pool_(GetArrowPool(GetDefaultPool())),
          schema_(schema),
          options_(options) {
        assert(schema);
    }

    WriterBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = GetArrowPool(pool);
        return this;
    }

    Result<std::unique_ptr<FormatWriter>> Build(const std::shared_ptr<OutputStream>& out,
                                                const std::string& compression) override;

 private:
    static Result<::parquet::ParquetVersion::type> ConvertWriterVersion(
        const std::string& writer_version);

    static std::string CompressLevelOptionsKey(::arrow::Compression::type compression);

    Result<std::shared_ptr<::parquet::WriterProperties>> PrepareWriterProperties(
        const std::string& compression);

    int32_t batch_size_ = -1;
    std::shared_ptr<arrow::MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon::parquet
