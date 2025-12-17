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
#include <string>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/file_format.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_reader_builder.h"
#include "paimon/format/parquet/parquet_stats_extractor.h"
#include "paimon/format/parquet/parquet_writer_builder.h"

struct ArrowSchema;

namespace paimon {

class WriterBuilder;
class ReaderBuilder;
class FormatStatsExtractor;

namespace parquet {

class ParquetFileFormat : public FileFormat {
 public:
    explicit ParquetFileFormat(const std::map<std::string, std::string>& options)
        : identifier_("parquet"), options_(options) {}

    const std::string& Identifier() const override {
        return identifier_;
    }

    Result<std::unique_ptr<ReaderBuilder>> CreateReaderBuilder(int32_t batch_size) const override {
        return std::make_unique<ParquetReaderBuilder>(options_, batch_size);
    }

    Result<std::unique_ptr<WriterBuilder>> CreateWriterBuilder(::ArrowSchema* schema,
                                                               int32_t batch_size) const override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> typed_schema,
                                          arrow::ImportSchema(schema));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> new_schema,
                               ParquetFieldIdConverter::AddParquetIdsFromPaimonIds(typed_schema));
        return std::make_unique<ParquetWriterBuilder>(new_schema, batch_size, options_);
    }

    Result<std::unique_ptr<FormatStatsExtractor>> CreateStatsExtractor(
        ::ArrowSchema* schema) const override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> typed_schema,
                                          arrow::ImportSchema(schema));
        return std::make_unique<ParquetStatsExtractor>(typed_schema);
    }

 protected:
    std::string identifier_;
    std::map<std::string, std::string> options_;
};

}  // namespace parquet
}  // namespace paimon
