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

#include "paimon/format/avro/avro_file_format.h"

#include <memory>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/avro/avro_reader_builder.h"
#include "paimon/format/avro/avro_writer_builder.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow
struct ArrowSchema;

namespace paimon::avro {

AvroFileFormat::AvroFileFormat(const std::map<std::string, std::string>& options)
    : identifier_("avro"), options_(options) {}

Result<std::unique_ptr<ReaderBuilder>> AvroFileFormat::CreateReaderBuilder(
    int32_t batch_size) const {
    return std::make_unique<AvroReaderBuilder>(options_, batch_size);
}

Result<std::unique_ptr<WriterBuilder>> AvroFileFormat::CreateWriterBuilder(
    ::ArrowSchema* schema, int32_t batch_size) const {
    if (schema == nullptr) {
        return Status::Invalid("schema is nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> typed_schema,
                                      arrow::ImportSchema(schema));
    return std::make_unique<AvroWriterBuilder>(typed_schema, batch_size, options_);
}

Result<std::unique_ptr<FormatStatsExtractor>> AvroFileFormat::CreateStatsExtractor(
    ::ArrowSchema* schema) const {
    ArrowSchemaRelease(schema);
    return Status::NotImplemented("do not support extract stats in avro format for now");
}

}  // namespace paimon::avro
