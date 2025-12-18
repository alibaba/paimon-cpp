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

#include <memory>
#include <string>

#include "paimon/format/format_stats_extractor.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate.h"

struct ArrowSchema;

namespace paimon {

/// `FileFormat` is used to create `ReaderBuilder` and `WriterBuilder`.
class PAIMON_EXPORT FileFormat {
 public:
    virtual ~FileFormat() = default;
    /// @return The corresponding identifier of file format, e.g., orc.
    virtual const std::string& Identifier() const = 0;
    /// @return A reader builder which will create reader with specific batch size.
    virtual Result<std::unique_ptr<ReaderBuilder>> CreateReaderBuilder(
        int32_t batch_size) const = 0;

    /// @return A `WriterBuilder` of the corresponding schema, or error status when schema is
    /// invalid.
    virtual Result<std::unique_ptr<WriterBuilder>> CreateWriterBuilder(
        ::ArrowSchema* schema, int32_t batch_size) const = 0;
    /// @return A `FormatStatsExtractor` of current file format.
    virtual Result<std::unique_ptr<FormatStatsExtractor>> CreateStatsExtractor(
        ::ArrowSchema* schema) const = 0;
};

}  // namespace paimon
