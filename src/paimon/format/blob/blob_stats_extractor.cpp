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

#include "paimon/format/blob/blob_stats_extractor.h"

#include <cassert>
#include <optional>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/format/blob/blob_file_batch_reader.h"
#include "paimon/format/column_stats.h"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;
}  // namespace paimon

namespace paimon::blob {

Result<std::pair<ColumnStatsVector, FormatStatsExtractor::FileInfo>>
BlobStatsExtractor::ExtractWithFileInfo(const std::shared_ptr<FileSystem>& file_system,
                                        const std::string& path,
                                        const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream, file_system->Open(path));
    assert(input_stream);
    if (write_schema_->num_fields() != 1) {
        return Status::Invalid(
            fmt::format("schema field number {} is not 1", write_schema_->num_fields()));
    }
    if (!BlobUtils::IsBlobField(write_schema_->field(0))) {
        return Status::Invalid(
            fmt::format("field {} is not BLOB", write_schema_->field(0)->ToString()));
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BlobFileBatchReader> blob_reader,
        BlobFileBatchReader::Create(input_stream,
                                    /*batch_size=*/1024, /*blob_as_descriptor=*/true, pool));
    ColumnStatsVector result_stats;
    result_stats.push_back(
        ColumnStats::CreateStringColumnStats(std::nullopt, std::nullopt, /*null_count=*/0));
    PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, blob_reader->GetNumberOfRows());
    return std::make_pair(result_stats, FileInfo(num_rows));
}

}  // namespace paimon::blob
