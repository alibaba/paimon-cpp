/*
 * Copyright 2026-present Alibaba Inc.
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

#include "arrow/api.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/format_stats_extractor.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace arrow {
class DataType;
}  // namespace arrow
namespace paimon {
class FileSystem;
class MemoryPool;
}  // namespace paimon

namespace paimon::avro {

class AvroStatsExtractor : public FormatStatsExtractor {
 public:
    explicit AvroStatsExtractor(const std::map<std::string, std::string>& options)
        : options_(options) {}

    Result<ColumnStatsVector> Extract(const std::shared_ptr<FileSystem>& file_system,
                                      const std::string& path,
                                      const std::shared_ptr<MemoryPool>& pool) override {
        PAIMON_ASSIGN_OR_RAISE(auto result, ExtractWithFileInfoInternal(file_system, path, pool,
                                                                        /*with_file_info=*/false));
        return result.first;
    }

    Result<std::pair<ColumnStatsVector, FileInfo>> ExtractWithFileInfo(
        const std::shared_ptr<FileSystem>& file_system, const std::string& path,
        const std::shared_ptr<MemoryPool>& pool) override {
        return ExtractWithFileInfoInternal(file_system, path, pool, /*with_file_info=*/true);
    }

 private:
    Result<std::pair<ColumnStatsVector, FileInfo>> ExtractWithFileInfoInternal(
        const std::shared_ptr<FileSystem>& file_system, const std::string& path,
        const std::shared_ptr<MemoryPool>& pool, bool with_file_info) const;

    Result<std::unique_ptr<ColumnStats>> FetchColumnStatistics(
        const std::shared_ptr<arrow::DataType>& type) const;

 private:
    std::map<std::string, std::string> options_;
};

}  // namespace paimon::avro
