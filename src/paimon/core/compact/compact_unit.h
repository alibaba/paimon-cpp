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

#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/mergetree/level_sorted_run.h"
namespace paimon {
/// A files unit for compaction.
struct CompactUnit {
    static CompactUnit FromLevelRuns(int32_t output_level,
                                     const std::vector<LevelSortedRun>& runs) {
        std::vector<std::shared_ptr<DataFileMeta>> files;
        for (const auto& run : runs) {
            const auto& files_in_run = run.run.Files();
            files.insert(files.end(), files_in_run.begin(), files_in_run.end());
        }
        return FromFiles(output_level, files, /*file_rewrite=*/false);
    }

    static CompactUnit FromFiles(int32_t output_level,
                                 const std::vector<std::shared_ptr<DataFileMeta>>& files,
                                 bool file_rewrite) {
        return CompactUnit(output_level, files, file_rewrite);
    }

    CompactUnit(int32_t _output_level, const std::vector<std::shared_ptr<DataFileMeta>>& _files,
                bool _file_rewrite)
        : output_level(_output_level), files(_files), file_rewrite(_file_rewrite) {}

    int32_t output_level;
    std::vector<std::shared_ptr<DataFileMeta>> files;
    bool file_rewrite;
};
}  // namespace paimon
