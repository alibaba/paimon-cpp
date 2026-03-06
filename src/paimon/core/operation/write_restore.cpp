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

#include "paimon/core/operation/write_restore.h"

#include <memory>
#include <optional>
#include <vector>

namespace paimon {

Result<std::optional<int32_t>> WriteRestore::ExtractDataFiles(
    const std::vector<ManifestEntry>& entries,
    std::vector<std::shared_ptr<DataFileMeta>>* data_files) {
    std::optional<int32_t> total_buckets;
    for (const auto& entry : entries) {
        if (total_buckets.has_value() && total_buckets.value() != entry.TotalBuckets()) {
            return Status::Invalid(fmt::format(
                "Bucket data files has different total bucket number, {} vs {}, this should "
                "be a bug.",
                total_buckets.value(), entry.TotalBuckets()));
        }
        total_buckets = entry.TotalBuckets();
        data_files->push_back(entry.File());
    }
    return total_buckets;
}

}  // namespace paimon
