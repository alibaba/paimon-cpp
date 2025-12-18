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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>

#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/stats/simple_stats_collector.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow
struct ArrowArray;

namespace paimon {
class ManifestEntry;
class MemoryPool;

class ManifestEntryWriter : public SingleFileWriter<const ManifestEntry&, ManifestFileMeta> {
 public:
    ManifestEntryWriter(const std::string& compression,
                        std::function<Status(const ManifestEntry&, ArrowArray*)> converter,
                        const std::shared_ptr<MemoryPool>& pool,
                        const std::shared_ptr<arrow::Schema>& partition_type)
        : SingleFileWriter<const ManifestEntry&, ManifestFileMeta>(compression, converter),
          pool_(pool),
          partition_stats_collector_(std::make_shared<SimpleStatsCollector>(partition_type)) {}

    Status Write(const ManifestEntry& entry) override;
    Result<ManifestFileMeta> GetResult() override;

 private:
    struct RowIdStats {
        void Collect(int64_t first_row_id, int64_t row_count) {
            min_row_id = std::min(min_row_id, first_row_id);
            max_row_id = std::max(max_row_id, first_row_id + row_count - 1);
        }

        int64_t min_row_id = std::numeric_limits<int64_t>::max();
        int64_t max_row_id = std::numeric_limits<int64_t>::min();
    };

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<SimpleStatsCollector> partition_stats_collector_;

    int64_t num_added_files_ = 0;
    int64_t num_deleted_files_ = 0;
    int64_t schema_id_ = 0;
    int32_t min_bucket_ = std::numeric_limits<int32_t>::max();
    int32_t max_bucket_ = std::numeric_limits<int32_t>::min();
    int32_t min_level_ = std::numeric_limits<int32_t>::max();
    int32_t max_level_ = std::numeric_limits<int32_t>::min();
    std::optional<RowIdStats> row_id_stats_ = RowIdStats();
};

}  // namespace paimon
