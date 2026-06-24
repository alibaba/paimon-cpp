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

#include <memory>
#include <vector>

#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class CoreOptions;
struct DataFileMeta;
class DataFilePathFactory;
class MapSharedShreddingContext;
class MemoryPool;

class MapSharedShreddingCoreUtils {
 public:
    MapSharedShreddingCoreUtils() = delete;
    ~MapSharedShreddingCoreUtils() = delete;

    static Result<std::shared_ptr<MapSharedShreddingContext>> CreateAndRestoreContext(
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::vector<std::shared_ptr<DataFileMeta>>& restore_files,
        const std::shared_ptr<DataFilePathFactory>& path_factory, const CoreOptions& options,
        const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
