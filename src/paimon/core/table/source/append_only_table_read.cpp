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

#include "paimon/core/table/source/append_only_table_read.h"

#include "paimon/core/core_options.h"
#include "paimon/core/operation/data_evolution_split_read.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/status.h"

namespace paimon {
class DataSplit;
class Executor;
class FileStorePathFactory;
class MemoryPool;

AppendOnlyTableRead::AppendOnlyTableRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                         const std::shared_ptr<InternalReadContext>& context,
                                         const std::shared_ptr<MemoryPool>& memory_pool,
                                         const std::shared_ptr<Executor>& executor)
    : TableRead(memory_pool) {
    const auto& core_options = context->GetCoreOptions();
    if (core_options.DataEvolutionEnabled()) {
        // add data evolution first
        split_reads_.push_back(
            std::make_unique<DataEvolutionSplitRead>(path_factory, context, memory_pool, executor));
    } else {
        split_reads_.push_back(
            std::make_unique<RawFileSplitRead>(path_factory, context, memory_pool, executor));
    }
}

Result<std::unique_ptr<BatchReader>> AppendOnlyTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    for (const auto& read : split_reads_) {
        PAIMON_ASSIGN_OR_RAISE(bool matched, read->Match(split, /*force_keep_delete=*/false));
        if (matched) {
            return read->CreateReader(split);
        }
    }
    return Status::Invalid("create reader failed, not read match with data split.");
}

}  // namespace paimon
