/*
 * Copyright 2025-present Alibaba Inc.
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

#include <cstdint>
#include <memory>
#include <vector>

#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class DataSplitImpl;
class Executor;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;
class MergeFileSplitRead;

/// Optimized COUNT(*) reader for PK tables.
///
/// Supports two optimization levels:
///   Level 0: Pure metadata count (DV mode) - zero IO
///   Level 1: MOR mode - column pruning + merge + drop delete + count
///
/// Usage:
///   auto count_read = CountSplitRead::Create(path_factory, context, pool, executor);
///   int64_t count = count_read->CountRows(split);
class CountSplitRead {
 public:
    static Result<std::unique_ptr<CountSplitRead>> Create(
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<InternalReadContext>& context,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<Executor>& executor);

    /// Count rows for a single split. This is the main entry point.
    /// Automatically selects the best optimization level based on split properties.
    Result<int64_t> CountRows(const std::shared_ptr<DataSplitImpl>& split);

 private:
    CountSplitRead(const std::shared_ptr<InternalReadContext>& context,
                   std::unique_ptr<MergeFileSplitRead>&& merge_read,
                   const std::shared_ptr<MemoryPool>& memory_pool);

    /// Level 0: Pure metadata count. DV mode.
    /// Formula: sum(file.row_count - dv.cardinality)
    /// Complexity: O(num_files), zero IO.
    Result<int64_t> MetadataCount(const std::shared_ptr<DataSplitImpl>& split);

    /// Level 1: MOR mode.
    /// Column-pruned merge + drop delete + count.
    Result<int64_t> MergeCount(const std::shared_ptr<DataSplitImpl>& split);

    /// Build a minimal read schema for COUNT(*) queries.
    /// The schema contains only PK columns (required for merge sort).
    /// Note: _SEQUENCE_NUMBER and _VALUE_KIND are automatically added by
    /// GenerateKeyValueReadSchema inside MergeFileSplitRead::Create.
    /// Sequence-group fields are also auto-completed by CompleteSequenceField.
    static Result<std::shared_ptr<arrow::Schema>> BuildCountReadSchema(
        const InternalReadContext& context);

 private:
    std::shared_ptr<InternalReadContext> context_;
    std::unique_ptr<MergeFileSplitRead> merge_read_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
