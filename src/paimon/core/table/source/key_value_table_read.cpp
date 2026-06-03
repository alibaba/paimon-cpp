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

#include "paimon/core/table/source/key_value_table_read.h"

#include <utility>

#include "paimon/core/operation/merge_file_split_read.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/pk_count_reader.h"
#include "paimon/status.h"

namespace paimon {
class DataSplit;
class Executor;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;

KeyValueTableRead::KeyValueTableRead(std::vector<std::unique_ptr<SplitRead>>&& split_reads,
                                     const std::shared_ptr<FileStorePathFactory>& path_factory,
                                     const std::shared_ptr<InternalReadContext>& context,
                                     const std::shared_ptr<MemoryPool>& memory_pool,
                                     const std::shared_ptr<Executor>& executor)
    : TableRead(memory_pool),
      split_reads_(std::move(split_reads)),
      path_factory_(path_factory),
      context_(context),
      executor_(executor) {}

Result<std::unique_ptr<TableRead>> KeyValueTableRead::Create(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor) {
    auto raw_file_split_read =
        std::make_unique<RawFileSplitRead>(path_factory, context, memory_pool, executor);
    std::vector<std::unique_ptr<SplitRead>> split_reads;
    split_reads.emplace_back(std::move(raw_file_split_read));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<MergeFileSplitRead> merge_file_split_read,
        MergeFileSplitRead::Create(path_factory, context, memory_pool, executor));
    split_reads.emplace_back(std::move(merge_file_split_read));

    return std::unique_ptr<TableRead>(new KeyValueTableRead(std::move(split_reads), path_factory,
                                                            context, memory_pool, executor));
}

void KeyValueTableRead::ForceKeepDelete(bool force_keep_delete) {
    force_keep_delete_ = force_keep_delete;
    for (const auto& read : split_reads_) {
        auto* merge_read = dynamic_cast<MergeFileSplitRead*>(read.get());
        if (merge_read != nullptr) {
            merge_read->ForceKeepDelete(force_keep_delete);
        }
    }
}

Result<std::unique_ptr<BatchReader>> KeyValueTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    auto data_split = std::dynamic_pointer_cast<DataSplit>(split);
    if (!data_split) {
        return Status::Invalid("split cannot be casted to DataSplit");
    }
    for (const auto& read : split_reads_) {
        PAIMON_ASSIGN_OR_RAISE(bool matched, read->Match(data_split, force_keep_delete_));
        if (matched) {
            return read->CreateReader(data_split);
        }
    }
    return Status::Invalid("create reader failed, not read match with data split.");
}

Result<std::unique_ptr<CountReader>> KeyValueTableRead::CreateCountReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    if (context_->GetPredicate() != nullptr) {
        return Status::NotImplemented(
            "CreateCountReader with predicate pushdown is not supported yet");
    }

    if (force_keep_delete_) {
        return Status::NotImplemented("CreateCountReader with force_keep_delete is not supported");
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<PKCountReader> pk_count_reader,
        PKCountReader::Create(splits, path_factory_, context_, GetMemoryPool(), executor_));

    return pk_count_reader;
}

}  // namespace paimon
