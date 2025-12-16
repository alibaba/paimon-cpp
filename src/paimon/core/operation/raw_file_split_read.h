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
#include <unordered_map>

#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/abstract_split_read.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}

namespace paimon {
class DataFilePathFactory;
class DataSplit;
class Executor;
class FileBatchReader;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;
class Predicate;
struct DataFileMeta;
struct DeletionFile;

/// If the class name below is enclosed in parentheses, it might be present in the read path;
/// otherwise, it must be present in the read path.
///
/// Readers Overview: (ConcatBatchReader across
/// splits)->CompleteRowKindBatchReader->(PredicateBatchReader)
/// ->ConcatBatchReader across
/// files->FieldMappingReader->(ApplyBitmapIndexBatchReader)->(CompleteRowTrackingFieldsBatchReader)
/// ->(DelegatingPrefetchReader)->(PrefetchFileBatchReader)->FormatReader

class RawFileSplitRead : public AbstractSplitRead {
 public:
    RawFileSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                     const std::shared_ptr<InternalReadContext>& context,
                     const std::shared_ptr<MemoryPool>& memory_pool,
                     const std::shared_ptr<Executor>& executor);

    Result<std::unique_ptr<BatchReader>> CreateReader(const std::shared_ptr<Split>& split) override;

    Result<bool> Match(const std::shared_ptr<Split>& split, bool force_keep_delete) const override;

    Result<std::unique_ptr<BatchReader>> ApplyIndexAndDvReaderIfNeeded(
        std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
        const std::shared_ptr<arrow::Schema>& data_schema,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
        const std::vector<Range>& ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const override;
};

}  // namespace paimon
