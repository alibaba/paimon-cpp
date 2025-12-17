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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/common/reader/data_evolution_file_reader.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/abstract_split_read.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"

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
class BinaryRow;
struct DeletionFile;

/// If the class name below is enclosed in parentheses, it might be present in the read path;
/// otherwise, it must be present in the read path.
///
/// Readers Overview: (ConcatBatchReader across
/// splits)->(CompleteIndexScoreBatchReader)->CompleteRowKindBatchReader->(PredicateBatchReader)
/// ->ConcatBatchReader across files->DataEvolutionFileReader->(ConcatBatchReader across blob files)
/// ->FieldMappingReader->(CompleteRowTrackingFieldsBatchReader)
/// ->(DelegatingPrefetchReader)->(PrefetchFileBatchReader)->FormatReader
///
///
/// A union `SplitRead` to read multiple inner files to merge columns, note that this class
/// does not support filtering push down and deletion vectors, as they can interfere with the
/// process of merging columns.
class DataEvolutionSplitRead : public AbstractSplitRead {
 public:
    DataEvolutionSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
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
        const std::vector<Range>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const override;

 private:
    /// Files for partial field.
    class FieldBunch {
     public:
        virtual ~FieldBunch() = default;
        virtual int64_t RowCount() const = 0;
        virtual const std::vector<std::shared_ptr<DataFileMeta>>& Files() const = 0;
    };

    class DataBunch : public FieldBunch {
     public:
        explicit DataBunch(const std::shared_ptr<DataFileMeta>& data_file)
            : data_files_({data_file}) {}
        int64_t RowCount() const override {
            return data_files_[0]->row_count;
        }
        const std::vector<std::shared_ptr<DataFileMeta>>& Files() const override {
            return data_files_;
        }

     private:
        std::vector<std::shared_ptr<DataFileMeta>> data_files_;
    };

    class BlobBunch : public FieldBunch {
     public:
        explicit BlobBunch(int64_t expected_row_count, bool has_row_ids_selection)
            : expected_row_count_(expected_row_count),
              has_row_ids_selection_(has_row_ids_selection) {}
        int64_t RowCount() const override {
            return row_count_;
        }
        const std::vector<std::shared_ptr<DataFileMeta>>& Files() const override {
            return files_;
        }
        Status Add(const std::shared_ptr<DataFileMeta>& file);

     private:
        int64_t expected_row_count_ = -1;
        int64_t latest_first_row_id_ = -1;
        int64_t expected_next_first_row_id_ = -1;
        int64_t latest_max_sequence_number_ = -1;
        int64_t row_count_ = 0;
        bool has_row_ids_selection_ = false;
        std::vector<std::shared_ptr<DataFileMeta>> files_;
    };

 private:
    Result<std::unique_ptr<BatchReader>> InnerCreateReader(
        const std::shared_ptr<DataSplit>& data_split, const std::vector<Range>& row_ranges) const;

    static Result<std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>>>
    SplitFieldBunches(const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
                      const std::function<Result<int32_t>(const std::shared_ptr<DataFileMeta>&)>&
                          blob_field_to_field_id,
                      bool has_row_ranges_selection);
    static Result<std::vector<std::vector<std::shared_ptr<DataFileMeta>>>> MergeRangesAndSort(
        std::vector<std::shared_ptr<DataFileMeta>>&& files);

    static bool HasIndexScoreField(const std::shared_ptr<arrow::Schema>& read_schema);

 private:
    Result<std::unique_ptr<DataEvolutionFileReader>> CreateUnionReader(
        const BinaryRow& partition,
        const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
        const std::vector<Range>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const;
};

}  // namespace paimon
