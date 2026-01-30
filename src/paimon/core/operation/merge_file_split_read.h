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
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/core/io/concat_key_value_record_reader.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/mergetree/compact/interval_partition.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/sort_merge_reader.h"
#include "paimon/core/mergetree/sorted_run.h"
#include "paimon/core/operation/abstract_split_read.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/predicate/predicate.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class BinaryRow;
class CoreOptions;
class DataField;
class DataFilePathFactory;
class DataSplit;
class DataSplitImpl;
class Executor;
class FieldsComparator;
class FileBatchReader;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;
class SchemaManager;
class SortedRun;
class TableSchema;
struct DataFileMeta;
struct DeletionFile;
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

/// If the class name below is enclosed in parentheses, it might be present in the read path;
/// otherwise, it must be present in the read path.
///
/// Readers Overview: (ConcatBatchReader across
/// splits)->CompleteRowKindBatchReader->(PredicateBatchReader)
/// ->ConcatBatchReader across no overlapped
/// files->KeyValueProjectionReader/AsyncKeyValueProjectionReader
/// ->DropDeleteReader->SortMergeReader->ConcatKeyValueRecordReader->KeyValueDataFileRecordReader
/// ->FieldMappingReader->(ApplyDeletionVectorBatchReader)->(DelegatingPrefetchReader)
/// ->(PrefetchFileBatchReader)->FormatReader
class MergeFileSplitRead : public AbstractSplitRead {
 public:
    static Result<std::unique_ptr<MergeFileSplitRead>> Create(
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<InternalReadContext>& context,
        const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor);

    Result<std::unique_ptr<BatchReader>> CreateReader(const std::shared_ptr<Split>& split) override;

    Result<bool> Match(const std::shared_ptr<Split>& split, bool force_keep_delete) const override;

    Result<std::unique_ptr<BatchReader>> ApplyIndexAndDvReaderIfNeeded(
        std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
        const std::shared_ptr<arrow::Schema>& data_schema,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
        const std::optional<std::vector<Range>>& ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const override;

 private:
    Result<std::unique_ptr<BatchReader>> CreateMergeReader(
        const std::shared_ptr<DataSplitImpl>& data_split,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const;

    Result<std::unique_ptr<BatchReader>> CreateNoMergeReader(
        const std::shared_ptr<DataSplitImpl>& data_split, bool only_filter_key,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const;

    Result<std::unique_ptr<BatchReader>> CreateReaderForSection(
        const std::vector<SortedRun>& section, const std::string& bucket_path,
        const BinaryRow& partition,
        const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const;

    Result<std::unique_ptr<KeyValueRecordReader>> CreateReaderForRun(
        const std::string& bucket_path, const BinaryRow& partition, const SortedRun& sorted_run,
        const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
        const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const;

    Result<std::unique_ptr<SortMergeReader>> CreateSortMergeReader(
        std::vector<std::unique_ptr<KeyValueRecordReader>>&& record_readers) const;

    MergeFileSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                       const std::shared_ptr<InternalReadContext>& context,
                       std::unique_ptr<SchemaManager>&& schema_manager, int32_t key_arity,
                       const std::shared_ptr<arrow::Schema>& value_schema,
                       const std::shared_ptr<arrow::Schema>& read_schema,
                       const std::vector<int32_t>& projection,
                       const std::shared_ptr<FieldsComparator>& key_comparator,
                       const std::shared_ptr<FieldsComparator>& interval_partition_comparator,
                       const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
                       const std::shared_ptr<Predicate>& predicate_for_keys,
                       const std::shared_ptr<MemoryPool>& memory_pool,
                       const std::shared_ptr<Executor>& executor);

 private:
    static Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> CreateMergeFunctionWrapper(
        const CoreOptions& core_options, const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::Schema>& value_schema);

    static Status GenerateKeyValueReadSchema(
        const TableSchema& table_schema, const CoreOptions& options,
        const std::shared_ptr<arrow::Schema>& raw_read_schema,
        std::shared_ptr<arrow::Schema>* value_schema, std::shared_ptr<arrow::Schema>* read_schema,
        std::shared_ptr<FieldsComparator>* key_comparator,
        std::shared_ptr<FieldsComparator>* interval_partition_comparator,
        std::shared_ptr<FieldsComparator>* sequence_fields_comparator);

    static Status SplitKeyAndNonKeyField(const std::vector<std::string>& trimmed_key_fields,
                                         const std::vector<DataField>& read_fields,
                                         std::vector<DataField>* key_fields,
                                         std::vector<DataField>* non_key_fields);

    static Status CompleteSequenceField(const TableSchema& table_schema, const CoreOptions& options,
                                        std::vector<DataField>* non_key_fields);

    static Result<std::shared_ptr<Predicate>> GenerateKeyPredicates(
        const std::shared_ptr<Predicate>& predicate, const TableSchema& table_schema);

 private:
    int32_t key_arity_;
    // schema of value member in KeyValue object
    std::shared_ptr<arrow::Schema> value_schema_;
    // actual read schema, e.g., complete all key fields, user defined sequence fields
    std::shared_ptr<arrow::Schema> read_schema_;
    std::vector<int32_t> projection_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<FieldsComparator> interval_partition_comparator_;
    std::shared_ptr<FieldsComparator> user_defined_seq_comparator_;
    std::shared_ptr<Predicate> predicate_for_keys_;
};
}  // namespace paimon
