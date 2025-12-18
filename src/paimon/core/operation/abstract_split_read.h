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
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/field_mapping_reader.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/split_read.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/format/reader_builder.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class BinaryRow;
class DataField;
class DataFilePathFactory;
class DataSplitImpl;
class Executor;
class FieldMappingBuilder;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;
class Predicate;
struct DataFileMeta;
class TableSchema;

class AbstractSplitRead : public SplitRead {
 public:
    ~AbstractSplitRead() override = default;

 protected:
    AbstractSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                      const std::shared_ptr<InternalReadContext>& context,
                      std::unique_ptr<SchemaManager>&& schema_manager,
                      const std::shared_ptr<MemoryPool>& memory_pool,
                      const std::shared_ptr<Executor>& executor);

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateRawFileReaders(
        const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
        const std::vector<Range>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const;

    static std::unordered_map<std::string, DeletionFile> CreateDeletionFileMap(
        const DataSplitImpl& data_split);

    Result<std::unique_ptr<BatchReader>> ApplyPredicateFilterIfNeeded(
        std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<Predicate>& predicate) const;

 protected:
    // return nullptr if file is skipped by index or dv
    virtual Result<std::unique_ptr<BatchReader>> ApplyIndexAndDvReaderIfNeeded(
        std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
        const std::shared_ptr<arrow::Schema>& data_schema,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
        const std::vector<Range>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const = 0;

    // 1. project write cols to data schema
    // 2. add partition fields (if write cols not contain)
    // 3. add row tracking fields
    static Result<std::vector<DataField>> ProjectFieldsForRowTrackingAndDataEvolution(
        const std::shared_ptr<TableSchema>& data_schema,
        const std::optional<std::vector<std::string>>& write_cols);

 private:
    Result<std::unique_ptr<ReaderBuilder>> PrepareReaderBuilder(
        const std::string& format_identifier) const;

    Result<std::unique_ptr<FileBatchReader>> CreateFileBatchReader(
        const std::shared_ptr<DataFileMeta>& file_meta, const std::string& data_file_path,
        const ReaderBuilder* reader_builder) const;

    // return nullptr if data file is skipped by index or dv
    Result<std::unique_ptr<BatchReader>> CreateFieldMappingReader(
        const std::string& data_file_path, const std::shared_ptr<DataFileMeta>& file_meta,
        const BinaryRow& partition, const ReaderBuilder* reader_builder,
        const FieldMappingBuilder* field_mapping_builder,
        const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
        const std::vector<Range>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const;

    static bool NeedCompleteRowTrackingFields(bool row_tracking_enabled,
                                              const std::shared_ptr<arrow::Schema>& read_schema);

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    CoreOptions options_;
    // user recall schema
    std::shared_ptr<arrow::Schema> raw_read_schema_;
    std::shared_ptr<InternalReadContext> context_;
    std::unique_ptr<SchemaManager> schema_manager_;
};

}  // namespace paimon
