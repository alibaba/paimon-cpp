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

#include "paimon/core/operation/abstract_split_read.h"

#include <cassert>
#include <cstddef>
#include <utility>

#include "arrow/type.h"
#include "paimon/common/reader/delegating_prefetch_reader.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/reader/prefetch_file_batch_reader_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/io/complete_row_tracking_fields_reader.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/field_mapping_reader.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/partition/partition_info.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"

namespace paimon {
class BinaryRow;
class Executor;
class FileStorePathFactory;
class MemoryPool;
class Predicate;

AbstractSplitRead::AbstractSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                     const std::shared_ptr<InternalReadContext>& context,
                                     std::unique_ptr<SchemaManager>&& schema_manager,
                                     const std::shared_ptr<MemoryPool>& memory_pool,
                                     const std::shared_ptr<Executor>& executor)
    : pool_(memory_pool),
      executor_(executor),
      path_factory_(path_factory),
      options_(context->GetCoreOptions()),
      raw_read_schema_(context->GetReadSchema()),
      context_(context),
      schema_manager_(std::move(schema_manager)) {}

Result<std::vector<std::unique_ptr<BatchReader>>> AbstractSplitRead::CreateRawFileReaders(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    if (data_files.empty()) {
        return std::vector<std::unique_ptr<BatchReader>>();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FieldMappingBuilder> field_mapping_builder,
        FieldMappingBuilder::Create(read_schema, context_->GetPartitionKeys(), predicate));

    std::vector<std::unique_ptr<BatchReader>> raw_file_readers;
    raw_file_readers.reserve(data_files.size());
    for (const auto& file : data_files) {
        auto data_file_path = data_file_path_factory->ToPath(file);
        PAIMON_ASSIGN_OR_RAISE(std::string data_file_identifier, file->FileFormat());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> reader_builder,
                               PrepareReaderBuilder(data_file_identifier));
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<BatchReader> file_reader,
            CreateFieldMappingReader(data_file_path, file, partition, reader_builder.get(),
                                     field_mapping_builder.get(), deletion_file_map, row_ranges,
                                     data_file_path_factory));
        if (file_reader) {
            raw_file_readers.push_back(std::move(file_reader));
        }
    }
    return std::move(raw_file_readers);
}

bool AbstractSplitRead::NeedCompleteRowTrackingFields(
    bool row_tracking_enabled, const std::shared_ptr<arrow::Schema>& read_schema) {
    if (row_tracking_enabled &&
        (read_schema->GetFieldIndex(SpecialFields::RowId().Name()) != -1 ||
         read_schema->GetFieldIndex(SpecialFields::SequenceNumber().Name()) != -1)) {
        return true;
    }
    return false;
}

std::unordered_map<std::string, DeletionFile> AbstractSplitRead::CreateDeletionFileMap(
    const DataSplitImpl& data_split) {
    std::unordered_map<std::string, DeletionFile> deletion_file_map;
    auto deletion_files = data_split.DeletionFiles();
    if (deletion_files.empty()) {
        return deletion_file_map;
    }
    auto data_file_metas = data_split.DataFiles();
    assert(deletion_files.size() == data_file_metas.size());
    size_t file_count = deletion_files.size();
    for (size_t i = 0; i < file_count; i++) {
        if (deletion_files[i] != std::nullopt) {
            deletion_file_map.emplace(data_file_metas[i]->file_name, deletion_files[i].value());
        }
    }
    return deletion_file_map;
}

Result<std::unique_ptr<BatchReader>> AbstractSplitRead::ApplyPredicateFilterIfNeeded(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<Predicate>& predicate) const {
    if (!context_->EnablePredicateFilter() || predicate == nullptr) {
        return std::move(reader);
    }
    return PredicateBatchReader::Create(std::move(reader), predicate, pool_);
}

Result<std::unique_ptr<ReaderBuilder>> AbstractSplitRead::PrepareReaderBuilder(
    const std::string& format_identifier) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> file_format,
                           FileFormatFactory::Get(format_identifier, options_.ToMap()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> reader_builder,
                           file_format->CreateReaderBuilder(options_.GetReadBatchSize()));
    reader_builder->WithMemoryPool(pool_);
    return reader_builder;
}

Result<std::unique_ptr<FileBatchReader>> AbstractSplitRead::CreateFileBatchReader(
    const std::shared_ptr<DataFileMeta>& file_meta, const std::string& data_file_path,
    const ReaderBuilder* reader_builder) const {
    PAIMON_ASSIGN_OR_RAISE(std::string file_format_identifier, file_meta->FileFormat());
    if (file_format_identifier == "lance") {
        // lance do not support stream build with input stream
        return reader_builder->Build(data_file_path);
    }
    if (context_->EnablePrefetch() && file_format_identifier != "blob" &&
        file_format_identifier != "avro") {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<PrefetchFileBatchReaderImpl> prefetch_reader,
            PrefetchFileBatchReaderImpl::Create(
                data_file_path, reader_builder, options_.GetFileSystem(),
                context_->GetPrefetchMaxParallelNum(), options_.GetReadBatchSize(),
                context_->GetPrefetchBatchCount(), options_.EnableAdaptivePrefetchStrategy(),
                executor_,
                /*initialize_read_ranges=*/false, context_->EnablePrefetchCache(),
                context_->GetCacheConfig(), pool_));
        return std::make_unique<DelegatingPrefetchReader>(std::move(prefetch_reader));
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                               options_.GetFileSystem()->Open(data_file_path));
        return reader_builder->Build(input_stream);
    }
}

Result<std::unique_ptr<BatchReader>> AbstractSplitRead::CreateFieldMappingReader(
    const std::string& data_file_path, const std::shared_ptr<DataFileMeta>& file_meta,
    const BinaryRow& partition, const ReaderBuilder* reader_builder,
    const FieldMappingBuilder* field_mapping_builder,
    const std::unordered_map<std::string, DeletionFile>& deletion_file_map,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    std::shared_ptr<TableSchema> data_schema;
    if (file_meta->schema_id == context_->GetTableSchema()->Id()) {
        data_schema = context_->GetTableSchema();
    } else {
        // load schema to get data schema
        PAIMON_ASSIGN_OR_RAISE(data_schema, schema_manager_->ReadSchema(file_meta->schema_id));
    }
    std::unique_ptr<FieldMapping> field_mapping;
    if (!data_schema->PrimaryKeys().empty()) {
        // for pk table, add special fields to file schema when field mapping
        std::vector<DataField> file_fields = {SpecialFields::SequenceNumber(),
                                              SpecialFields::ValueKind()};
        file_fields.insert(file_fields.end(), data_schema->Fields().begin(),
                           data_schema->Fields().end());
        PAIMON_ASSIGN_OR_RAISE(field_mapping,
                               field_mapping_builder->CreateFieldMapping(file_fields));
    } else {
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<DataField> projected_data_fields,
            ProjectFieldsForRowTrackingAndDataEvolution(data_schema, file_meta->write_cols));
        PAIMON_ASSIGN_OR_RAISE(field_mapping,
                               field_mapping_builder->CreateFieldMapping(projected_data_fields));
    }

    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(
        field_mapping->non_partition_info.non_partition_data_schema);

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> file_reader,
                           CreateFileBatchReader(file_meta, data_file_path, reader_builder));
    if (NeedCompleteRowTrackingFields(options_.RowTrackingEnabled(), read_schema)) {
        file_reader = std::make_unique<CompleteRowTrackingFieldsBatchReader>(
            std::move(file_reader), file_meta->first_row_id, file_meta->max_sequence_number, pool_);
    }

    const auto& predicate = field_mapping->non_partition_info.non_partition_filter;
    auto all_data_schema = DataField::ConvertDataFieldsToArrowSchema(data_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> final_reader,
                           ApplyIndexAndDvReaderIfNeeded(
                               std::move(file_reader), file_meta, all_data_schema, read_schema,
                               predicate, deletion_file_map, row_ranges, data_file_path_factory));
    if (!final_reader) {
        // file is skipped by index or dv
        return std::unique_ptr<BatchReader>();
    }

    return std::make_unique<FieldMappingReader>(field_mapping_builder->GetReadFieldCount(),
                                                std::move(final_reader), partition,
                                                std::move(field_mapping), pool_);
}

Result<std::vector<DataField>> AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
    const std::shared_ptr<TableSchema>& data_schema,
    const std::optional<std::vector<std::string>>& write_cols) {
    std::vector<DataField> projected_fields;
    const std::vector<std::string>& partition_keys = data_schema->PartitionKeys();
    if (write_cols == std::nullopt) {
        projected_fields = data_schema->Fields();
    } else {
        if (write_cols.value().empty()) {
            return Status::Invalid("write cols cannot be empty");
        }
        for (const auto& write_col : write_cols.value()) {
            if (write_col == SpecialFields::RowId().Name() ||
                write_col == SpecialFields::SequenceNumber().Name()) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, data_schema->GetField(write_col));
            projected_fields.push_back(field);
        }
        for (const auto& partition_key : partition_keys) {
            if (!ObjectUtils::Contains(write_cols.value(), partition_key)) {
                PAIMON_ASSIGN_OR_RAISE(DataField partition_field,
                                       data_schema->GetField(partition_key));
                projected_fields.push_back(partition_field);
            }
        }
    }

    projected_fields.push_back(SpecialFields::RowId());
    projected_fields.push_back(SpecialFields::SequenceNumber());
    return projected_fields;
}

}  // namespace paimon
