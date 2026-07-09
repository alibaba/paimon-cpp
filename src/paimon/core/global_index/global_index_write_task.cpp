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

#include "paimon/global_index/global_index_write_task.h"

#include <set>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/read_context.h"
#include "paimon/table/source/table_read.h"
namespace paimon {
namespace {
Result<std::unique_ptr<GlobalIndexer>> CreateGlobalIndexer(const std::string& index_type,
                                                           const CoreOptions& core_options) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                           GlobalIndexerFactory::Get(index_type, core_options.ToMap()));
    if (!indexer) {
        return Status::Invalid(
            fmt::format("Unknown index type {}, may not registered", index_type));
    }
    return indexer;
}

Result<std::shared_ptr<GlobalIndexFileManager>> CreateGlobalIndexFileManager(
    const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
    const CoreOptions& core_options, const std::shared_ptr<MemoryPool>& pool) {
    auto all_arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            table_path, all_arrow_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            pool));
    std::shared_ptr<IndexPathFactory> index_path_factory =
        path_factory->CreateGlobalIndexFileFactory();
    return std::make_shared<GlobalIndexFileManager>(core_options.GetFileSystem(),
                                                    index_path_factory);
}

Result<std::shared_ptr<GlobalIndexWriter>> CreateGlobalIndexWriter(
    const GlobalIndexer& indexer, const DataField& field,
    const std::vector<DataField>& extra_fields,
    const std::shared_ptr<GlobalIndexFileManager>& index_file_manager,
    const std::shared_ptr<MemoryPool>& pool) {
    arrow::FieldVector arrow_fields;
    arrow_fields.reserve(extra_fields.size() + 1);
    arrow_fields.push_back(DataField::ConvertDataFieldToArrowField(field));
    for (const auto& extra_field : extra_fields) {
        arrow_fields.push_back(DataField::ConvertDataFieldToArrowField(extra_field));
    }
    auto arrow_schema = arrow::schema(arrow_fields);
    ArrowSchema c_arrow_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
    ScopeGuard guard([&]() { ArrowSchemaRelease(&c_arrow_schema); });
    return indexer.CreateWriter(field.Name(), &c_arrow_schema, index_file_manager, pool);
}

Result<std::vector<DataField>> GetExtraFields(const TableSchema& table_schema,
                                              const std::string& field_name,
                                              const std::vector<std::string>& extra_field_names) {
    std::vector<DataField> extra_fields;
    extra_fields.reserve(extra_field_names.size());
    std::set<std::string> dedup_field_names;
    for (const auto& extra_field_name : extra_field_names) {
        if (extra_field_name == field_name) {
            return Status::Invalid(fmt::format(
                "global index extra field {} must not be the indexed field", extra_field_name));
        }
        if (!dedup_field_names.insert(extra_field_name).second) {
            return Status::Invalid(fmt::format("global index extra field {} must not be duplicated",
                                               extra_field_name));
        }
        PAIMON_ASSIGN_OR_RAISE(DataField extra_field, table_schema.GetField(extra_field_name));
        extra_fields.push_back(extra_field);
    }
    return extra_fields;
}

std::vector<std::string> BuildReadFieldNames(const std::string& field_name,
                                             const std::vector<DataField>& extra_fields) {
    std::vector<std::string> read_field_names;
    read_field_names.reserve(extra_fields.size() + 2);
    read_field_names.push_back(field_name);
    for (const auto& extra_field : extra_fields) {
        read_field_names.push_back(extra_field.Name());
    }
    read_field_names.push_back(SpecialFields::RowId().Name());
    return read_field_names;
}

std::vector<std::string> BuildWriterFieldNames(const std::string& field_name,
                                               const std::vector<DataField>& extra_fields) {
    std::vector<std::string> writer_field_names;
    writer_field_names.reserve(extra_fields.size() + 1);
    writer_field_names.push_back(field_name);
    for (const auto& extra_field : extra_fields) {
        writer_field_names.push_back(extra_field.Name());
    }
    return writer_field_names;
}

std::optional<std::vector<int32_t>> GetExtraFieldIds(const std::vector<DataField>& extra_fields) {
    if (extra_fields.empty()) {
        return std::nullopt;
    }
    std::vector<int32_t> extra_field_ids;
    extra_field_ids.reserve(extra_fields.size());
    for (const auto& extra_field : extra_fields) {
        extra_field_ids.push_back(extra_field.Id());
    }
    return extra_field_ids;
}

Result<std::unique_ptr<BatchReader>> CreateBatchReader(
    const std::string& table_path, const std::vector<std::string>& read_field_names,
    const std::shared_ptr<IndexedSplit>& indexed_split, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetOptions(core_options.ToMap())
        .WithFileSystem(core_options.GetFileSystem())
        .EnablePrefetch(true)
        .WithMemoryPool(pool)
        .SetReadFieldNames(read_field_names);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                           read_context_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           TableRead::Create(std::move(read_context)));
    return table_read->CreateReader(indexed_split);
}

Result<std::vector<GlobalIndexIOMeta>> BuildIndex(
    const std::string& field_name, const Range& range,
    const std::vector<std::string>& writer_field_names, BatchReader* batch_reader,
    GlobalIndexWriter* global_index_writer) {
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch read_batch, batch_reader->NextBatch());
        if (BatchReader::IsEofBatch(read_batch)) {
            break;
        }
        auto& [c_array, c_schema] = read_batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
        if (!struct_array) {
            return Status::Invalid(
                "array read from batch reader is not a struct array in GlobalIndexWriteTask");
        }
        auto row_id_array = struct_array->GetFieldByName(SpecialFields::RowId().Name());
        auto typed_row_id_array = std::dynamic_pointer_cast<arrow::Int64Array>(row_id_array);
        if (!typed_row_id_array) {
            return Status::Invalid(
                fmt::format("read array does not contain {} field, or it cannot be casted to "
                            "Int64Array in GlobalIndexWriteTask",
                            SpecialFields::RowId().Name()));
        }
        std::vector<int64_t> relative_row_ids;
        relative_row_ids.reserve(typed_row_id_array->length());
        for (int64_t i = 0; i < typed_row_id_array->length(); i++) {
            int64_t row_id = typed_row_id_array->Value(i);
            if (row_id < range.from || row_id > range.to) {
                return Status::Invalid("invalid row id {}, out of range [{}, {}]", row_id,
                                       range.from, range.to);
            }
            relative_row_ids.push_back(row_id - range.from);
        }
        std::vector<std::shared_ptr<arrow::Array>> writer_arrays;
        writer_arrays.reserve(writer_field_names.size());
        for (const auto& writer_field_name : writer_field_names) {
            auto writer_array = struct_array->GetFieldByName(writer_field_name);
            if (!writer_array) {
                return Status::Invalid(
                    fmt::format("read array does not contain {} field in GlobalIndexWriteTask",
                                writer_field_name));
            }
            writer_arrays.push_back(writer_array);
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> new_array,
            arrow::StructArray::Make(writer_arrays, writer_field_names));
        ::ArrowArray c_new_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*new_array, &c_new_array));
        PAIMON_RETURN_NOT_OK(
            global_index_writer->AddBatch(&c_new_array, std::move(relative_row_ids)));
    }
    return global_index_writer->Finish();
}

Result<std::shared_ptr<CommitMessage>> ToCommitMessage(
    const std::string& index_type, int32_t field_id, const Range& range,
    const std::vector<GlobalIndexIOMeta>& global_index_io_metas, const BinaryRow& partition,
    int32_t bucket, const std::shared_ptr<GlobalIndexFileManager>& file_manager,
    const std::optional<std::vector<int32_t>>& extra_field_ids) {
    std::vector<std::shared_ptr<IndexFileMeta>> index_file_metas;
    index_file_metas.reserve(global_index_io_metas.size());
    bool is_external_path = file_manager->IsExternalPath();
    for (const auto& io_meta : global_index_io_metas) {
        std::optional<std::string> external_path;
        if (is_external_path) {
            PAIMON_ASSIGN_OR_RAISE(Path path, PathUtil::ToPath(io_meta.file_path));
            external_path = path.ToString();
        }
        index_file_metas.push_back(std::make_shared<IndexFileMeta>(
            index_type, PathUtil::GetName(io_meta.file_path), io_meta.file_size, range.Count(),
            /*dv_ranges=*/std::nullopt, external_path,
            GlobalIndexMeta(range.from, range.to, field_id, extra_field_ids, io_meta.metadata)));
    }
    DataIncrement data_increment(std::move(index_file_metas));
    return std::make_shared<CommitMessageImpl>(partition, bucket,
                                               /*total_buckets=*/std::nullopt, data_increment,
                                               CompactIncrement({}, {}, {}));
}
}  // namespace
Result<std::shared_ptr<CommitMessage>> GlobalIndexWriteTask::WriteIndex(
    const std::string& table_path, const std::string& field_name, const std::string& index_type,
    const std::shared_ptr<IndexedSplit>& indexed_split,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<FileSystem>& file_system) {
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
    if (!data_split) {
        return Status::Invalid("split cannot be casted to data split");
    }
    const auto& ranges = indexed_split->RowRanges();
    if (ranges.size() != 1) {
        return Status::Invalid("GlobalIndexWriteTask only supports a single contiguous range.");
    }
    const auto& range = ranges[0];
    std::shared_ptr<MemoryPool> pool = memory_pool ? memory_pool : GetDefaultPool();

    // load schema
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options, CoreOptions::FromMap(options, file_system));
    SchemaManager schema_manager(tmp_options.GetFileSystem(), table_path);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_table_schema,
                           schema_manager.Latest());
    if (latest_table_schema == std::nullopt) {
        return Status::Invalid("not found latest schema");
    }
    // merge options
    const auto& table_schema = latest_table_schema.value();
    auto final_options = table_schema->Options();
    for (const auto& [key, value] : options) {
        final_options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(final_options, file_system));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                           CreateGlobalIndexer(index_type, core_options));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::vector<std::string>> extra_field_names,
                           indexer->GetExtraFieldNames());
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(field_name));
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> extra_fields,
                           GetExtraFields(*table_schema, field_name,
                                          extra_field_names.value_or(std::vector<std::string>())));
    std::optional<std::vector<int32_t>> extra_field_ids = GetExtraFieldIds(extra_fields);
    std::vector<std::string> writer_field_names = BuildWriterFieldNames(field_name, extra_fields);
    std::vector<std::string> read_field_names = BuildReadFieldNames(field_name, extra_fields);

    // create index file manager
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<GlobalIndexFileManager> index_file_manager,
        CreateGlobalIndexFileManager(table_path, table_schema, core_options, pool));

    // create batch reader
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BatchReader> batch_reader,
        CreateBatchReader(table_path, read_field_names, indexed_split, core_options, pool));

    // create global index writer
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<GlobalIndexWriter> global_index_writer,
        CreateGlobalIndexWriter(*indexer, field, extra_fields, index_file_manager, pool));

    ScopeGuard guard([&]() {
        global_index_writer.reset();
        batch_reader.reset();
    });

    // read from data split and write to index writer
    PAIMON_ASSIGN_OR_RAISE(std::vector<GlobalIndexIOMeta> global_index_io_metas,
                           BuildIndex(field_name, range, writer_field_names, batch_reader.get(),
                                      global_index_writer.get()));

    // generate commit message
    return ToCommitMessage(index_type, field.Id(), range, global_index_io_metas,
                           data_split->Partition(), data_split->Bucket(), index_file_manager,
                           extra_field_ids);
}

}  // namespace paimon
