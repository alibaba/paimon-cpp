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

#include "arrow/c/bridge.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
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
            core_options.GetPartitionDefaultName(), core_options.GetWriteFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            pool));
    std::shared_ptr<IndexPathFactory> index_path_factory =
        path_factory->CreateGlobalIndexFileFactory();
    return std::make_shared<GlobalIndexFileManager>(core_options.GetFileSystem(),
                                                    index_path_factory);
}

Result<std::shared_ptr<GlobalIndexWriter>> CreateGlobalIndexWriter(
    const std::string& index_type, const DataField& field,
    const std::shared_ptr<GlobalIndexFileManager>& index_file_manager,
    const CoreOptions& core_options, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                           GlobalIndexerFactory::Get(index_type, core_options.ToMap()));
    if (!indexer) {
        return Status::Invalid(
            fmt::format("Unknown index type {}, may not registered", index_type));
    }
    // TODO(xinyu.lxy): may add additional fields to read for index write
    auto arrow_field = DataField::ConvertDataFieldToArrowField(field);
    auto arrow_schema = arrow::schema({arrow_field});
    ArrowSchema c_arrow_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
    return indexer->CreateWriter(field.Name(), &c_arrow_schema, index_file_manager, pool);
}

Result<std::unique_ptr<BatchReader>> CreateBatchReader(
    const std::string& table_path, const std::string& field_name,
    const std::shared_ptr<IndexedSplit>& indexed_split, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetOptions(core_options.ToMap())
        .EnablePrefetch(true)
        .WithMemoryPool(pool)
        .SetReadSchema({field_name});
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                           read_context_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           TableRead::Create(std::move(read_context)));
    return table_read->CreateReader(indexed_split);
}

Result<std::vector<GlobalIndexIOMeta>> BuildIndex(const std::string& field_name,
                                                  BatchReader* batch_reader,
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
            return Status::Invalid("array read from batch reader is not a struct array");
        }
        arrow::ArrayVector fields = struct_array->fields();
        fields.erase(fields.begin());
        if (fields.empty()) {
            return Status::Invalid("array read from batch reader only contains row kind");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> new_array,
                                          arrow::StructArray::Make(fields, {field_name}));
        ::ArrowArray c_new_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*new_array, &c_new_array));
        PAIMON_RETURN_NOT_OK(global_index_writer->AddBatch(&c_new_array));
    }
    return global_index_writer->Finish();
}

Result<std::shared_ptr<CommitMessage>> ToCommitMessage(
    const std::string& index_type, int32_t field_id, const Range& range,
    const std::vector<GlobalIndexIOMeta>& global_index_io_metas, const BinaryRow& partition,
    int32_t bucket, const std::shared_ptr<GlobalIndexFileManager>& file_manager) {
    std::vector<std::shared_ptr<IndexFileMeta>> index_file_metas;
    index_file_metas.reserve(global_index_io_metas.size());
    bool is_external_path = file_manager->IsExternalPath();
    for (const auto& io_meta : global_index_io_metas) {
        if (range.Count() != io_meta.range_end + 1) {
            return Status::Invalid(
                fmt::format("specified range length {} mismatch indexed range length {}",
                            range.Count(), io_meta.range_end + 1));
        }
        std::optional<std::string> external_path;
        if (is_external_path) {
            PAIMON_ASSIGN_OR_RAISE(Path path, PathUtil::ToPath(io_meta.file_path));
            external_path = path.ToString();
        }
        index_file_metas.push_back(std::make_shared<IndexFileMeta>(
            index_type, PathUtil::GetName(io_meta.file_path), io_meta.file_size,
            io_meta.range_end + 1, /*dv_ranges=*/std::nullopt, external_path,
            GlobalIndexMeta(range.from, io_meta.range_end + range.from, field_id,
                            /*extra_field_ids=*/std::nullopt, io_meta.metadata)));
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

    // create index file manager
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<GlobalIndexFileManager> index_file_manager,
        CreateGlobalIndexFileManager(table_path, table_schema, core_options, pool));

    // create global index writer
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(field_name));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<GlobalIndexWriter> global_index_writer,
        CreateGlobalIndexWriter(index_type, field, index_file_manager, core_options, pool));

    // create batch reader
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BatchReader> batch_reader,
        CreateBatchReader(table_path, field_name, indexed_split, core_options, pool));

    // read from data split and write to index writer
    PAIMON_ASSIGN_OR_RAISE(std::vector<GlobalIndexIOMeta> global_index_io_metas,
                           BuildIndex(field_name, batch_reader.get(), global_index_writer.get()));

    // generate commit message
    return ToCommitMessage(index_type, field.Id(), range, global_index_io_metas,
                           data_split->Partition(), data_split->Bucket(), index_file_manager);
}

}  // namespace paimon
