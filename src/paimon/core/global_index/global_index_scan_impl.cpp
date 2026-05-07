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
#include "paimon/core/global_index/global_index_scan_impl.h"

#include <string>
#include <thread>
#include <utility>

#include "arrow/c/bridge.h"
#include "paimon/common/global_index/offset_global_index_reader.h"
#include "paimon/common/global_index/union_global_index_reader.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/global_index_evaluator_impl.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"

namespace paimon {
GlobalIndexScanImpl::GlobalIndexScanImpl(const std::shared_ptr<TableSchema>& table_schema,
                                         const CoreOptions& options,
                                         const std::shared_ptr<IndexPathFactory>& path_factory,
                                         IndexMetaMap&& index_metas,
                                         const std::shared_ptr<Executor>& executor,
                                         const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      table_schema_(table_schema),
      options_(options),
      index_file_manager_(
          std::make_shared<GlobalIndexFileManager>(options.GetFileSystem(), path_factory)),
      index_metas_(std::move(index_metas)),
      executor_(executor) {}

Result<std::unique_ptr<GlobalIndexScanImpl>> GlobalIndexScanImpl::Create(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const Snapshot& snapshot, const std::shared_ptr<PredicateFilter>& partitions,
    const CoreOptions& options, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool) {
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> file_store_path_factory,
        FileStorePathFactory::Create(
            root_path, arrow_schema, table_schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, options.IndexFileInDataFileDir(), pool));
    std::shared_ptr<IndexPathFactory> path_factory =
        file_store_path_factory->CreateGlobalIndexFileFactory();

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                                  options.GetManifestCompression(), file_store_path_factory,
                                  options.GetBucket(), pool, options));
    auto index_file_handler = std::make_unique<IndexFileHandler>(
        options.GetFileSystem(), std::move(index_manifest_file),
        std::make_shared<IndexFilePathFactories>(file_store_path_factory),
        options.DeletionVectorsBitmap64(), pool);

    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> partition_fields,
                           table_schema->GetFields(table_schema->PartitionKeys()));
    auto partition_schema = DataField::ConvertDataFieldsToArrowSchema(partition_fields);
    std::function<Result<bool>(const IndexManifestEntry&)> filter =
        [&](const IndexManifestEntry& entry) -> Result<bool> {
        if (partitions) {
            PAIMON_ASSIGN_OR_RAISE(bool saved, partitions->Test(partition_schema, entry.partition));
            if (!saved) {
                return false;
            }
        }
        if (!entry.index_file->GetGlobalIndexMeta()) {
            return false;
        }
        return true;
    };
    PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> entries,
                           index_file_handler->Scan(snapshot, filter));
    IndexMetaMap index_metas;
    for (const auto& entry : entries) {
        auto index_file_meta = entry.index_file;
        const auto& index_meta = index_file_meta->GetGlobalIndexMeta();
        assert(index_meta);
        Range range(index_meta->row_range_start, index_meta->row_range_end);
        index_metas[index_meta->index_field_id][index_file_meta->IndexType()][range].push_back(
            index_file_meta);
    }
    auto final_executor = executor;
    if (!final_executor) {
        std::optional<int32_t> thread_num = options.GetGlobalIndexThreadNum();
        if (!thread_num) {
            uint32_t cpu_count = std::thread::hardware_concurrency();
            thread_num = cpu_count > 0 ? static_cast<int32_t>(cpu_count) : 1;
        }
        final_executor = CreateDefaultExecutor(static_cast<uint32_t>(thread_num.value()));
    }
    return std::unique_ptr<GlobalIndexScanImpl>(new GlobalIndexScanImpl(
        table_schema, options, path_factory, std::move(index_metas), final_executor, pool));
}

Result<std::shared_ptr<GlobalIndexEvaluator>> GlobalIndexScanImpl::GetOrCreateIndexEvaluator() {
    if (evaluator_) {
        return evaluator_;
    }
    GlobalIndexEvaluatorImpl::IndexReadersCreator create_index_readers =
        [this](int32_t field_id) -> Result<std::vector<std::shared_ptr<GlobalIndexReader>>> {
        return CreateReaders(field_id, /*row_range_index=*/std::nullopt);
    };
    evaluator_ = std::make_shared<GlobalIndexEvaluatorImpl>(table_schema_, create_index_readers);
    return evaluator_;
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GlobalIndexScanImpl::CreateReaders(
    int32_t field_id, const std::optional<RowRangeIndex>& row_range_index) const {
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(field_id));
    return CreateReaders(field, row_range_index);
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GlobalIndexScanImpl::CreateReaders(
    const std::string& field_name, const std::optional<RowRangeIndex>& row_range_index) const {
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(field_name));
    return CreateReaders(field, row_range_index);
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GlobalIndexScanImpl::CreateReaders(
    const DataField& field, const std::optional<RowRangeIndex>& row_range_index) const {
    auto field_iter = index_metas_.find(field.Id());
    if (field_iter == index_metas_.end()) {
        return std::vector<std::shared_ptr<GlobalIndexReader>>();
    }
    const auto& index_type_to_metas = field_iter->second;
    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    readers.reserve(index_type_to_metas.size());
    for (const auto& [index_type, range_to_metas] : index_type_to_metas) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                               GlobalIndexerFactory::Get(index_type, options_.ToMap()));
        if (!indexer) {
            continue;
        }
        std::vector<std::shared_ptr<GlobalIndexReader>> union_readers;
        union_readers.reserve(range_to_metas.size());
        for (const auto& [range, metas] : range_to_metas) {
            if (row_range_index && !row_range_index->Intersects(range.from, range.to)) {
                continue;
            }
            // TODO(xinyu.lxy): c_arrow_schema may contains additional associated fields.
            auto arrow_field = DataField::ConvertDataFieldToArrowField(field);
            auto arrow_schema = arrow::schema({arrow_field});

            ArrowSchema c_arrow_schema;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
            auto index_io_metas = ToGlobalIndexIOMetas(metas);
            ScopeGuard guard([&]() { ArrowSchemaRelease(&c_arrow_schema); });
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<GlobalIndexReader> index_reader,
                indexer->CreateReader(&c_arrow_schema, index_file_manager_, index_io_metas, pool_));
            union_readers.push_back(
                std::make_shared<OffsetGlobalIndexReader>(std::move(index_reader), range.from));
        }
        if (union_readers.empty()) {
            continue;
        }
        readers.push_back(
            std::make_shared<UnionGlobalIndexReader>(std::move(union_readers), executor_));
    }
    return readers;
}

std::vector<GlobalIndexIOMeta> GlobalIndexScanImpl::ToGlobalIndexIOMetas(
    const std::vector<std::shared_ptr<IndexFileMeta>>& metas) const {
    std::vector<GlobalIndexIOMeta> index_io_metas;
    index_io_metas.reserve(metas.size());
    for (const auto& meta : metas) {
        index_io_metas.push_back(ToGlobalIndexIOMeta(meta));
    }
    return index_io_metas;
}

GlobalIndexIOMeta GlobalIndexScanImpl::ToGlobalIndexIOMeta(
    const std::shared_ptr<IndexFileMeta>& index_meta) const {
    assert(index_meta->GetGlobalIndexMeta());
    const auto& global_index_meta = index_meta->GetGlobalIndexMeta().value();
    return {index_file_manager_->ToPath(index_meta), index_meta->FileSize(),
            global_index_meta.index_meta};
}

Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexScanImpl::Scan(
    const std::shared_ptr<Predicate>& predicate) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexEvaluator> evaluator,
                           GetOrCreateIndexEvaluator());
    return evaluator->Evaluate(predicate);
}

}  // namespace paimon
