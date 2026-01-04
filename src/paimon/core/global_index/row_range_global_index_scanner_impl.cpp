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

#include "paimon/core/global_index/row_range_global_index_scanner_impl.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/global_index_evaluator_impl.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
namespace paimon {
RowRangeGlobalIndexScannerImpl::RowRangeGlobalIndexScannerImpl(
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<IndexPathFactory>& path_factory,
    const RowRangeGlobalIndexScannerImpl::IndexManifestEntryGroup& grouped_entries,
    const CoreOptions& options, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      table_schema_(table_schema),
      options_(options),
      grouped_entries_(grouped_entries),
      index_file_manager_(
          std::make_shared<GlobalIndexFileManager>(options.GetFileSystem(), path_factory)) {}

Result<std::shared_ptr<GlobalIndexEvaluator>> RowRangeGlobalIndexScannerImpl::CreateIndexEvaluator()
    const {
    GlobalIndexEvaluatorImpl::IndexReadersCreator create_index_readers =
        [scanner = shared_from_this()](
            int32_t field_id) -> Result<std::vector<std::shared_ptr<GlobalIndexReader>>> {
        return scanner->CreateReaders(field_id);
    };
    return std::make_shared<GlobalIndexEvaluatorImpl>(table_schema_, create_index_readers);
}

Result<std::shared_ptr<GlobalIndexReader>> RowRangeGlobalIndexScannerImpl::CreateReader(
    const std::string& field_name, const std::string& index_type) const {
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(field_name));
    auto field_iter = grouped_entries_.find(field.Id());
    if (field_iter == grouped_entries_.end()) {
        return std::shared_ptr<GlobalIndexReader>();
    }
    const auto& index_type_to_entries = field_iter->second;
    auto entry_iter = index_type_to_entries.find(index_type);
    if (entry_iter == index_type_to_entries.end()) {
        return std::shared_ptr<GlobalIndexReader>();
    }
    const auto& entries = entry_iter->second;
    return CreateReader(field, index_type, entries);
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>>
RowRangeGlobalIndexScannerImpl::CreateReaders(const std::string& field_name) const {
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(field_name));
    return CreateReaders(field);
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>>
RowRangeGlobalIndexScannerImpl::CreateReaders(int32_t field_id) const {
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(field_id));
    return CreateReaders(field);
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>>
RowRangeGlobalIndexScannerImpl::CreateReaders(const DataField& field) const {
    auto field_iter = grouped_entries_.find(field.Id());
    if (field_iter == grouped_entries_.end()) {
        return std::vector<std::shared_ptr<GlobalIndexReader>>();
    }
    const auto& index_type_to_entries = field_iter->second;
    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    readers.reserve(index_type_to_entries.size());
    for (const auto& [index_type, entries] : index_type_to_entries) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexReader> reader,
                               CreateReader(field, index_type, entries));
        if (reader) {
            readers.push_back(std::move(reader));
        }
    }
    return readers;
}

Result<std::shared_ptr<GlobalIndexReader>> RowRangeGlobalIndexScannerImpl::CreateReader(
    const DataField& field, const std::string& index_type,
    const std::vector<IndexManifestEntry>& entries) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                           GlobalIndexerFactory::Get(index_type, options_.ToMap()));
    if (!indexer) {
        return std::shared_ptr<GlobalIndexReader>();
    }
    // TODO(xinyu.lxy): c_arrow_schema may contains additional associated fields.
    auto arrow_field = DataField::ConvertDataFieldToArrowField(field);
    auto arrow_schema = arrow::schema({arrow_field});

    ArrowSchema c_arrow_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
    auto index_io_metas = ToGlobalIndexIOMetas(entries);
    ScopeGuard guard([&]() { ArrowSchemaRelease(&c_arrow_schema); });
    return indexer->CreateReader(&c_arrow_schema, index_file_manager_, index_io_metas, pool_);
}

std::vector<GlobalIndexIOMeta> RowRangeGlobalIndexScannerImpl::ToGlobalIndexIOMetas(
    const std::vector<IndexManifestEntry>& entries) {
    std::vector<GlobalIndexIOMeta> index_io_metas;
    index_io_metas.reserve(entries.size());
    for (const auto& entry : entries) {
        index_io_metas.push_back(ToGlobalIndexIOMeta(entry));
    }
    return index_io_metas;
}

GlobalIndexIOMeta RowRangeGlobalIndexScannerImpl::ToGlobalIndexIOMeta(
    const IndexManifestEntry& entry) {
    const auto& index_file = entry.index_file;
    assert(index_file->GetGlobalIndexMeta());
    const auto& global_index_meta = index_file->GetGlobalIndexMeta().value();
    return {index_file->FileName(), index_file->FileSize(),
            /*range_end=*/global_index_meta.row_range_end - global_index_meta.row_range_start,
            global_index_meta.index_meta};
}

}  // namespace paimon
