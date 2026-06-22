/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/core/io/map_shared_shredding_core_utils.h"

#include <algorithm>
#include <set>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "paimon/common/data/shredding/map_shared_shredding_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {
namespace {

bool ContainsWriteColumn(const std::vector<std::string>& write_cols, const std::string& field) {
    return std::find(write_cols.begin(), write_cols.end(), field) != write_cols.end();
}

Result<std::shared_ptr<arrow::Schema>> ReadFileSchema(
    const std::shared_ptr<DataFileMeta>& file,
    const std::shared_ptr<DataFilePathFactory>& path_factory, const CoreOptions& options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::string format_str, file->FileFormat());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> format,
                           FileFormatFactory::Get(format_str, options.ToMap()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> reader_builder,
                           format->CreateReaderBuilder(options.GetReadBatchSize()));
    reader_builder->WithMemoryPool(pool);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                           options.GetFileSystem()->Open(path_factory->ToPath(file)));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                           reader_builder->Build(input_stream));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_file_schema, reader->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_schema,
                                      arrow::ImportSchema(c_file_schema.get()));
    return file_schema;
}

// Restores each shared-shredding field from the newest data file that carries its file metadata.
// In data evolution mode, the newest file may only contain a subset of write_cols, so restoring
// from files.back() alone can miss other shared-shredding fields. Use write_cols to avoid opening
// unrelated files, and fall back to Kmax for fields whose metadata cannot be found.
Status RestoreContextFromRecentFiles(const std::vector<std::shared_ptr<DataFileMeta>>& files,
                                     const std::shared_ptr<DataFilePathFactory>& path_factory,
                                     const CoreOptions& options,
                                     const std::shared_ptr<MemoryPool>& pool,
                                     MapSharedShreddingContext* context) {
    if (!context || files.empty()) {
        return Status::OK();
    }

    std::vector<std::string> shredding_fields = context->GetShreddingColumnNames();
    std::set<std::string> pending_fields(shredding_fields.begin(), shredding_fields.end());

    for (auto file_it = files.rbegin(); file_it != files.rend() && !pending_fields.empty();
         ++file_it) {
        const auto& file = *file_it;
        std::vector<std::string> candidate_fields;
        if (!file->write_cols) {
            candidate_fields.assign(pending_fields.begin(), pending_fields.end());
        } else {
            for (const auto& field : pending_fields) {
                if (ContainsWriteColumn(file->write_cols.value(), field)) {
                    candidate_fields.push_back(field);
                }
            }
        }
        if (candidate_fields.empty()) {
            continue;
        }

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> file_schema,
                               ReadFileSchema(file, path_factory, options, pool));
        for (const auto& field_name : candidate_fields) {
            std::shared_ptr<arrow::Field> field = file_schema->GetFieldByName(field_name);
            if (!field) {
                continue;
            }
            const auto& metadata = field->metadata();
            if (!metadata) {
                continue;
            }
            auto metadata_copy = metadata->Copy();
            if (!MapSharedShreddingUtils::HasShreddingMetadata(metadata_copy)) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(
                MapSharedShreddingFieldMeta field_meta,
                MapSharedShreddingUtils::DeserializeMetadata(
                    metadata_copy, MapSharedShreddingDefine::kDefaultDictCompression));
            context->ReportFileStats(field->name(), field_meta.max_row_width);
            pending_fields.erase(field_name);
        }
    }
    return Status::OK();
}

}  // namespace

Result<std::shared_ptr<MapSharedShreddingContext>>
MapSharedShreddingCoreUtils::CreateAndRestoreContext(
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::vector<std::shared_ptr<DataFileMeta>>& restore_files,
    const std::shared_ptr<DataFilePathFactory>& path_factory, const CoreOptions& options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MapSharedShreddingContext> context,
                           MapSharedShreddingUtils::CreateShreddingContext(write_schema, options));
    PAIMON_RETURN_NOT_OK(
        RestoreContextFromRecentFiles(restore_files, path_factory, options, pool, context.get()));
    return context;
}

}  // namespace paimon
