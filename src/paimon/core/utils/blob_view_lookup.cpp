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

#include "paimon/core/utils/blob_view_lookup.h"

#include <algorithm>
#include <future>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/bytes.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/utils/special_field_ids.h"

namespace paimon {
BlobViewLookup::TableReadPlan::TableReadPlan(const BlobViewStruct& view_struct)
    : identifier_(view_struct.GetIdentifier()) {
    references_by_field_id_.insert(view_struct.FieldId());
    row_ranges_.push_back(view_struct.RowId());
}

void BlobViewLookup::TableReadPlan::Add(const BlobViewStruct& view_struct) {
    references_by_field_id_.insert(view_struct.FieldId());
    row_ranges_.push_back(view_struct.RowId());
}

const Identifier& BlobViewLookup::TableReadPlan::GetIdentifier() const {
    return identifier_;
}

std::vector<int32_t> BlobViewLookup::TableReadPlan::GetFieldIds() const {
    return std::vector<int32_t>(references_by_field_id_.begin(), references_by_field_id_.end());
}

std::vector<Range> BlobViewLookup::TableReadPlan::GetSortedDistinctRanges() const {
    if (row_ranges_.empty()) {
        return {};
    }
    std::vector<int64_t> sorted = row_ranges_;
    std::sort(sorted.begin(), sorted.end());
    std::vector<Range> ranges;
    int64_t range_start = sorted[0];
    int64_t range_end = range_start;
    for (size_t i = 1; i < sorted.size(); ++i) {
        const int64_t row_id = sorted[i];
        if (row_id == range_end) {
            continue;
        }
        if (row_id != range_end + 1) {
            ranges.emplace_back(range_start, range_end);
            range_start = row_id;
        }
        range_end = row_id;
    }
    ranges.emplace_back(range_start, range_end);
    return ranges;
}

Result<BlobViewResolver> BlobViewLookup::CreateResolver(
    const std::unordered_set<BlobViewStruct>& view_structs,
    const std::shared_ptr<CatalogContext>& catalog_context, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor) {
    PAIMON_ASSIGN_OR_RAISE(DescriptorMapping mapping,
                           PreloadDescriptors(view_structs, catalog_context, pool, executor));
    return BlobViewResolver([cached = std::move(mapping)](const BlobViewStruct& view_struct)
                                -> Result<std::shared_ptr<Bytes>> {
        auto iter = cached.find(view_struct);
        if (iter == cached.end()) {
            return Status::Invalid(fmt::format("BlobViewStruct not found in preloaded cache: {}",
                                               view_struct.ToString()));
        }
        return iter->second;
    });
}

Result<BlobViewLookup::DescriptorMapping> BlobViewLookup::PreloadDescriptors(
    const std::unordered_set<BlobViewStruct>& view_structs,
    const std::shared_ptr<CatalogContext>& catalog_context, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor) {
    std::unordered_map<Identifier, BlobViewLookup::TableReadPlan> plan_by_identifier =
        GroupByIdentifier(view_structs);
    int64_t target_rows_per_task = TargetRowsPerTask(plan_by_identifier, executor->GetThreadNum());

    std::vector<std::future<Result<DescriptorMapping>>> futures;
    for (const auto& [identifier, table_read_plan] : plan_by_identifier) {
        const auto& id = identifier;
        std::vector<int32_t> field_ids = table_read_plan.GetFieldIds();
        std::vector<std::vector<Range>> range_chunks =
            SplitRowRanges(table_read_plan.GetSortedDistinctRanges(), target_rows_per_task);
        for (const auto& range_chunk : range_chunks) {
            futures.push_back(Via(
                executor.get(),
                [catalog_context, id, field_ids, range_chunk, pool]() -> Result<DescriptorMapping> {
                    return LoadTableDescriptorChunk(catalog_context, id, field_ids, range_chunk,
                                                    pool);
                }));
        }
    }

    DescriptorMapping mapping;
    std::vector<Result<DescriptorMapping>> chunk_results = CollectAll(futures);
    for (auto& chunk_result : chunk_results) {
        if (!chunk_result.ok()) {
            return chunk_result.status();
        }
        for (const auto& [view_struct, descriptor] : chunk_result.value()) {
            mapping[view_struct] = descriptor;
        }
    }
    return mapping;
}

Result<BlobViewLookup::DescriptorMapping> BlobViewLookup::LoadTableDescriptorChunk(
    const std::shared_ptr<CatalogContext>& catalog_context, const Identifier& identifier,
    const std::vector<int32_t>& field_ids, const std::vector<Range>& row_ranges,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> branch, identifier.GetBranchName());
    if (branch) {
        return Status::Invalid("do not support upstream table with branch");
    }
    auto file_system = catalog_context->file_system;
    PAIMON_ASSIGN_OR_RAISE(std::string table_path, GetTableLocation(catalog_context, identifier));
    ScanContextBuilder scan_builder(table_path);
    auto global_index_result = BitmapGlobalIndexResult::FromRanges(row_ranges);
    scan_builder.SetGlobalIndexResult(global_index_result)
        .WithMemoryPool(pool)
        .WithFileSystem(file_system);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> table_scan,
                           TableScan::Create(std::move(scan_context)));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, table_scan->CreatePlan());

    ReadContextBuilder read_builder(table_path);
    std::vector<int32_t> read_field_ids = field_ids;
    read_field_ids.push_back(SpecialFieldIds::ROW_ID);
    read_builder.SetReadFieldIds(read_field_ids)
        .AddOption(Options::BLOB_AS_DESCRIPTOR, "true")
        .EnablePrefetch(true)
        .WithMemoryPool(pool)
        .WithFileSystem(file_system);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           TableRead::Create(std::move(read_context)));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                           table_read->CreateReader(plan->Splits()));

    DescriptorMapping mapping;
    PAIMON_RETURN_NOT_OK(
        ExtractBlobDescriptors(identifier, read_field_ids, pool, reader.get(), &mapping));
    return mapping;
}

Result<std::string> BlobViewLookup::GetTableLocation(
    const std::shared_ptr<CatalogContext>& catalog_context, const Identifier& identifier) {
    auto file_system = catalog_context->file_system;
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<Catalog> catalog,
        Catalog::Create(catalog_context->root_path, catalog_context->options, file_system));
    // The table path may be either xxx/test_database/test_table or
    // xxx/test_database.db/test_table. If neither path exists or both paths exist, it means we
    // cannot infer the table path, and an error will be reported. If only one of the paths
    // exists, we will use that path.
    PAIMON_ASSIGN_OR_RAISE(std::string source_table_path, catalog->GetTableLocation(identifier));
    std::string database_name = identifier.GetDatabaseName();
    PAIMON_ASSIGN_OR_RAISE(std::string data_table_name, identifier.GetDataTableName());
    std::string fallback_source_table_path = PathUtil::JoinPath(
        PathUtil::JoinPath(catalog_context->root_path, database_name), data_table_name);

    PAIMON_ASSIGN_OR_RAISE(bool exist, catalog_context->file_system->Exists(source_table_path));
    PAIMON_ASSIGN_OR_RAISE(bool fallback_exist, file_system->Exists(fallback_source_table_path));
    if (exist == fallback_exist) {
        return Status::Invalid(
            fmt::format("Ambiguous table path: both table path {} and fallback table path {} are "
                        "present or absent",
                        source_table_path, fallback_source_table_path));
    }
    std::string final_table_path = exist ? source_table_path : fallback_source_table_path;
    return final_table_path;
}

Status BlobViewLookup::ExtractBlobDescriptors(const Identifier& identifier,
                                              const std::vector<int32_t>& field_ids,
                                              const std::shared_ptr<MemoryPool>& pool,
                                              BatchReader* reader, DescriptorMapping* mapping) {
    if (reader == nullptr) {
        return Status::Invalid("invalid reader in ExtractBlobDescriptors, reader is nullptr");
    }
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
        if (struct_array == nullptr) {
            return Status::Invalid(
                "invalid array in ExtractBlobDescriptors, batch array is not a StructArray.");
        }
        // skip the _VALUE_KIND column
        if (static_cast<size_t>(struct_array->num_fields()) - 1 != field_ids.size()) {
            return Status::Invalid(
                fmt::format("invalid array in ExtractBlobDescriptors, batch array fields(exclude "
                            "_VALUE_KIND) {} mismatch read field ids {}.",
                            struct_array->num_fields() - 1, field_ids.size()));
        }
        // get _VALUE_KIND
        if (struct_array->struct_type()->field(0)->name() != SpecialFields::ValueKind().Name()) {
            return Status::Invalid(
                "invalid array in ExtractBlobDescriptors, expected _VALUE_KIND as the first "
                "column");
        }

        // get _ROW_ID
        if (struct_array->struct_type()->field(struct_array->num_fields() - 1)->name() !=
            SpecialFields::RowId().Name()) {
            return Status::Invalid(
                "invalid array in ExtractBlobDescriptors, expected _ROW_ID as the last column");
        }
        auto row_id_array = struct_array->field(struct_array->num_fields() - 1);
        auto typed_row_id_array = std::dynamic_pointer_cast<arrow::Int64Array>(row_id_array);
        if (!typed_row_id_array) {
            return Status::Invalid(
                fmt::format("invalid array does not contain {} field, or it cannot be casted to "
                            "Int64Array in ExtractBlobDescriptors.",
                            SpecialFields::RowId().Name()));
        }

        // skip _VALUE_KIND
        for (int32_t idx = 1; idx < struct_array->num_fields() - 1; ++idx) {
            auto binary_array =
                std::dynamic_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(idx));
            if (binary_array == nullptr) {
                return Status::Invalid(
                    "invalid array in ExtractBlobDescriptors, column is not a LargeBinaryArray.");
            }
            for (int64_t row = 0; row < binary_array->length(); ++row) {
                BlobViewStruct blob_view_struct(identifier, field_ids[idx - 1],
                                                typed_row_id_array->Value(row));
                if (binary_array->IsNull(row)) {
                    // null in source table
                    (*mapping)[blob_view_struct] = nullptr;
                    continue;
                }
                std::string_view bytes = binary_array->GetView(row);
                PAIMON_ASSIGN_OR_RAISE(bool is_descriptor, BlobDescriptor::IsBlobDescriptor(
                                                               bytes.data(), bytes.size()));
                if (!is_descriptor) {
                    return Status::Invalid(
                        "requires blob field value to be a serialized BlobDescriptor in source "
                        "table.");
                }
                auto descriptor_bytes = std::make_shared<Bytes>(bytes.size(), pool.get());
                std::memcpy(descriptor_bytes->data(), bytes.data(), bytes.size());
                (*mapping)[blob_view_struct] = std::move(descriptor_bytes);
            }
        }
    }
    return Status::OK();
}

std::unordered_map<Identifier, BlobViewLookup::TableReadPlan> BlobViewLookup::GroupByIdentifier(
    const std::unordered_set<BlobViewStruct>& view_structs) {
    std::unordered_map<Identifier, BlobViewLookup::TableReadPlan> grouped;
    for (const auto& view_struct : view_structs) {
        auto identifier = view_struct.GetIdentifier();
        auto iter = grouped.find(identifier);
        if (iter != grouped.end()) {
            iter->second.Add(view_struct);
        } else {
            grouped.emplace(identifier, BlobViewLookup::TableReadPlan(view_struct));
        }
    }
    return grouped;
}

int64_t BlobViewLookup::TargetRowsPerTask(
    const std::unordered_map<Identifier, TableReadPlan>& plan_by_identifier, uint32_t thread_num) {
    int64_t total_rows = 0;
    for (const auto& [identifier, table_read_plan] : plan_by_identifier) {
        for (const auto& row_range : table_read_plan.GetSortedDistinctRanges()) {
            total_rows += row_range.Count();
        }
    }
    int64_t balanced_rows = (total_rows + thread_num - 1) / thread_num;
    return std::max(MIN_ROW_PER_TASK, balanced_rows);
}

std::vector<std::vector<Range>> BlobViewLookup::SplitRowRanges(const std::vector<Range>& row_ranges,
                                                               int64_t target_rows_per_task) {
    if (row_ranges.empty()) {
        return {};
    }
    std::vector<std::vector<Range>> chunks;
    std::vector<Range> current_chunk;
    int64_t current_chunk_rows = 0;
    for (const auto& row_range : row_ranges) {
        int64_t next_from = row_range.from;
        while (next_from <= row_range.to) {
            if (current_chunk_rows == target_rows_per_task) {
                chunks.push_back(current_chunk);
                current_chunk.clear();
                current_chunk_rows = 0;
            }

            int64_t remaining_rows = target_rows_per_task - current_chunk_rows;
            int64_t next_to = std::min(row_range.to, next_from + remaining_rows - 1);
            current_chunk.emplace_back(next_from, next_to);
            current_chunk_rows += next_to - next_from + 1;
            next_from = next_to + 1;
        }
    }
    if (!current_chunk.empty()) {
        chunks.push_back(current_chunk);
    }
    return chunks;
}

}  // namespace paimon
