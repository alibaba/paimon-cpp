/*
 * Copyright 2025-present Alibaba Inc.
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

#include "paimon/core/operation/count_split_read.h"

#include <utility>

#include "arrow/type.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/mergetree/compact/interval_partition.h"
#include "paimon/core/mergetree/row_count_accumulator.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/merge_file_split_read.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

// =============================================================================
// Construction
// =============================================================================

CountSplitRead::CountSplitRead(const std::shared_ptr<InternalReadContext>& context,
                               std::unique_ptr<MergeFileSplitRead>&& merge_read,
                               const std::shared_ptr<MemoryPool>& memory_pool)
    : context_(context),
      merge_read_(std::move(merge_read)),
      pool_(memory_pool) {}

Result<std::unique_ptr<CountSplitRead>> CountSplitRead::Create(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<Executor>& executor) {
    // Build a minimal read schema for COUNT(*).
    // Only includes PK columns.
    // _SEQUENCE_NUMBER, _VALUE_KIND, and sequence-group fields are auto-completed
    // by GenerateKeyValueReadSchema inside MergeFileSplitRead::Create.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> count_read_schema,
                           BuildCountReadSchema(*context));

    // Create a new InternalReadContext with the minimal schema.
    // This ensures that MergeFileSplitRead only reads
    // the minimum required columns from Parquet files.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> count_context,
                           InternalReadContext::CreateWithSchema(context, count_read_schema));

    // Create MergeFileSplitRead for MOR mode (Level 1) with minimal schema.
    // Internally, GenerateKeyValueReadSchema will:
    //   Input:  count_read_schema = [pk_col]
    //   Output: read_schema = [_SEQUENCE_NUMBER, _VALUE_KIND, pk_col, (seq_field)]
    // This is the actual schema used to read Parquet files — only 3~4 columns instead of 100.
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeFileSplitRead> merge_read,
                           MergeFileSplitRead::Create(path_factory, count_context, memory_pool,
                                                      executor));

    return std::unique_ptr<CountSplitRead>(
        new CountSplitRead(count_context, std::move(merge_read), memory_pool));
}

// =============================================================================
// Main Entry Point
// =============================================================================

Result<int64_t> CountSplitRead::CountRows(const std::shared_ptr<DataSplitImpl>& split) {
    // Empty split: return 0 immediately
    if (split->DataFiles().empty()) {
        return 0;
    }

    if (split->RawConvertible()) {
        // DV mode: files are independent, no merge needed
        // Level 0: Pure metadata count (zero IO)
        return MetadataCount(split);
    } else {
        // MOR mode: must merge across files
        // Level 1: Merge + count
        return MergeCount(split);
    }
}

// =============================================================================
// Level 0: Pure Metadata Count
// =============================================================================

Result<int64_t> CountSplitRead::MetadataCount(const std::shared_ptr<DataSplitImpl>& split) {
    int64_t count = split->PartialMergedRowCount();
    if (count > 0) {
        // Successfully computed from metadata alone
        return count;
    }

    // PartialMergedRowCount returns 0 when DV cardinality is not available.
    // This happens with old-format files that lack the cardinality field.
    // Fallback to MergeCount which handles this case correctly.
    return MergeCount(split);
}

// =============================================================================
// Level 1: MOR Mode Count
// =============================================================================

Result<int64_t> CountSplitRead::MergeCount(const std::shared_ptr<DataSplitImpl>& split) {
    // Create DataFilePathFactory for this split's partition/bucket
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<DataFilePathFactory> data_file_path_factory,
        merge_read_->GetPathFactory()->CreateDataFilePathFactory(
            split->Partition(), split->Bucket()));

    // Build deletion file map from split's data files and deletion files
    std::unordered_map<std::string, DeletionFile> deletion_file_map;
    const auto& data_files = split->DataFiles();
    const auto& deletion_files = split->DeletionFiles();
    if (!deletion_files.empty()) {
        for (size_t i = 0; i < deletion_files.size(); i++) {
            if (deletion_files[i] != std::nullopt) {
                deletion_file_map.emplace(data_files[i]->file_name, deletion_files[i].value());
            }
        }
    }

    // Create DV factory from deletion file map
    auto dv_factory = DeletionVector::CreateFactory(
        context_->GetCoreOptions().GetFileSystem(),
        deletion_file_map, pool_);

    // Partition files into non-overlapping sections
    std::vector<std::vector<SortedRun>> sections =
        IntervalPartition(split->DataFiles(), merge_read_->GetKeyComparator()).Partition();

    int64_t total_count = 0;

    for (const auto& section : sections) {
        // Create SortMergeReader with DropDelete for this section.
        // The merged_reader internally handles:
        //   1. Reads data files (column-pruned via read_schema)
        //   2. Converts Arrow columns to KV objects (KVDataFileRecordReader)
        //   3. Multi-way merge sort by PK
        //   4. Calls MergeFunction to determine final version and KIND
        //   5. DropDeleteReader filters out rows with KIND=DELETE
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<SortMergeReader> merged_reader,
            merge_read_->CreateSortMergeReaderForSection(
                section, split->Partition(), dv_factory,
                /*predicate=*/nullptr,
                data_file_path_factory,
                /*drop_delete=*/true));

        // Use RowCountAccumulator to iterate and count
        RowCountAccumulator accumulator(std::move(merged_reader));

        PAIMON_ASSIGN_OR_RAISE(int64_t section_count, accumulator.CountAll());
        total_count += section_count;

        accumulator.Close();
    }

    return total_count;
}

// =============================================================================
// Schema Construction — Column Pruning for COUNT(*)
// =============================================================================

Result<std::shared_ptr<arrow::Schema>> CountSplitRead::BuildCountReadSchema(
    const InternalReadContext& context) {
    const auto& table_schema = context.GetTableSchema();

    // Only include PK columns (required for merge sort / deduplication)
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> pk_fields,
                           table_schema->TrimmedPrimaryKeyFields());

    std::vector<DataField> result_fields;
    for (const auto& field : pk_fields) {
        result_fields.push_back(field);
    }

    // Note: The following are automatically handled by GenerateKeyValueReadSchema:
    //   - _SEQUENCE_NUMBER and _VALUE_KIND (always prepended)
    //   - Sequence-group fields (via CompleteSequenceField)
    //   - User-defined sequence fields (via GetSequenceField)
    //   So we do NOT need to add them here.

    // Convert to Arrow Schema — this becomes the "raw_read_schema" for MergeFileSplitRead
    return DataField::ConvertDataFieldsToArrowSchema(result_fields);
}

}  // namespace paimon
