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

#include "paimon/core/operation/data_evolution_file_store_scan.h"

#include <map>
#include <string>

#include "paimon/common/data/blob_utils.h"
#include "paimon/common/reader/data_evolution_array.h"
#include "paimon/common/reader/data_evolution_row.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/range_helper.h"
namespace paimon {
Result<bool> DataEvolutionFileStoreScan::FilterEntryByRowRanges(
    const ManifestEntry& entry, const std::optional<RowRangeIndex>& row_range_index) {
    // If row range index is null, all entries should be kept
    if (!row_range_index) {
        return true;
    }
    // If firstRowId does not exist, keep the entry
    std::optional<int64_t> first_row_id = entry.File()->first_row_id;
    if (first_row_id == std::nullopt) {
        return true;
    }

    // Check if any value in indices is in the range [firstRowId, firstRowId + rowCount - 1]
    int64_t end_row_id = first_row_id.value() + entry.File()->row_count - 1;

    return row_range_index->Intersects(first_row_id.value(), end_row_id);
}

Result<bool> DataEvolutionFileStoreScan::FilterByStats(const ManifestEntry& entry) const {
    return FilterEntryByRowRanges(entry, row_range_index_);
}

Result<std::vector<ManifestEntry>> DataEvolutionFileStoreScan::PostFilterManifestEntries(
    std::vector<ManifestEntry>&& entries) const {
    if (!predicates_) {
        return std::move(entries);
    }
    // group by row id range
    RangeHelper<ManifestEntry> range_helper(
        [](const ManifestEntry& entry) -> Result<int64_t> {
            return entry.File()->NonNullFirstRowId();
        },
        [](const ManifestEntry& entry) -> Result<int64_t> {
            const auto& file_meta = entry.File();
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file_meta->NonNullFirstRowId());
            return first_row_id + file_meta->row_count - 1;
        });

    std::vector<ManifestEntry> result_entries;
    result_entries.reserve(entries.size());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<ManifestEntry>> split_by_row_id,
                           range_helper.MergeOverlappingRanges(std::move(entries)));

    for (auto& with_same_row_id : split_by_row_id) {
        PAIMON_ASSIGN_OR_RAISE(bool saved, FilterByStatsWithSameRowId(with_same_row_id));
        if (saved) {
            for (auto& entry : with_same_row_id) {
                result_entries.push_back(std::move(entry));
            }
        }
    }
    return result_entries;
}

Result<bool> DataEvolutionFileStoreScan::FilterByStatsWithSameRowId(
    const std::vector<ManifestEntry>& entries) const {
    if (entries.empty()) {
        return Status::Invalid(
            "DataEvolutionFileStoreScan FilterByStats must have at least one ManifestEntry");
    }
    // evolution stats from multiple entries with the same first row id, from data schema to
    // table schema, also deal with dense fields
    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [this](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == table_schema_->Id()) {
            return table_schema_;
        }
        return schema_manager_->ReadSchema(schema_id);
    };
    std::pair<int64_t, SimpleStatsEvolution::EvolutionStats> row_count_new_stats;
    PAIMON_ASSIGN_OR_RAISE(row_count_new_stats,
                           EvolutionStats(entries, table_schema_, schema_fetcher));
    const auto& [row_count, new_stats] = row_count_new_stats;

    // predicate tests evolution stats
    auto predicate_filter = std::dynamic_pointer_cast<PredicateFilter>(predicates_);
    if (!predicate_filter) {
        return Status::Invalid("cannot cast to predicate filter");
    }
    return predicate_filter->Test(schema_, row_count, *(new_stats.min_values),
                                  *(new_stats.max_values), *(new_stats.null_counts));
}

Result<std::pair<int64_t, SimpleStatsEvolution::EvolutionStats>>
DataEvolutionFileStoreScan::EvolutionStats(
    const std::vector<ManifestEntry>& old_entries, const std::shared_ptr<TableSchema>& table_schema,
    const std::function<Result<std::shared_ptr<TableSchema>>(int64_t)>& schema_fetcher) {
    // exclude blob files, useless for predicate eval
    std::vector<ManifestEntry> entries;
    entries.reserve(old_entries.size());
    for (const auto& entry : old_entries) {
        if (!BlobUtils::IsBlobFile(entry.File()->file_name)) {
            entries.push_back(entry);
        }
    }
    if (entries.empty()) {
        return Status::Invalid(
            "DataEvolutionFileStoreScan EvolutionStats: after exclude blob files, entries cannot "
            "be empty");
    }
    std::stable_sort(entries.begin(), entries.end(),
                     [](const ManifestEntry& e1, const ManifestEntry& e2) {
                         return e1.File()->max_sequence_number > e2.File()->max_sequence_number;
                     });

    // Init all we need to create a compound stats
    const auto& table_fields = table_schema->Fields();
    //  which row the read field index belongs to
    std::vector<int32_t> row_offsets(table_fields.size(), -1);
    // which field index in the reading row
    std::vector<int32_t> field_offsets(table_fields.size(), -1);

    for (int32_t entry_idx = 0; entry_idx < static_cast<int32_t>(entries.size()); entry_idx++) {
        const auto& file_meta = entries[entry_idx].File();
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> data_schema,
                               schema_fetcher(file_meta->schema_id));

        PAIMON_ASSIGN_OR_RAISE(
            std::vector<DataField> write_fields,
            DataField::ProjectFields(data_schema->Fields(), file_meta->write_cols));
        PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> stats_fields,
                               DataField::ProjectFields(write_fields, file_meta->value_stats_cols));
        // field name to stats idx
        std::map<std::string, int32_t> stats_field_map = ObjectUtils::CreateIdentifierToIndexMap(
            stats_fields, [](const DataField& field) -> std::string { return field.Name(); });

        for (int32_t table_field_idx = 0;
             table_field_idx < static_cast<int32_t>(table_fields.size()); table_field_idx++) {
            // -1 indicates that the table fields are not matched
            if (row_offsets[table_field_idx] != -1) {
                continue;
            }
            for (const auto& write_field : write_fields) {
                const auto& table_field = table_fields[table_field_idx];
                if (write_field.Id() == table_field.Id()) {
                    // indicates write field matches table field
                    // -2 indicates that the table fields will be matched to the current file.
                    row_offsets[table_field_idx] = -2;
                    auto stats_iter = stats_field_map.find(write_field.Name());
                    // if the fields are of the same type and contain statistics, then update
                    // row_offsets, otherwise evolved stats of current field is null
                    if (table_field.Type()->Equals(write_field.Type()) &&
                        stats_iter != stats_field_map.end()) {
                        row_offsets[table_field_idx] = entry_idx;
                        field_offsets[table_field_idx] = stats_iter->second;
                    }
                    break;
                }
            }
        }
    }
    std::vector<BinaryRow> min_rows;
    std::vector<BinaryRow> max_rows;
    std::vector<BinaryArray> null_counts;
    min_rows.reserve(entries.size());
    max_rows.reserve(entries.size());
    null_counts.reserve(entries.size());
    for (const auto& entry : entries) {
        const auto& stats = entry.File()->value_stats;
        min_rows.push_back(stats.MinValues());
        max_rows.push_back(stats.MaxValues());
        null_counts.push_back(stats.NullCounts());
    }

    auto final_min = std::make_shared<DataEvolutionRow>(min_rows, row_offsets, field_offsets);
    auto final_max = std::make_shared<DataEvolutionRow>(max_rows, row_offsets, field_offsets);
    auto final_null_counts =
        std::make_shared<DataEvolutionArray>(null_counts, row_offsets, field_offsets);
    return std::make_pair(
        entries[0].File()->row_count,
        SimpleStatsEvolution::EvolutionStats(final_min, final_max, final_null_counts));
}

}  // namespace paimon
