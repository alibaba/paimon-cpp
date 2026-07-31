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

#pragma once

#include <deque>
#include <memory>
#include <vector>

#include "arrow/type.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/range.h"

namespace arrow {
class Array;
class StructArray;
}  // namespace arrow

namespace paimon {

/// Merges the blob files of one data-evolution blob bunch that span multiple max sequence
/// number layers, resolving placeholder entries row by row. Aligned with Java's
/// BlobFallbackRecordReader / AllPlaceholdersRecordReader.
///
/// A data-evolution partial update rewrites only the touched rows of a blob column; the new blob
/// file records every untouched row as a placeholder entry. Reading therefore needs, per row, the
/// value from the newest layer that holds a real (non-placeholder) entry:
///
/// 1. The caller groups the blob files by max sequence number (one group per layer, newest
///    first) and, inside each group, orders them by first row id. Row id ranges the group's
///    files do not cover are represented by gap segments, which stand for all-placeholder rows.
/// 2. All groups span the same overall row id range, so with the same row-ranges selection
///    applied they yield the same number of rows and can be stepped in lockstep.
/// 3. Each output row takes the first group, in max-sequence order, whose row is not a
///    placeholder. Placeholder rows are identified by exact equality with the
///    BlobDefs::kPlaceholderSentinel bytes, emitted by the blob format reader when
///    BlobDefs::kEmitPlaceholderSentinelKey is set.
/// 4. A row that is a placeholder in every group degrades to a null blob: it keeps its
///    _ROW_ID, reports -1 as its _SEQUENCE_NUMBER, and returns null for every other field.
class BlobFallbackBatchReader : public BatchReader {
 public:
    /// One piece of a sequence group: either a reader over a single blob file (already wrapped
    /// with the usual per-file mapping readers and row selection), or a virtual gap standing for
    /// row ids the group's files do not cover. Segments must be ordered by ascending row id.
    struct Segment {
        /// File segment: emits the rows of one blob file. Null for a gap segment.
        std::unique_ptr<FileBatchReader> reader;
        /// Gap segment only: the selected row ids the gap emits (all placeholders), as sorted
        /// disjoint ranges. Must not be empty for a gap segment.
        std::vector<Range> gap_selected_ranges;
    };

    /// `sequence_groups` must be ordered by descending max sequence number and contain at least
    /// two groups (a single group needs no fallback). `read_schema` is the schema every file
    /// reader emits; it must contain exactly one blob field and may additionally contain the
    /// row-tracking fields _ROW_ID and _SEQUENCE_NUMBER (completed per file by
    /// CompleteRowTrackingFieldsBatchReader), which stay correct for rows that are a
    /// placeholder in every layer.
    static Result<std::unique_ptr<BlobFallbackBatchReader>> Create(
        std::vector<std::vector<Segment>>&& sequence_groups,
        const std::shared_ptr<arrow::Schema>& read_schema, int32_t read_batch_size,
        const std::shared_ptr<MemoryPool>& pool);

    Result<ReadBatch> NextBatch() override;

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    void Close() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

 private:
    /// A run of consecutive rows already fetched from one group: `array` rows
    /// [offset, offset + length) for a file segment, or `length` placeholder rows for a gap
    /// segment (array is null).
    struct Chunk {
        std::shared_ptr<arrow::StructArray> array;
        int64_t offset = 0;
        int64_t length = 0;
        /// Gap chunk only: the row id of each of the `length` rows, filled when the read
        /// schema contains _ROW_ID so all-placeholder rows can keep their row id.
        std::vector<int64_t> gap_row_ids;
    };

    /// Read progress of one sequence group. Move-only, matching Segment's unique_ptr member.
    struct GroupCursor {
        GroupCursor() = default;
        GroupCursor(const GroupCursor&) = delete;
        GroupCursor& operator=(const GroupCursor&) = delete;
        GroupCursor(GroupCursor&&) = default;
        GroupCursor& operator=(GroupCursor&&) = default;

        std::vector<Segment> segments;
        size_t segment_idx = 0;
        /// Position inside the current gap segment: index into gap_selected_ranges and the
        /// number of rows already emitted from that range.
        size_t gap_range_idx = 0;
        int64_t gap_range_pos = 0;
        /// Rows fetched from the current file segment but not yet consumed.
        std::deque<std::shared_ptr<arrow::StructArray>> pending;
        int64_t pending_pos = 0;
    };

    BlobFallbackBatchReader(std::vector<GroupCursor>&& groups,
                            const std::shared_ptr<arrow::Schema>& read_schema,
                            int32_t blob_field_idx, int32_t row_id_field_idx,
                            int32_t seq_num_field_idx, int32_t read_batch_size,
                            const std::shared_ptr<MemoryPool>& pool);

    /// Collects up to `want` rows from the group into chunks. Only the first group may come up
    /// short (which defines the window size); any later group ending early is a misalignment.
    Result<int64_t> FillWindow(size_t group_idx, int64_t want, std::vector<Chunk>* chunks);

    /// Flags each of the `row_count` window rows of the given chunks as placeholder or not.
    Result<std::vector<bool>> ComputePlaceholderFlags(const std::vector<Chunk>& chunks,
                                                      int64_t row_count) const;

    /// Assembles one output column by stitching, per run of rows choosing the same group,
    /// slices of that group's chunks. For rows choosing no group (placeholder in every layer),
    /// _ROW_ID is kept, _SEQUENCE_NUMBER becomes -1, and every other field becomes null.
    Result<std::shared_ptr<arrow::Array>> AssembleColumn(
        int32_t field_idx, const std::vector<int32_t>& group_choice,
        const std::vector<std::vector<Chunk>>& group_chunks) const;

    /// Assembles the _ROW_ID values of rows [run_start, run_end) from the given group's chunks;
    /// gap chunks contribute their synthesized row ids.
    Result<std::shared_ptr<arrow::Array>> AssembleRowIdRun(const std::vector<Chunk>& chunks,
                                                           int64_t run_start,
                                                           int64_t run_end) const;

    std::vector<GroupCursor> groups_;
    std::shared_ptr<arrow::Schema> read_schema_;
    const int32_t blob_field_idx_;
    /// Index of _ROW_ID / _SEQUENCE_NUMBER in the read schema, -1 when not read.
    const int32_t row_id_field_idx_;
    const int32_t seq_num_field_idx_;
    const int32_t read_batch_size_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    bool closed_ = false;
};

}  // namespace paimon
