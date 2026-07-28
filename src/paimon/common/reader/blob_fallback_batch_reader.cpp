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

#include "paimon/common/reader/blob_fallback_batch_reader.h"

#include <algorithm>
#include <string_view>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

Result<std::unique_ptr<BlobFallbackBatchReader>> BlobFallbackBatchReader::Create(
    std::vector<std::vector<Segment>>&& sequence_groups,
    const std::shared_ptr<arrow::Schema>& read_schema, int32_t read_batch_size,
    const std::shared_ptr<MemoryPool>& pool) {
    if (sequence_groups.size() < 2) {
        return Status::Invalid(
            "Blob fallback needs at least two sequence groups; a single group should be read "
            "sequentially.");
    }
    if (read_schema == nullptr) {
        return Status::Invalid("Blob fallback read schema cannot be nullptr.");
    }
    if (read_batch_size <= 0) {
        return Status::Invalid(fmt::format(
            "Blob fallback read batch size '{}' should be larger than zero", read_batch_size));
    }
    int32_t blob_field_idx = -1;
    for (int32_t i = 0; i < read_schema->num_fields(); i++) {
        if (BlobUtils::IsBlobField(read_schema->field(i))) {
            if (blob_field_idx != -1) {
                return Status::Invalid(
                    "Blob fallback read schema should contain exactly one blob field.");
            }
            blob_field_idx = i;
        }
    }
    if (blob_field_idx == -1) {
        return Status::Invalid("Blob fallback read schema should contain a blob field.");
    }
    int32_t row_id_field_idx = read_schema->GetFieldIndex(SpecialFields::RowId().Name());
    int32_t seq_num_field_idx = read_schema->GetFieldIndex(SpecialFields::SequenceNumber().Name());
    std::vector<GroupCursor> groups;
    groups.reserve(sequence_groups.size());
    for (auto& segments : sequence_groups) {
        if (segments.empty()) {
            return Status::Invalid("Blob fallback sequence group should not be empty.");
        }
        for (const auto& segment : segments) {
            if (segment.reader == nullptr && segment.gap_selected_ranges.empty()) {
                return Status::Invalid(
                    "Blob fallback gap segment should cover at least one selected row id.");
            }
        }
        GroupCursor cursor;
        cursor.segments = std::move(segments);
        groups.push_back(std::move(cursor));
    }
    return std::unique_ptr<BlobFallbackBatchReader>(
        new BlobFallbackBatchReader(std::move(groups), read_schema, blob_field_idx,
                                    row_id_field_idx, seq_num_field_idx, read_batch_size, pool));
}

BlobFallbackBatchReader::BlobFallbackBatchReader(std::vector<GroupCursor>&& groups,
                                                 const std::shared_ptr<arrow::Schema>& read_schema,
                                                 int32_t blob_field_idx, int32_t row_id_field_idx,
                                                 int32_t seq_num_field_idx, int32_t read_batch_size,
                                                 const std::shared_ptr<MemoryPool>& pool)
    : groups_(std::move(groups)),
      read_schema_(read_schema),
      blob_field_idx_(blob_field_idx),
      row_id_field_idx_(row_id_field_idx),
      seq_num_field_idx_(seq_num_field_idx),
      read_batch_size_(read_batch_size),
      arrow_pool_(GetArrowPool(pool)) {}

Result<int64_t> BlobFallbackBatchReader::FillWindow(size_t group_idx, int64_t want,
                                                    std::vector<Chunk>* chunks) {
    GroupCursor& cursor = groups_[group_idx];
    int64_t collected = 0;
    while (collected < want) {
        if (!cursor.pending.empty()) {
            const std::shared_ptr<arrow::StructArray>& front = cursor.pending.front();
            int64_t available = front->length() - cursor.pending_pos;
            int64_t take = std::min(available, want - collected);
            chunks->push_back(Chunk{front, cursor.pending_pos, take, {}});
            cursor.pending_pos += take;
            collected += take;
            if (cursor.pending_pos == front->length()) {
                cursor.pending.pop_front();
                cursor.pending_pos = 0;
            }
            continue;
        }
        if (cursor.segment_idx >= cursor.segments.size()) {
            // group exhausted; only the first group may define a shorter window
            break;
        }
        Segment& segment = cursor.segments[cursor.segment_idx];
        if (segment.reader == nullptr) {
            // gap segment: all rows are placeholders, stepped range by range so the row ids
            // stay available for all-placeholder rows
            if (cursor.gap_range_idx >= segment.gap_selected_ranges.size()) {
                cursor.segment_idx++;
                cursor.gap_range_idx = 0;
                cursor.gap_range_pos = 0;
                continue;
            }
            const Range& range = segment.gap_selected_ranges[cursor.gap_range_idx];
            int64_t remaining = range.Count() - cursor.gap_range_pos;
            int64_t take = std::min(remaining, want - collected);
            Chunk chunk{nullptr, 0, take, {}};
            if (row_id_field_idx_ >= 0) {
                chunk.gap_row_ids.reserve(take);
                for (int64_t k = 0; k < take; k++) {
                    chunk.gap_row_ids.push_back(range.from + cursor.gap_range_pos + k);
                }
            }
            chunks->push_back(std::move(chunk));
            cursor.gap_range_pos += take;
            collected += take;
            if (cursor.gap_range_pos == range.Count()) {
                cursor.gap_range_idx++;
                cursor.gap_range_pos = 0;
            }
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch_with_bitmap,
                               segment.reader->NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(batch_with_bitmap)) {
            cursor.segment_idx++;
            continue;
        }
        auto& [read_batch, bitmap] = batch_with_bitmap;
        auto& [c_array, c_schema] = read_batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> src_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        PAIMON_ASSIGN_OR_RAISE(arrow::ArrayVector selected_array_vec,
                               ReaderUtils::GenerateFilteredArrayVector(src_array, bitmap));
        for (const auto& selected_array : selected_array_vec) {
            if (selected_array->length() == 0) {
                continue;
            }
            auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(selected_array);
            if (struct_array == nullptr) {
                return Status::Invalid("Blob fallback expects file readers to emit struct arrays.");
            }
            cursor.pending.push_back(std::move(struct_array));
        }
    }
    return collected;
}

Result<std::vector<bool>> BlobFallbackBatchReader::ComputePlaceholderFlags(
    const std::vector<Chunk>& chunks, int64_t row_count) const {
    std::vector<bool> flags(row_count, false);
    int64_t pos = 0;
    for (const auto& chunk : chunks) {
        if (chunk.array == nullptr) {
            // gap rows stand for placeholders
            std::fill(flags.begin() + pos, flags.begin() + pos + chunk.length, true);
        } else {
            std::shared_ptr<arrow::Array> blob_col = chunk.array->field(blob_field_idx_);
            auto binary_col = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(blob_col);
            if (binary_col == nullptr) {
                return Status::Invalid(fmt::format(
                    "Blob fallback expects the blob column to be large binary, but got {}",
                    blob_col->type()->ToString()));
            }
            for (int64_t k = 0; k < chunk.length; k++) {
                int64_t idx = chunk.offset + k;
                if (binary_col->IsNull(idx)) {
                    continue;
                }
                std::string_view value = binary_col->GetView(idx);
                if (BlobDefs::IsPlaceholderSentinel(value.data(), value.size())) {
                    flags[pos + k] = true;
                }
            }
        }
        pos += chunk.length;
    }
    return flags;
}

Result<std::shared_ptr<arrow::Array>> BlobFallbackBatchReader::AssembleRowIdRun(
    const std::vector<Chunk>& chunks, int64_t run_start, int64_t run_end) const {
    arrow::ArrayVector pieces;
    int64_t pos = 0;
    for (const auto& chunk : chunks) {
        int64_t overlap_start = std::max(run_start, pos);
        int64_t overlap_end = std::min(run_end, pos + chunk.length);
        if (overlap_start < overlap_end) {
            if (chunk.array != nullptr) {
                std::shared_ptr<arrow::Array> column = chunk.array->field(row_id_field_idx_);
                pieces.push_back(column->Slice(chunk.offset + (overlap_start - pos),
                                               overlap_end - overlap_start));
            } else {
                arrow::Int64Builder builder(arrow_pool_.get());
                for (int64_t r = overlap_start; r < overlap_end; r++) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(chunk.gap_row_ids[r - pos]));
                }
                std::shared_ptr<arrow::Array> piece;
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&piece));
                pieces.push_back(std::move(piece));
            }
        }
        pos += chunk.length;
        if (pos >= run_end) {
            break;
        }
    }
    if (pieces.size() == 1 && pieces[0]->offset() == 0) {
        return pieces[0];
    }
    // Concatenate flattens non-zero offsets left by Slice, so the exported batch honors the
    // zero-offset BatchReader contract.
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> concat_array,
                                      arrow::Concatenate(pieces, arrow_pool_.get()));
    return concat_array;
}

Result<std::shared_ptr<arrow::Array>> BlobFallbackBatchReader::AssembleColumn(
    int32_t field_idx, const std::vector<int32_t>& group_choice,
    const std::vector<std::vector<Chunk>>& group_chunks) const {
    const auto row_count = static_cast<int64_t>(group_choice.size());
    arrow::ArrayVector pieces;
    int64_t run_start = 0;
    while (run_start < row_count) {
        const int32_t group = group_choice[run_start];
        int64_t run_end = run_start + 1;
        while (run_end < row_count && group_choice[run_end] == group) {
            run_end++;
        }
        if (group < 0) {
            // placeholder in every layer: the blob degrades to null; the row keeps its row id
            // (taken from the newest group, which steps in lockstep), reports -1 as its
            // sequence number, and returns null for every other field
            if (field_idx == row_id_field_idx_) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> row_id_piece,
                                       AssembleRowIdRun(group_chunks[0], run_start, run_end));
                pieces.push_back(std::move(row_id_piece));
            } else if (field_idx == seq_num_field_idx_) {
                arrow::Int64Scalar seq_scalar(-1);
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> seq_piece,
                    arrow::MakeArrayFromScalar(seq_scalar, run_end - run_start, arrow_pool_.get()));
                pieces.push_back(std::move(seq_piece));
            } else {
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> null_piece,
                    arrow::MakeArrayOfNull(read_schema_->field(field_idx)->type(),
                                           run_end - run_start, arrow_pool_.get()));
                pieces.push_back(std::move(null_piece));
            }
        } else {
            int64_t pos = 0;
            for (const auto& chunk : group_chunks[group]) {
                int64_t overlap_start = std::max(run_start, pos);
                int64_t overlap_end = std::min(run_end, pos + chunk.length);
                if (overlap_start < overlap_end) {
                    if (chunk.array == nullptr) {
                        return Status::Invalid(
                            "Unexpected: a gap row was chosen as a blob fallback result.");
                    }
                    std::shared_ptr<arrow::Array> column = chunk.array->field(field_idx);
                    pieces.push_back(column->Slice(chunk.offset + (overlap_start - pos),
                                                   overlap_end - overlap_start));
                }
                pos += chunk.length;
                if (pos >= run_end) {
                    break;
                }
            }
        }
        run_start = run_end;
    }
    if (pieces.size() == 1 && pieces[0]->offset() == 0) {
        return pieces[0];
    }
    // Concatenate flattens non-zero offsets left by Slice, so the exported batch honors the
    // zero-offset BatchReader contract.
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> concat_array,
                                      arrow::Concatenate(pieces, arrow_pool_.get()));
    return concat_array;
}

Result<BatchReader::ReadBatch> BlobFallbackBatchReader::NextBatch() {
    if (closed_) {
        return Status::Invalid("blob fallback batch reader is closed");
    }
    std::vector<std::vector<Chunk>> group_chunks(groups_.size());
    // the first (newest) group defines the window; the others must step in lockstep
    PAIMON_ASSIGN_OR_RAISE(int64_t row_count, FillWindow(0, read_batch_size_, &group_chunks[0]));
    for (size_t g = 1; g < groups_.size(); g++) {
        // ask for one row even when the first group is exhausted, so that a longer group is
        // reported as a misalignment instead of silently truncating the read
        const int64_t want = std::max<int64_t>(row_count, 1);
        PAIMON_ASSIGN_OR_RAISE(int64_t got, FillWindow(g, want, &group_chunks[g]));
        if (got != row_count) {
            return Status::Invalid(fmt::format(
                "All sequence groups of a blob fallback read should have the same number of "
                "rows: group {} yielded {} rows in a window of {}",
                g, got, row_count));
        }
    }
    if (row_count == 0) {
        return BatchReader::MakeEofBatch();
    }

    std::vector<std::vector<bool>> placeholder_flags(groups_.size());
    for (size_t g = 0; g < groups_.size(); g++) {
        PAIMON_ASSIGN_OR_RAISE(placeholder_flags[g],
                               ComputePlaceholderFlags(group_chunks[g], row_count));
    }
    // per row, the first group in max-sequence order with a real entry wins; -1 means the row is
    // a placeholder in every group
    std::vector<int32_t> group_choice(row_count, -1);
    for (int64_t r = 0; r < row_count; r++) {
        for (size_t g = 0; g < groups_.size(); g++) {
            if (!placeholder_flags[g][r]) {
                group_choice[r] = static_cast<int32_t>(g);
                break;
            }
        }
    }

    arrow::ArrayVector columns;
    columns.reserve(read_schema_->num_fields());
    for (int32_t field_idx = 0; field_idx < read_schema_->num_fields(); field_idx++) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> column,
                               AssembleColumn(field_idx, group_choice, group_chunks));
        columns.push_back(std::move(column));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> target_array,
                                      arrow::StructArray::Make(columns, read_schema_->fields()));
    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*target_array, c_array.get(), c_schema.get()));
    return std::make_pair(std::move(c_array), std::move(c_schema));
}

Result<BatchReader::ReadBatchWithBitmap> BlobFallbackBatchReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, NextBatch());
    if (BatchReader::IsEofBatch(batch)) {
        return BatchReader::MakeEofBatchWithBitmap();
    }
    return ReaderUtils::AddAllValidBitmap(std::move(batch));
}

void BlobFallbackBatchReader::Close() {
    for (auto& group : groups_) {
        group.pending.clear();
        for (auto& segment : group.segments) {
            if (segment.reader) {
                segment.reader->Close();
            }
        }
    }
    closed_ = true;
}

std::shared_ptr<Metrics> BlobFallbackBatchReader::GetReaderMetrics() const {
    auto metrics = std::make_shared<MetricsImpl>();
    for (const auto& group : groups_) {
        for (const auto& segment : group.segments) {
            if (segment.reader) {
                auto reader_metrics = segment.reader->GetReaderMetrics();
                if (reader_metrics) {
                    metrics->Merge(reader_metrics);
                }
            }
        }
    }
    return metrics;
}

}  // namespace paimon
