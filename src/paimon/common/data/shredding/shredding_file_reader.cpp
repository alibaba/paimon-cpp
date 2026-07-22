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

#include "paimon/common/data/shredding/shredding_file_reader.h"

#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

ShreddingFileReader::ShreddingFileReader(
    std::unique_ptr<FileBatchReader>&& reader,
    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>>&& plans,
    const std::shared_ptr<MemoryPool>& pool)
    : arrow_pool_(GetArrowPool(pool)), reader_(std::move(reader)), plans_(std::move(plans)) {}

Result<std::unique_ptr<::ArrowSchema>> ShreddingFileReader::GetFileSchema() const {
    return reader_->GetFileSchema();
}

Status ShreddingFileReader::SetReadSchema(::ArrowSchema* read_schema,
                                          const std::shared_ptr<Predicate>& predicate,
                                          const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!read_schema) {
        return Status::Invalid("invalid read schema in ShreddingFileReader, cannot be null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> logical_read_schema,
                                      arrow::ImportSchema(read_schema));
    arrow::FieldVector resolved_fields = logical_read_schema->fields();
    bool any_resolved = false;
    for (auto& resolved_field : resolved_fields) {
        auto it = plans_.find(resolved_field->name());
        if (it == plans_.end()) {
            continue;
        }
        // Push the physical (possibly pruned) subtree down so the inner reader materializes it.
        resolved_field = it->second->PhysicalField();
        any_resolved = true;
    }
    if (!any_resolved) {
        return Status::Invalid("no planned shredded columns exist in the read schema");
    }
    auto resolved_schema = arrow::schema(resolved_fields, logical_read_schema->metadata());
    auto c_resolved_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*resolved_schema, c_resolved_schema.get()));
    return reader_->SetReadSchema(c_resolved_schema.get(), predicate, selection_bitmap);
}

Result<BatchReader::ReadBatch> ShreddingFileReader::NextBatch() {
    return Status::Invalid(
        "paimon inner reader ShreddingFileReader should use NextBatchWithBitmap");
}

Result<BatchReader::ReadBatchWithBitmap> ShreddingFileReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           reader_->NextBatchWithBitmap());
    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
        return batch_with_bitmap;
    }

    auto& [batch, bitmap] = batch_with_bitmap;
    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    if (arrow_array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("cannot cast batch to StructArray in ShreddingFileReader");
    }
    auto struct_array = std::static_pointer_cast<arrow::StructArray>(arrow_array);

    arrow::ArrayVector resolved_arrays = struct_array->fields();
    arrow::FieldVector resolved_fields = struct_array->struct_type()->fields();
    for (int32_t field_idx = 0; field_idx < struct_array->num_fields(); ++field_idx) {
        const auto& physical_field = struct_array->struct_type()->field(field_idx);
        auto it = plans_.find(physical_field->name());
        if (it == plans_.end()) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Array> logical_array,
            it->second->Assemble(struct_array->field(field_idx), arrow_pool_.get()));
        resolved_arrays[field_idx] = logical_array;
        resolved_fields[field_idx] = it->second->LogicalField();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> new_struct_array,
                                      arrow::StructArray::Make(resolved_arrays, resolved_fields));
    auto new_c_array = std::make_unique<ArrowArray>();
    auto new_c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*new_struct_array, new_c_array.get(), new_c_schema.get()));
    batch = std::make_pair(std::move(new_c_array), std::move(new_c_schema));
    return batch_with_bitmap;
}

std::shared_ptr<Metrics> ShreddingFileReader::GetReaderMetrics() const {
    return reader_->GetReaderMetrics();
}

void ShreddingFileReader::Close() {
    reader_->Close();
}

Result<uint64_t> ShreddingFileReader::GetPreviousBatchFileRowId(uint64_t batch_row_id) const {
    return reader_->GetPreviousBatchFileRowId(batch_row_id);
}

Result<uint64_t> ShreddingFileReader::GetNumberOfRows() const {
    return reader_->GetNumberOfRows();
}

bool ShreddingFileReader::SupportPreciseBitmapSelection() const {
    return reader_->SupportPreciseBitmapSelection();
}

}  // namespace paimon
