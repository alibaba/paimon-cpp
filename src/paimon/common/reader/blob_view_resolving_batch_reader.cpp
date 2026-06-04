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

#include "paimon/common/reader/blob_view_resolving_batch_reader.h"

#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {
BlobViewResolvingBatchReader::BlobViewResolvingBatchReader(
    std::unique_ptr<BatchReader>&& reader, std::vector<std::string> read_blob_view_fields,
    BlobViewResolver resolver, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      arrow_pool_(GetArrowPool(pool)),
      reader_(std::move(reader)),
      read_blob_view_fields_(std::make_move_iterator(read_blob_view_fields.begin()),
                             std::make_move_iterator(read_blob_view_fields.end())),
      resolver_(std::move(resolver)) {}

Result<BatchReader::ReadBatch> BlobViewResolvingBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader_->NextBatch());
    if (BatchReader::IsEofBatch(batch)) {
        return batch;
    }
    if (read_blob_view_fields_.empty()) {
        return batch;
    }

    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
    if (struct_array == nullptr) {
        return Status::Invalid(
            "invalid batch, BlobViewResolvingBatchReader expects a StructArray batch.");
    }
    const auto struct_type = struct_array->struct_type();

    arrow::ArrayVector new_fields = struct_array->fields();
    std::vector<std::string> field_names;
    field_names.reserve(struct_type->num_fields());

    for (int32_t field_idx = 0; field_idx < struct_type->num_fields(); ++field_idx) {
        const auto& field = struct_type->field(field_idx);
        field_names.push_back(field->name());
        if (read_blob_view_fields_.find(field->name()) == read_blob_view_fields_.end()) {
            continue;
        }
        const auto& column = struct_array->field(field_idx);
        if (auto large_binary_array = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(column)) {
            PAIMON_ASSIGN_OR_RAISE(new_fields[field_idx], ResolveBinaryColumn(large_binary_array));
        } else {
            return Status::Invalid(fmt::format(
                "BlobViewResolvingBatchReader expects blob-view column {} to be LargeBinaryArray.",
                field->name()));
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> resolved_struct_array,
                                      arrow::StructArray::Make(new_fields, field_names));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*resolved_struct_array, c_array.get(), c_schema.get()));
    return batch;
}

Result<std::shared_ptr<arrow::Array>> BlobViewResolvingBatchReader::ResolveBinaryColumn(
    const std::shared_ptr<arrow::LargeBinaryArray>& blob_view_struct_array) {
    arrow::LargeBinaryBuilder builder(arrow_pool_.get());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(blob_view_struct_array->length()));
    for (int64_t row = 0; row < blob_view_struct_array->length(); ++row) {
        if (blob_view_struct_array->IsNull(row)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
            continue;
        }
        auto view = blob_view_struct_array->GetView(row);
        PAIMON_ASSIGN_OR_RAISE(bool is_view_struct,
                               BlobViewStruct::IsBlobViewStruct(view.data(), view.size()));
        if (!is_view_struct) {
            return Status::Invalid(
                "BlobViewResolvingBatchReader expects a serialized BlobViewStruct in the "
                "blob-view column.");
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobViewStruct> view_struct,
                               BlobViewStruct::Deserialize(view.data(), view.size()));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> descriptor_bytes, resolver_(*view_struct));
        if (descriptor_bytes == nullptr) {
            // null in source table
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
            continue;
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(
            reinterpret_cast<const uint8_t*>(descriptor_bytes->data()), descriptor_bytes->size()));
    }
    std::shared_ptr<arrow::Array> blob_descriptor_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&blob_descriptor_array));
    return blob_descriptor_array;
}

}  // namespace paimon
