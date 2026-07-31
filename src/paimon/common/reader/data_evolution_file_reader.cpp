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

#include "paimon/common/reader/data_evolution_file_reader.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/util.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

Result<std::unique_ptr<DataEvolutionFileReader>> DataEvolutionFileReader::Create(
    std::vector<std::unique_ptr<BatchReader>>&& readers,
    const std::shared_ptr<arrow::Schema>& read_schema, int32_t read_batch_size,
    const std::vector<int32_t>& reader_offsets, const std::vector<int32_t>& field_offsets,
    const std::shared_ptr<MemoryPool>& pool) {
    if (read_schema->num_fields() == 0) {
        return Status::Invalid("read schema must not be empty");
    }
    if (static_cast<size_t>(read_schema->num_fields()) != reader_offsets.size() ||
        reader_offsets.size() != field_offsets.size()) {
        return Status::Invalid(
            "read schema, row offsets and field offsets must have the same size");
    }
    if (readers.empty()) {
        return Status::Invalid("readers must not be empty");
    }
    for (int32_t reader_offset : reader_offsets) {
        if (reader_offset >= static_cast<int32_t>(readers.size())) {
            return Status::Invalid("reader offset is out of range of readers");
        }
    }
    return std::unique_ptr<DataEvolutionFileReader>(
        new DataEvolutionFileReader(std::move(readers), read_schema, read_batch_size,
                                    reader_offsets, field_offsets, GetArrowPool(pool)));
}

Result<BatchReader::ReadBatchWithBitmap> DataEvolutionFileReader::NextBatchWithBitmap() {
    std::vector<std::shared_ptr<arrow::StructArray>> array_for_each_reader;
    array_for_each_reader.reserve(readers_.size());
    int64_t array_length = -1;
    for (size_t i = 0; i < readers_.size(); i++) {
        if (!readers_[i]) {
            // no read field from readers_[i]
            array_for_each_reader.push_back(nullptr);
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array, NextBatchForSingleReader(i));
        if (array == nullptr) {
            // read eof
            return BatchReader::MakeEofBatchWithBitmap();
        }
        if (array_length == -1) {
            array_length = array->length();
        } else if (array_length != array->length()) {
            return Status::Invalid("array for single reader length mismatch others");
        }
        auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(array);
        assert(struct_array);
        array_for_each_reader.push_back(struct_array);
    }
    int32_t read_field_count = read_schema_->num_fields();
    arrow::ArrayVector target_sub_array_vec;
    target_sub_array_vec.reserve(read_field_count);
    for (int32_t i = 0; i < read_field_count; i++) {
        if (reader_offsets_[i] == -1) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> null_array,
                                   GetOrCreateNonExistArray(i, array_length));
            target_sub_array_vec.push_back(null_array);
            continue;
        }
        const auto& sub_array = array_for_each_reader[reader_offsets_[i]];
        assert(sub_array->num_fields() > field_offsets_[i]);
        // Each file is already aligned to its read schema by its FieldMappingReader.
        target_sub_array_vec.push_back(sub_array->field(field_offsets_[i]));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> target_array,
        arrow::StructArray::Make(target_sub_array_vec, read_schema_->field_names()));
    std::unique_ptr<ArrowArray> target_c_arrow_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> target_c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*target_array, target_c_arrow_array.get(), target_c_schema.get()));
    auto target_batch = std::make_pair(std::move(target_c_arrow_array), std::move(target_c_schema));
    return ReaderUtils::AddAllValidBitmap(std::move(target_batch));
}

Result<std::shared_ptr<arrow::Array>> DataEvolutionFileReader::GetOrCreateNonExistArray(
    int32_t field_idx, int64_t array_length) {
    if (!non_exist_array_vec_[field_idx] ||
        non_exist_array_vec_[field_idx]->length() < array_length) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            non_exist_array_vec_[field_idx],
            arrow::MakeArrayOfNull(read_schema_->field(field_idx)->type(), array_length,
                                   arrow_pool_.get()));
    }
    if (non_exist_array_vec_[field_idx]->length() == array_length) {
        return non_exist_array_vec_[field_idx];
    }
    return non_exist_array_vec_[field_idx]->Slice(0, array_length);
}

int64_t DataEvolutionFileReader::CalculateCachedArrayLength(size_t reader_idx) const {
    int64_t total_length = 0;
    for (const auto& array : cached_array_vec_[reader_idx]) {
        total_length += array->length();
    }
    return total_length;
}

Result<std::shared_ptr<arrow::Array>> DataEvolutionFileReader::NextBatchForSingleReader(
    size_t reader_idx) {
    int64_t total_array_length = CalculateCachedArrayLength(reader_idx);
    if (total_array_length >= read_batch_size_) {
        assert(false);
        return Status::Invalid(fmt::format(
            "Unexpected: the length of cached array in last turn {} exceed read batch size {}",
            total_array_length, read_batch_size_));
    }
    // array left for last turn
    arrow::ArrayVector concat_array_vec = std::move(cached_array_vec_[reader_idx]);
    cached_array_vec_[reader_idx].clear();
    while (total_array_length < read_batch_size_) {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap src_array_with_bitmap,
                               readers_[reader_idx]->NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(src_array_with_bitmap)) {
            // read finish
            break;
        }
        auto& [read_batch, bitmap] = src_array_with_bitmap;
        auto& [c_array, c_schema] = read_batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> src_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        PAIMON_ASSIGN_OR_RAISE(arrow::ArrayVector selected_array_vec,
                               ReaderUtils::GenerateFilteredArrayVector(src_array, bitmap));
        for (const auto& selected_array : selected_array_vec) {
            if (total_array_length + selected_array->length() > read_batch_size_) {
                // need truncate current array to align read_batch_size_
                int64_t truncated_length = read_batch_size_ - total_array_length;
                if (truncated_length == 0) {
                    // total_array_length equals to read_batch_size_, all selected_array left will
                    // be added to cached_array_vec_
                    cached_array_vec_[reader_idx].push_back(selected_array);
                } else {
                    concat_array_vec.push_back(selected_array->Slice(0, truncated_length));
                    cached_array_vec_[reader_idx].push_back(
                        selected_array->Slice(truncated_length));
                    total_array_length += truncated_length;
                }
            } else {
                concat_array_vec.push_back(selected_array);
                total_array_length += selected_array->length();
            }
        }
    }
    if (concat_array_vec.empty()) {
        return std::shared_ptr<arrow::Array>();
    }
    if (concat_array_vec.size() == 1 && concat_array_vec[0]->offset() == 0) {
        // Avoid data copy when the array is already normalized.
        return concat_array_vec[0];
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> concat_array,
                                      arrow::Concatenate(concat_array_vec, arrow_pool_.get()));
    assert(concat_array->length() == total_array_length);
    assert(concat_array->length() <= read_batch_size_);
    return concat_array;
}

void DataEvolutionFileReader::Close() {
    cached_array_vec_.clear();
    non_exist_array_vec_.clear();
    for (const auto& reader : readers_) {
        if (reader) {
            reader->Close();
        }
    }
}

std::shared_ptr<Metrics> DataEvolutionFileReader::GetReaderMetrics() const {
    return MetricsImpl::CollectReadMetrics(readers_);
}

}  // namespace paimon
