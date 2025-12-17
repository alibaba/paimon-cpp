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

#pragma once

#include <unistd.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/api.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/io/key_value_data_file_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/testing/utils/dict_array_converter.h"
namespace paimon::test {
class ReadResultCollector {
 public:
    ReadResultCollector() = delete;
    ~ReadResultCollector() = delete;

    template <typename ReaderType, typename IteratorType>
    static Result<std::vector<KeyValue>> CollectKeyValueResult(ReaderType* reader) {
        std::vector<KeyValue> results;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IteratorType> iterator, reader->NextBatch());
            if (iterator == nullptr) {
                break;
            }
            while (true) {
                if constexpr (std::is_same_v<IteratorType, KeyValueRecordReader::Iterator>) {
                    if (!iterator->HasNext()) {
                        break;
                    }
                    PAIMON_ASSIGN_OR_RAISE(KeyValue kv, iterator->Next());
                    results.emplace_back(std::move(kv));
                } else {
                    PAIMON_ASSIGN_OR_RAISE(bool has_next, iterator->HasNext());
                    if (!has_next) {
                        break;
                    }
                    KeyValue kv = iterator->Next();
                    results.emplace_back(std::move(kv));
                }
            }
        }
        return results;
    }

    static Result<std::shared_ptr<arrow::ChunkedArray>> CollectResult(BatchReader* batch_reader) {
        return CollectResult(batch_reader, /*max simulated data processing time*/ 0);
    }

    // will convert dictionary array to string array for comparing results
    static Result<std::shared_ptr<arrow::ChunkedArray>> CollectResult(
        BatchReader* batch_reader, int64_t max_data_processing_time_in_us) {
        arrow::ArrayVector result_array_vector;
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
        while (true) {
            // Prioritize calling NextBatch. If it fails (paimon inner reader e.g.,
            // PrefetchBatchReader, ApplyBitmapIndexBatchReader...), call NextBatchWithBitmap.
            auto batch_result = batch_reader->NextBatch();
            BatchReader::ReadBatch batch;
            if (!batch_result.ok()) {
                if (batch_result.status().ToString().find("should use NextBatchWithBitmap") !=
                    std::string::npos) {
                    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                                           batch_reader->NextBatchWithBitmap());
                    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
                        break;
                    }
                    assert(!batch_with_bitmap.second.IsEmpty());
                    PAIMON_ASSIGN_OR_RAISE(
                        batch, ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap),
                                                                   arrow::default_memory_pool()));
                } else {
                    return batch_result.status();
                }
            } else {
                batch = std::move(batch_result).value();
                if (BatchReader::IsEofBatch(batch)) {
                    break;
                }
            }
            auto& [c_array, c_schema] = batch;
            assert(c_array->length > 0);
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto result_array,
                                              arrow::ImportArray(c_array.get(), c_schema.get()));
            result_array_vector.push_back(result_array);
            if (max_data_processing_time_in_us > 0) {
                usleep(std::rand() % max_data_processing_time_in_us);
            }
        }
        if (result_array_vector.empty()) {
            return std::shared_ptr<arrow::ChunkedArray>();
        }
        // accumulate all the batch array and convert dictionary to string array together to avoid
        // the problem (multiple batches in multiple stripes overlap dictionary data) being
        // difficult to expose
        arrow::ArrayVector converted_array_vector;
        for (const auto& array : result_array_vector) {
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Array> converted_array,
                DictArrayConverter::ConvertDictArray(array, arrow::default_memory_pool()));
            converted_array_vector.push_back(converted_array);
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto chunk_array,
                                          arrow::ChunkedArray::Make(converted_array_vector));
        return chunk_array;
    }

    static Result<std::shared_ptr<arrow::Array>> GetArray(BatchReader::ReadBatch&& batch) {
        if (BatchReader::IsEofBatch(batch)) {
            return std::shared_ptr<arrow::Array>();
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        return DictArrayConverter::ConvertDictArray(array, arrow::default_memory_pool());
    }

    static Result<BatchReader::ReadBatch> GetReadBatch(const std::shared_ptr<arrow::Array>& array) {
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
        return std::make_pair(std::move(c_array), std::move(c_schema));
    }

    // Noted that, sort chunked array by multiple key for timestamp type may cause
    // coredump in arrow, refer to https://github.com/apache/arrow/issues/47252
    static Result<std::shared_ptr<arrow::ChunkedArray>> SortArray(
        const std::shared_ptr<arrow::ChunkedArray>& array,
        const std::shared_ptr<arrow::Schema>& schema) {
        std::vector<arrow::compute::SortKey> sort_keys;
        for (const auto& name : schema->field_names()) {
            sort_keys.emplace_back(name, arrow::compute::SortOrder::Ascending);
        }
        auto sort_options = arrow::compute::SortOptions(sort_keys);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto sorted_indices, arrow::compute::SortIndices(arrow::Datum(array), sort_options));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto sorted_batch,
            arrow::compute::Take(arrow::Datum(array), arrow::Datum(sorted_indices)));
        return sorted_batch.chunked_array();
    }
};
}  // namespace paimon::test
