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

#include "paimon/core/mergetree/write_buffer.h"

#include <limits>
#include <memory>
#include <utility>

#include "arrow/type.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/io/merged_key_value_record_reader.h"
#include "paimon/core/mergetree/in_memory_sort_buffer.h"

namespace paimon {

WriteBuffer::WriteBuffer(
    int64_t last_sequence_number, const std::shared_ptr<arrow::DataType>& value_type,
    const std::vector<std::string>& trimmed_primary_keys,
    const std::vector<std::string>& user_defined_sequence_fields,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
    const std::shared_ptr<MemoryPool>& pool)
    : key_comparator_(key_comparator), merge_function_wrapper_(merge_function_wrapper) {
    // TODO(jinli.zjw): input sequence_fields_ascending as parameter
    sort_buffer_ = std::make_unique<InMemorySortBuffer>(
        last_sequence_number, value_type, trimmed_primary_keys, user_defined_sequence_fields,
        /*sequence_fields_ascending=*/true, key_comparator,
        /*write_buffer_size=*/std::numeric_limits<int64_t>::max(), pool);
}

Status WriteBuffer::Write(std::unique_ptr<RecordBatch>&& batch) {
    return sort_buffer_->Write(std::move(batch)).status();
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> WriteBuffer::CreateReaders() {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                           sort_buffer_->CreateReaders());
    std::vector<std::unique_ptr<KeyValueRecordReader>> merged_readers;
    merged_readers.reserve(readers.size());
    for (auto& reader : readers) {
        merged_readers.push_back(std::make_unique<MergedKeyValueRecordReader>(
            std::move(reader), key_comparator_, merge_function_wrapper_));
    }
    return merged_readers;
}

void WriteBuffer::Clear() {
    sort_buffer_->Clear();
}

}  // namespace paimon
