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

#include "paimon/core/io/merged_key_value_record_reader.h"

#include <cassert>
#include <optional>
#include <utility>

#include "paimon/common/utils/fields_comparator.h"

namespace paimon {

MergedKeyValueRecordReader::MergedKeyValueRecordReader(
    std::unique_ptr<KeyValueRecordReader>&& reader,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper)
    : reader_(std::move(reader)),
      key_comparator_(key_comparator),
      merge_function_wrapper_(merge_function_wrapper) {
    assert(reader_ != nullptr);
    assert(key_comparator_ != nullptr);
    assert(merge_function_wrapper_ != nullptr);
}

Result<std::unique_ptr<MergedKeyValueRecordReader::Iterator>>
MergedKeyValueRecordReader::Iterator::Create(MergedKeyValueRecordReader* reader) {
    std::unique_ptr<Iterator> iterator(new Iterator(reader));
    PAIMON_RETURN_NOT_OK(iterator->LoadNextKeyValue());
    return iterator;
}

Result<KeyValue> MergedKeyValueRecordReader::Iterator::Next() {
    assert(next_key_value_.has_value());
    reader_->merge_function_wrapper_->Reset();
    auto current_key = next_key_value_->key;
    PAIMON_RETURN_NOT_OK(reader_->merge_function_wrapper_->Add(std::move(*next_key_value_)));
    next_key_value_.reset();

    while (true) {
        PAIMON_RETURN_NOT_OK(LoadNextKeyValue());
        if (!next_key_value_.has_value()) {
            break;
        }
        if (reader_->key_comparator_->CompareTo(*current_key, *next_key_value_->key) != 0) {
            break;
        }
        PAIMON_RETURN_NOT_OK(reader_->merge_function_wrapper_->Add(std::move(*next_key_value_)));
        next_key_value_.reset();
    }

    PAIMON_ASSIGN_OR_RAISE(std::optional<KeyValue> result,
                           reader_->merge_function_wrapper_->GetResult());
    // TODO(jinli.zjw): support merge function producing no result (e.g. all rows are filtered out)
    if (result == std::nullopt) {
        return Status::Invalid("merged key value reader produced empty result");
    }
    return std::move(result).value();
}

Status MergedKeyValueRecordReader::Iterator::LoadNextKeyValue() {
    if (next_key_value_.has_value()) {
        return Status::OK();
    }

    while (true) {
        if (current_iterator_ != nullptr && current_iterator_->HasNext()) {
            PAIMON_ASSIGN_OR_RAISE(KeyValue key_value, current_iterator_->Next());
            next_key_value_.emplace(std::move(key_value));
            return Status::OK();
        }

        current_iterator_.reset();
        PAIMON_ASSIGN_OR_RAISE(current_iterator_, reader_->reader_->NextBatch());
        if (current_iterator_ == nullptr) {
            return Status::OK();
        }
    }
}

Result<std::unique_ptr<KeyValueRecordReader::Iterator>> MergedKeyValueRecordReader::NextBatch() {
    if (visited_) {
        return std::unique_ptr<KeyValueRecordReader::Iterator>();
    }
    visited_ = true;

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Iterator> iterator, Iterator::Create(this));
    if (!iterator->HasNext()) {
        return std::unique_ptr<KeyValueRecordReader::Iterator>();
    }
    return iterator;
}

std::shared_ptr<Metrics> MergedKeyValueRecordReader::GetReaderMetrics() const {
    return reader_->GetReaderMetrics();
}

void MergedKeyValueRecordReader::Close() {
    visited_ = true;
    reader_->Close();
}

}  // namespace paimon
