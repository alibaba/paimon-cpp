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
#include <memory>
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

Result<bool> MergedKeyValueRecordReader::Iterator::HasNext() const {
    if (merged_key_value_.has_value()) {
        return true;
    }
    return PrepareNextMergedKeyValue();
}

Result<KeyValue> MergedKeyValueRecordReader::Iterator::Next() {
    if (!merged_key_value_.has_value()) {
        return Status::Invalid("No more merged key values in current iterator");
    }

    KeyValue result = std::move(*merged_key_value_);
    merged_key_value_.reset();
    return result;
}

Result<bool> MergedKeyValueRecordReader::Iterator::PrepareNextMergedKeyValue() const {
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(bool has_next, MergeNextKey());
        if (!has_next) {
            return false;
        }
        // if merged_key_value_ is empty(maybe all filtered out), continue to fetch next key and
        // merge until we get a non-empty merged result or no more keys
        if (merged_key_value_.has_value()) {
            return true;
        }
    }
}

Result<bool> MergedKeyValueRecordReader::Iterator::MergeNextKey() const {
    PAIMON_RETURN_NOT_OK(LoadNextRawKeyValue());
    if (!next_raw_key_value_.has_value()) {
        return false;
    }

    reader_->merge_function_wrapper_->Reset();
    auto current_key = next_raw_key_value_->key;

    do {
        PAIMON_RETURN_NOT_OK(
            reader_->merge_function_wrapper_->Add(std::move(*next_raw_key_value_)));
        next_raw_key_value_.reset();
        PAIMON_RETURN_NOT_OK(LoadNextRawKeyValue());
    } while (next_raw_key_value_.has_value() &&
             reader_->key_comparator_->CompareTo(*current_key, *next_raw_key_value_->key) == 0);

    PAIMON_ASSIGN_OR_RAISE(std::optional<KeyValue> result,
                           reader_->merge_function_wrapper_->GetResult());
    merged_key_value_ = std::move(result);
    return true;
}

Status MergedKeyValueRecordReader::Iterator::LoadNextRawKeyValue() const {
    if (next_raw_key_value_.has_value()) {
        return Status::OK();
    }

    while (true) {
        if (current_iterator_ != nullptr) {
            PAIMON_ASSIGN_OR_RAISE(bool has_next, current_iterator_->HasNext());
            if (has_next) {
                PAIMON_ASSIGN_OR_RAISE(KeyValue key_value, current_iterator_->Next());
                next_raw_key_value_.emplace(std::move(key_value));
                return Status::OK();
            }
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

    auto iterator = std::make_unique<Iterator>(this);
    PAIMON_ASSIGN_OR_RAISE(bool has_next, iterator->HasNext());
    if (!has_next) {
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
