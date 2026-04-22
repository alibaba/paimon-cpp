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

#include "paimon/core/mergetree/compact/raw_sort_merge_reader_with_min_heap.h"

namespace paimon {

RawSortMergeReaderWithMinHeap::RawSortMergeReaderWithMinHeap(
    std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
    const std::shared_ptr<FieldsComparator>& user_key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator)
    : readers_holder_(std::move(readers)),
      min_heap_(HeapSorter(user_key_comparator, user_defined_seq_comparator)) {
    next_batch_readers_.reserve(readers_holder_.size());
    for (auto& reader : readers_holder_) {
        next_batch_readers_.push_back(reader.get());
    }
}

Result<std::unique_ptr<SortMergeReader::Iterator>> RawSortMergeReaderWithMinHeap::NextBatch() {
    for (auto* reader : next_batch_readers_) {
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<KeyValueRecordReader::Iterator> iterator,
                                   reader->NextBatch());
            if (!iterator) {
                reader->Close();
                break;
            }
            if (iterator->HasNext()) {
                PAIMON_ASSIGN_OR_RAISE(KeyValue kv, iterator->Next());
                min_heap_.emplace(std::move(kv), std::move(iterator), reader);
                break;
            }
        }
    }
    next_batch_readers_.clear();
    if (min_heap_.empty()) {
        return std::unique_ptr<SortMergeReader::Iterator>();
    }
    return std::make_unique<RawSortMergeReaderWithMinHeap::Iterator>(this);
}

Result<bool> RawSortMergeReaderWithMinHeap::AdvanceCurrentElement() {
    if (!current_element_.has_value()) {
        return true;
    }

    PAIMON_ASSIGN_OR_RAISE(bool updated, current_element_->Update());
    if (updated) {
        min_heap_.push(std::move(*current_element_));
    } else {
        next_batch_readers_.push_back(current_element_->reader);
    }
    current_element_.reset();
    return true;
}

Result<bool> RawSortMergeReaderWithMinHeap::Iterator::HasNext() {
    PAIMON_ASSIGN_OR_RAISE(bool advanced, reader_->AdvanceCurrentElement());
    if (!advanced) {
        return false;
    }

    if (!reader_->next_batch_readers_.empty()) {
        return false;
    }

    if (reader_->min_heap_.empty()) {
        return false;
    }

    auto element = std::move(const_cast<Element&>(reader_->min_heap_.top()));
    reader_->min_heap_.pop();
    result_ = std::move(element.kv);
    reader_->current_element_.emplace(std::move(element));
    return true;
}

}  // namespace paimon
