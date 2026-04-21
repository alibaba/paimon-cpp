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

#include "paimon/core/mergetree/compact/sort_merge_reader_with_min_heap.h"

#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/status.h"

namespace paimon {
class InternalRow;

SortMergeReaderWithMinHeap::SortMergeReaderWithMinHeap(
    std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
    const std::shared_ptr<FieldsComparator>& user_key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper)
    : readers_holder_(std::move(readers)),
      user_key_comparator_(user_key_comparator),
      merge_function_wrapper_(merge_function_wrapper),
      min_heap_(HeapSorter(user_key_comparator, user_defined_seq_comparator)) {
    next_batch_readers_.reserve(readers_holder_.size());
    for (auto& reader : readers_holder_) {
        next_batch_readers_.push_back(reader.get());
    }
}

Result<std::unique_ptr<SortMergeReader::Iterator>> SortMergeReaderWithMinHeap::NextBatch() {
    for (auto* reader : next_batch_readers_) {
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<KeyValueRecordReader::Iterator> iterator,
                                   reader->NextBatch());
            if (!iterator) {
                // no more batches, permanently remove this reader
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
    return std::make_unique<SortMergeReaderWithMinHeap::Iterator>(this);
}

Result<bool> SortMergeReaderWithMinHeap::Iterator::HasNext() {
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(bool has_more, NextImpl());
        if (!has_more) {
            return false;
        }
        PAIMON_ASSIGN_OR_RAISE(std::optional<KeyValue> result,
                               reader_->merge_function_wrapper_->GetResult());
        if (result) {
            result_ = std::move(result);
            return true;
        }
    }
}

Result<bool> SortMergeReaderWithMinHeap::Iterator::NextImpl() {
    assert(reader_->next_batch_readers_.empty());
    // add previously polled elements back to priority queue
    for (auto& element : reader_->polled_) {
        PAIMON_ASSIGN_OR_RAISE(bool updated, element.Update());
        if (updated) {
            // still kvs left, add back to priority queue
            reader_->min_heap_.push(std::move(element));
        } else {
            // reach end of batch, clean up
            reader_->next_batch_readers_.push_back(std::move(element.reader));
        }
    }
    reader_->polled_.clear();
    if (!reader_->next_batch_readers_.empty()) {
        return false;
    }

    if (reader_->min_heap_.empty()) {
        return Status::Invalid("Min heap is empty. This is a bug.");
    }
    reader_->merge_function_wrapper_->Reset();
    std::shared_ptr<InternalRow> key = reader_->min_heap_.top().kv.key;
    bool is_first = true;

    // fetch all elements with the same key
    // note that the same iterator should not produce the same keys, so this code is correct
    while (!reader_->min_heap_.empty()) {
        auto& element = const_cast<Element&>(reader_->min_heap_.top());
        if (!is_first && reader_->user_key_comparator_->CompareTo(*key, *(element.kv.key)) != 0) {
            break;
        }
        PAIMON_RETURN_NOT_OK(reader_->merge_function_wrapper_->Add(std::move(element.kv)));
        reader_->polled_.push_back(std::move(element));
        reader_->min_heap_.pop();
        is_first = false;
    }
    return true;
}

}  // namespace paimon
