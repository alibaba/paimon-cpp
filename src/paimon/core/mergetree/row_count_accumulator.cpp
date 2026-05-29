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

#include "paimon/core/mergetree/row_count_accumulator.h"

#include <utility>

#include "paimon/core/key_value.h"

namespace paimon {

RowCountAccumulator::RowCountAccumulator(std::unique_ptr<SortMergeReader>&& merged_reader)
    : merged_reader_(std::move(merged_reader)) {}

Result<int64_t> RowCountAccumulator::CountAll() {
    int64_t count = 0;

    while (true) {
        // Get next batch of merged KV iterators
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader::Iterator> iter,
                               merged_reader_->NextBatch());
        if (iter == nullptr) {
            // No more data
            break;
        }

        // Iterate through all KV objects in this batch
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(bool has_next, iter->HasNext());
            if (!has_next) {
                break;
            }

            iter->Next();

            // At this point:
            // - kv has passed through SortMergeReader (deduplicated, merged)
            // - kv has passed through DropDeleteReader (kind is guaranteed IsAdd())
            // - kv represents a final, valid, non-deleted row
            count++;
        }
    }

    return count;
}

void RowCountAccumulator::Close() {
    if (merged_reader_) {
        merged_reader_->Close();
    }
}

}  // namespace paimon
