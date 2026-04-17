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

#include "paimon/common/sst/block_iterator.h"

#include "paimon/common/sst/block_reader.h"

namespace paimon {
BlockIterator::BlockIterator(const std::shared_ptr<BlockReader>& reader)
    : input_(reader->BlockInput()), reader_(reader) {}

bool BlockIterator::HasNext() const {
    return polled_position_ >= 0 || input_.IsReadable();
}

Result<std::unique_ptr<BlockEntry>> BlockIterator::Next() {
    if (!HasNext()) {
        return Status::Invalid("no such element");
    }
    if (polled_position_ >= 0) {
        PAIMON_RETURN_NOT_OK(input_.SetPosition(polled_position_));
        polled_position_ = -1;
        return ReadEntry();
    }
    return ReadEntry();
}

Result<std::unique_ptr<BlockEntry>> BlockIterator::ReadEntry() {
    PAIMON_ASSIGN_OR_RAISE(int32_t key_length, input_.ReadVarLenInt());
    auto key = input_.ReadSlice(key_length);
    PAIMON_ASSIGN_OR_RAISE(int32_t value_length, input_.ReadVarLenInt());
    auto value = input_.ReadSlice(value_length);
    return std::make_unique<BlockEntry>(key, value);
}

Result<MemorySlice> BlockIterator::ReadKeyAndSkipValue() {
    PAIMON_ASSIGN_OR_RAISE(int32_t key_length, input_.ReadVarLenInt());
    auto key = input_.ReadSlice(key_length);
    PAIMON_ASSIGN_OR_RAISE(int32_t value_length, input_.ReadVarLenInt());
    PAIMON_RETURN_NOT_OK(input_.SetPosition(input_.Position() + value_length));
    return key;
}

Result<bool> BlockIterator::SeekTo(const MemorySlice& target_key) {
    int32_t left = 0;
    int32_t right = reader_->RecordCount() - 1;
    polled_position_ = -1;

    while (left <= right) {
        int32_t mid = left + (right - left) / 2;

        int32_t entry_position = reader_->SeekTo(mid);
        PAIMON_RETURN_NOT_OK(input_.SetPosition(entry_position));

        PAIMON_ASSIGN_OR_RAISE(MemorySlice mid_key, ReadKeyAndSkipValue());
        PAIMON_ASSIGN_OR_RAISE(int32_t compare, reader_->Comparator()(mid_key, target_key));

        if (compare == 0) {
            polled_position_ = entry_position;
            return true;
        } else if (compare > 0) {
            // mid_key > target_key, this could be the first key >= target_key
            polled_position_ = entry_position;
            right = mid - 1;
        } else {
            // mid_key < target_key, need to look at larger keys
            // Don't reset polled_position_ here - keep the last position where key > target
            left = mid + 1;
        }
    }

    // If we exit the loop without finding exact match, polled_position_ points to
    // the first entry with key > target_key (if any), or -1 if all keys < target_key
    return false;
}

}  // namespace paimon
