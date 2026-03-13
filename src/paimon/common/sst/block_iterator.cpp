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
BlockIterator::BlockIterator(const std::shared_ptr<BlockReader>& reader) : reader_(reader) {
    input_ = reader->BlockInput();
}

bool BlockIterator::HasNext() const {
    return polled_.get() || input_->IsReadable();
}

Result<std::unique_ptr<BlockEntry>> BlockIterator::Next() {
    if (!HasNext()) {
        return Status::Invalid("no such element");
    }
    if (polled_.get()) {
        return std::move(polled_);
    }
    return ReadEntry();
}

std::unique_ptr<BlockEntry> BlockIterator::ReadEntry() {
    int32_t key_length = input_->ReadVarLenInt();
    auto key = input_->ReadSlice(key_length);
    int32_t value_length = input_->ReadVarLenInt();
    auto value = input_->ReadSlice(value_length);
    return std::make_unique<BlockEntry>(key, value);
}

Result<bool> BlockIterator::SeekTo(const std::shared_ptr<MemorySlice>& target_key) {
    int32_t left = 0;
    int32_t right = reader_->RecordCount() - 1;

    while (left <= right) {
        int32_t mid = left + (right - left) / 2;

        PAIMON_RETURN_NOT_OK(input_->SetPosition(reader_->SeekTo(mid)));
        auto mid_entry = ReadEntry();
        PAIMON_ASSIGN_OR_RAISE(int32_t compare, reader_->Comparator()(mid_entry->key, target_key));

        if (compare == 0) {
            polled_ = std::move(mid_entry);
            return true;
        } else if (compare > 0) {
            polled_ = std::move(mid_entry);
            right = mid - 1;
        } else {
            polled_.reset();
            left = mid + 1;
        }
    }

    return false;
}

}  // namespace paimon
