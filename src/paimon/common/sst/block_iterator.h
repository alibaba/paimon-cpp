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

#pragma once

#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/sst/block_entry.h"

namespace paimon {
class BlockReader;

class PAIMON_EXPORT BlockIterator {
 public:
    explicit BlockIterator(const std::shared_ptr<BlockReader>& reader);

    bool HasNext() const;

    Result<std::unique_ptr<BlockEntry>> Next();

    Result<std::unique_ptr<BlockEntry>> ReadEntry();

    Result<bool> SeekTo(const MemorySlice& target_key);

 private:
    /// Read only the key MemorySlice from the current position, skipping the value.
    /// This avoids creating a value MemorySlice and BlockEntry during binary search.
    Result<MemorySlice> ReadKeyAndSkipValue();

    MemorySliceInput input_;
    /// Position of the entry that should be returned by Next() after SeekTo.
    /// -1 means no pending entry.
    int32_t polled_position_ = -1;
    std::shared_ptr<BlockReader> reader_;
};

}  // namespace paimon
