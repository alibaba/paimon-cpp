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

#include <stdexcept>

#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/sst/block_entry.h"

namespace paimon {
class BlockReader;

class BlockIterator {
 public:
    explicit BlockIterator(std::shared_ptr<BlockReader>& reader);

    ~BlockIterator() = default;

    bool HasNext() const;

    Result<std::unique_ptr<BlockEntry>> Next();

    std::unique_ptr<BlockEntry> ReadEntry();

    bool SeekTo(std::shared_ptr<paimon::MemorySlice> target_key);

 private:
    std::shared_ptr<MemorySliceInput> input_;
    std::unique_ptr<BlockEntry> polled_;
    std::shared_ptr<BlockReader> reader_;
};

}  // namespace paimon
