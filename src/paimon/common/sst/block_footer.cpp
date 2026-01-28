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

#include "paimon/common/sst/block_footer.h"

#include "paimon/common/memory/memory_slice_output.h"

namespace paimon {

Result<std::unique_ptr<BlockFooter>> BlockFooter::ReadBlockFooter(
    std::shared_ptr<MemorySliceInput>& input) {
    auto offset = input->ReadLong();
    auto size = input->ReadInt();
    auto expected_entries = input->ReadLong();
    std::shared_ptr<BloomFilterHandle> bloom_filter_handle = nullptr;
    if (offset || size || expected_entries) {
        bloom_filter_handle = std::make_shared<BloomFilterHandle>(offset, size, expected_entries);
    }
    auto index_offset = input->ReadLong();
    auto index_size = input->ReadInt();
    auto index_block_handle = std::make_shared<BlockHandle>(index_offset, index_size);

    auto magic = input->ReadInt();
    if (magic != MAGIC_NUMBER) {
        return Status::IOError(
            fmt::format("Expected magic number {}, but got {}", MAGIC_NUMBER, magic));
    }
    return std::make_unique<BlockFooter>(index_block_handle, bloom_filter_handle);
}

std::shared_ptr<MemorySlice> BlockFooter::WriteBlockFooter(MemoryPool* pool) {
    auto output = std::make_shared<MemorySliceOutput>(ENCODED_LENGTH, pool);
    // 20 bytes
    if (!bloom_filter_handle_.get()) {
        output->WriteValue(static_cast<int64_t>(0));
        output->WriteValue(static_cast<int32_t>(0));
        output->WriteValue(static_cast<int64_t>(0));
    } else {
        output->WriteValue(bloom_filter_handle_->Offset());
        output->WriteValue(bloom_filter_handle_->Size());
        output->WriteValue(bloom_filter_handle_->ExpectedEntries());
    }
    // 12 bytes
    output->WriteValue(index_block_handle_->Offset());
    output->WriteValue(index_block_handle_->Size());
    // 4 bytes
    output->WriteValue(MAGIC_NUMBER);
    return output->ToSlice();
}
}  // namespace paimon
