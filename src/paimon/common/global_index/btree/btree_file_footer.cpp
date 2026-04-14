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

#include "paimon/common/global_index/btree/btree_file_footer.h"

#include <fmt/format.h>

namespace paimon {

Result<std::shared_ptr<BTreeFileFooter>> BTreeFileFooter::Read(MemorySliceInput* input) {
    // read version and verify magic number
    PAIMON_RETURN_NOT_OK(input->SetPosition(ENCODED_LENGTH - 8));

    int32_t version = input->ReadInt();
    int32_t magic_number = input->ReadInt();
    if (magic_number != kMagicNumber) {
        return Status::Invalid(
            fmt::format("File is not a btree index file (expected magic number {:#x}, got {:#x})",
                        kMagicNumber, magic_number));
    }

    PAIMON_RETURN_NOT_OK(input->SetPosition(0));

    // read bloom filter and index handles
    auto offset = input->ReadLong();
    auto size = input->ReadInt();
    auto expected_entries = input->ReadLong();
    std::shared_ptr<BloomFilterHandle> bloom_filter_handle =
        std::make_shared<BloomFilterHandle>(offset, size, expected_entries);
    if (bloom_filter_handle->Offset() == 0 && bloom_filter_handle->Size() == 0 &&
        bloom_filter_handle->ExpectedEntries() == 0) {
        bloom_filter_handle = nullptr;
    }

    offset = input->ReadLong();
    size = input->ReadInt();
    std::shared_ptr<BlockHandle> index_block_handle = std::make_shared<BlockHandle>(offset, size);

    offset = input->ReadLong();
    size = input->ReadInt();
    std::shared_ptr<BlockHandle> null_bitmap_handle = std::make_shared<BlockHandle>(offset, size);
    if (null_bitmap_handle->Offset() == 0 && null_bitmap_handle->Size() == 0) {
        null_bitmap_handle = nullptr;
    }

    return std::make_shared<BTreeFileFooter>(version, bloom_filter_handle, index_block_handle,
                                             null_bitmap_handle);
}

MemorySlice BTreeFileFooter::Write(const std::shared_ptr<BTreeFileFooter>& footer,
                                   MemoryPool* pool) {
    MemorySliceOutput output(ENCODED_LENGTH, pool);
    return BTreeFileFooter::Write(footer, output);
}

MemorySlice BTreeFileFooter::Write(const std::shared_ptr<BTreeFileFooter>& footer,
                                   MemorySliceOutput& output) {
    // write bloom filter and index handles
    auto bloom_filter_handle = footer->GetBloomFilterHandle();
    if (!bloom_filter_handle) {
        output.WriteValue(static_cast<int64_t>(0));
        output.WriteValue(static_cast<int32_t>(0));
        output.WriteValue(static_cast<int64_t>(0));
    } else {
        output.WriteValue(bloom_filter_handle->Offset());
        output.WriteValue(bloom_filter_handle->Size());
        output.WriteValue(bloom_filter_handle->ExpectedEntries());
    }

    auto index_block_handle = footer->GetIndexBlockHandle();
    output.WriteValue(index_block_handle->Offset());
    output.WriteValue(index_block_handle->Size());

    auto null_bitmap_handle = footer->GetNullBitmapHandle();
    if (!null_bitmap_handle) {
        output.WriteValue(static_cast<int64_t>(0));
        output.WriteValue(static_cast<int32_t>(0));
    } else {
        output.WriteValue(null_bitmap_handle->Offset());
        output.WriteValue(null_bitmap_handle->Size());
    }

    // write version and magic number
    output.WriteValue(footer->GetVersion());
    output.WriteValue(kMagicNumber);

    return output.ToSlice();
}

}  // namespace paimon
