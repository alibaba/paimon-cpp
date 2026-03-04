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

namespace paimon {

Result<std::shared_ptr<BTreeFileFooter>> BTreeFileFooter::Read(
    const std::shared_ptr<MemorySliceInput>& input) {
    // read bloom filter and index handles
    std::shared_ptr<BloomFilterHandle> bloom_filter_handle =
        std::make_shared<BloomFilterHandle>(input->ReadLong(), input->ReadInt(), input->ReadLong());
    if (bloom_filter_handle->Offset() == 0 && bloom_filter_handle->Size() == 0 &&
        bloom_filter_handle->ExpectedEntries() == 0) {
        bloom_filter_handle = nullptr;
    }

    std::shared_ptr<BlockHandle> index_block_handle =
        std::make_shared<BlockHandle>(input->ReadLong(), input->ReadInt());

    std::shared_ptr<BlockHandle> null_bitmap_handle =
        std::make_shared<BlockHandle>(input->ReadLong(), input->ReadInt());
    if (null_bitmap_handle->Offset() == 0 && null_bitmap_handle->Size() == 0) {
        null_bitmap_handle = nullptr;
    }

    // skip padding
    input->SetPosition(ENCODED_LENGTH - 4);

    // verify magic number
    int32_t magic_number = input->ReadInt();
    if (magic_number != MAGIC_NUMBER) {
        return Status::IOError("File is not a table (bad magic number)");
    }

    return std::make_shared<BTreeFileFooter>(bloom_filter_handle, index_block_handle,
                                             null_bitmap_handle);
}

std::shared_ptr<MemorySlice> BTreeFileFooter::Write(const std::shared_ptr<BTreeFileFooter>& footer,
                                                    MemoryPool* pool) {
    auto output = std::make_shared<MemorySliceOutput>(ENCODED_LENGTH, pool);
    return BTreeFileFooter::Write(footer, output);
}

std::shared_ptr<MemorySlice> BTreeFileFooter::Write(
    const std::shared_ptr<BTreeFileFooter>& footer,
    const std::shared_ptr<MemorySliceOutput>& ouput) {
    // write bloom filter and index handles
    auto bloom_filter_handle = footer->GetBloomFilterHandle();
    if (!bloom_filter_handle) {
        ouput->WriteValue(static_cast<int64_t>(0));
        ouput->WriteValue(static_cast<int32_t>(0));
        ouput->WriteValue(static_cast<int64_t>(0));
    } else {
        ouput->WriteValue(bloom_filter_handle->Offset());
        ouput->WriteValue(bloom_filter_handle->Size());
        ouput->WriteValue(bloom_filter_handle->ExpectedEntries());
    }

    auto index_block_handle = footer->GetIndexBlockHandle();
    ouput->WriteValue(index_block_handle->Offset());
    ouput->WriteValue(index_block_handle->Size());

    auto null_bitmap_handle = footer->GetNullBitmapHandle();
    if (!null_bitmap_handle) {
        ouput->WriteValue(static_cast<int64_t>(0));
        ouput->WriteValue(static_cast<int32_t>(0));
    } else {
        ouput->WriteValue(null_bitmap_handle->Offset());
        ouput->WriteValue(null_bitmap_handle->Size());
    }

    // write magic number
    ouput->WriteValue(MAGIC_NUMBER);

    return ouput->ToSlice();
}

}  // namespace paimon
