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

#include "paimon/common/global_index/btree/btree_index_meta.h"

#include "paimon/common/memory/memory_slice_output.h"

namespace paimon {

std::shared_ptr<BTreeIndexMeta> BTreeIndexMeta::Deserialize(const std::shared_ptr<Bytes>& meta,
                                                            paimon::MemoryPool* pool) {
    auto slice = MemorySlice::Wrap(meta);
    auto input = slice.ToInput();
    auto first_key_len = input.ReadInt();
    std::shared_ptr<Bytes> first_key;
    if (first_key_len) {
        first_key = input.ReadSlice(first_key_len).CopyBytes(pool);
    }
    auto last_key_len = input.ReadInt();
    std::shared_ptr<Bytes> last_key;
    if (last_key_len) {
        last_key = input.ReadSlice(last_key_len).CopyBytes(pool);
    }
    auto has_nulls = static_cast<int8_t>(input.ReadByte()) == 1;
    return std::make_shared<BTreeIndexMeta>(first_key, last_key, has_nulls);
}

std::shared_ptr<Bytes> BTreeIndexMeta::Serialize(paimon::MemoryPool* pool) const {
    // Calculate total size: first_key_len(4) + first_key + last_key_len(4) + last_key +
    // has_nulls(1)
    int32_t first_key_size = first_key_ ? first_key_->size() : 0;
    int32_t last_key_size = last_key_ ? last_key_->size() : 0;
    int32_t total_size = Size();

    MemorySliceOutput output(total_size, pool);

    // Write first_key_len and first_key
    output.WriteValue(first_key_size);
    if (first_key_) {
        output.WriteBytes(first_key_);
    }

    // Write last_key_len and last_key
    output.WriteValue(last_key_size);
    if (last_key_) {
        output.WriteBytes(last_key_);
    }

    // Write has_nulls
    output.WriteValue(static_cast<int8_t>(has_nulls_ ? 1 : 0));

    return output.ToSlice().GetHeapMemory();
}

}  // namespace paimon
