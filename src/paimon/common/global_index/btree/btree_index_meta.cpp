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
namespace {

std::shared_ptr<Bytes> ReadKey(MemorySliceInput* input, int32_t key_length, MemoryPool* pool) {
    if (key_length == 0) {
        return std::make_shared<Bytes>(0, pool);
    }
    return input->ReadSliceView(key_length).CopyBytes(pool);
}

}  // namespace

std::shared_ptr<BTreeIndexMeta> BTreeIndexMeta::Deserialize(const std::shared_ptr<Bytes>& meta,
                                                            paimon::MemoryPool* pool) {
    MemorySlice slice = MemorySlice::Wrap(meta);
    MemorySliceInput input = slice.ToInput();
    int32_t first_key_len = input.ReadInt();
    std::shared_ptr<Bytes> first_key = ReadKey(&input, first_key_len, pool);
    int32_t last_key_len = input.ReadInt();
    std::shared_ptr<Bytes> last_key = ReadKey(&input, last_key_len, pool);
    bool has_nulls = input.ReadByte() == static_cast<int8_t>(1);

    if (input.Available() >= 2) {
        int8_t format_version = input.ReadByte();
        if (format_version == kFormatVersionWithNullFlags) {
            int8_t null_key_flags = input.ReadByte();
            if ((null_key_flags & kFirstKeyIsNull) != 0) {
                first_key.reset();
            }
            if ((null_key_flags & kLastKeyIsNull) != 0) {
                last_key.reset();
            }
        }
    } else if (first_key_len == 0 && last_key_len == 0 && has_nulls) {
        // Legacy metadata used zero length for null keys. Both empty boundaries plus a null bitmap
        // identify an all-null file; a single empty boundary remains a valid serialized key.
        first_key.reset();
        last_key.reset();
    }
    return std::make_shared<BTreeIndexMeta>(first_key, last_key, has_nulls);
}

std::shared_ptr<Bytes> BTreeIndexMeta::Serialize(paimon::MemoryPool* pool) const {
    int32_t first_key_size = first_key_ ? first_key_->size() : 0;
    int32_t last_key_size = last_key_ ? last_key_->size() : 0;
    int32_t total_size = Size();
    MemorySliceOutput output(total_size, pool);
    int8_t null_key_flags = 0;

    output.WriteValue(first_key_size);
    if (first_key_) {
        output.WriteBytes(first_key_);
    } else {
        null_key_flags |= kFirstKeyIsNull;
    }

    output.WriteValue(last_key_size);
    if (last_key_) {
        output.WriteBytes(last_key_);
    } else {
        null_key_flags |= kLastKeyIsNull;
    }

    output.WriteValue(static_cast<int8_t>(has_nulls_ ? 1 : 0));
    output.WriteValue(kFormatVersionWithNullFlags);
    output.WriteValue(null_key_flags);

    return output.ToSlice().GetOrCreateHeapMemory(pool);
}

}  // namespace paimon
