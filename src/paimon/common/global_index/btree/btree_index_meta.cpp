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

namespace paimon {

std::shared_ptr<BTreeIndexMeta> BTreeIndexMeta::Deserialize(const std::shared_ptr<Bytes>& meta,
                                                            paimon::MemoryPool* pool) {
    auto input = MemorySlice::Wrap(meta)->ToInput();
    auto first_key_len = input->ReadInt();
    std::shared_ptr<Bytes> first_key;
    if (first_key_len) {
        first_key = std::move(input->ReadSlice(first_key_len)->CopyBytes(pool));
    }
    auto last_key_len = input->ReadInt();
    std::shared_ptr<Bytes> last_key;
    if (last_key_len) {
        last_key = std::move(input->ReadSlice(last_key_len)->CopyBytes(pool));
    }
    auto has_nulls = input->ReadByte() == 1;
    return std::make_shared<BTreeIndexMeta>(first_key, last_key, has_nulls);
}

std::shared_ptr<Bytes> BTreeIndexMeta::Serialize(paimon::MemoryPool* pool) {
    // TODO(zhangchaoming.zcm): Implement serialization
    // For now, return an empty Bytes object
    return std::make_shared<Bytes>();
}

}  // namespace paimon
