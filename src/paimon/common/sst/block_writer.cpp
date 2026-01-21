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

#include "paimon/common/sst/block_writer.h"

#include "paimon/common/sst/block_aligned_type.h"

namespace paimon {

void BlockWriter::Write(std::shared_ptr<Bytes>& key, std::shared_ptr<Bytes>& value) {
    int start_position = block_->Size();
    block_->WriteVarLenInt(key->size());
    block_->WriteBytes(key);
    block_->WriteVarLenInt(value->size());
    block_->WriteBytes(value);
    int end_position = block_->Size();
    positions_.push_back(start_position);
    if (aligned_) {
        int current_size = end_position - start_position;
        if (aligned_size_ == 0) {
            aligned_size_ = current_size;
        } else {
            aligned_ = aligned_size_ == current_size;
        }
    }
}

void BlockWriter::Reset() {
    positions_.clear();
    block_ = std::make_shared<MemorySliceOutput>(size_, pool_.get());
    aligned_size_ = 0;
    aligned_ = true;
}

Result<std::unique_ptr<paimon::MemorySlice>> BlockWriter::Finish() {
    if (positions_.size() == 0) {
        // Do not use alignment mode, as it is impossible to calculate how many records are
        // inside when reading
        aligned_ = false;
    }
    if (aligned_) {
        block_->WriteValue(aligned_size_);
    } else {
        for (auto& position : positions_) {
            block_->WriteValue(position);
        }
        block_->WriteValue(static_cast<int32_t>(positions_.size()));
    }
    block_->WriteValue(aligned_ ? static_cast<char>(BlockAlignedType::ALIGNED)
                                : static_cast<char>(BlockAlignedType::UNALIGNED));
    return block_->ToSlice();
}

}  // namespace paimon
