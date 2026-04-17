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

#include "paimon/common/file_index/rangebitmap/bit_slice_index_bitmap.h"

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <string>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

Result<std::unique_ptr<BitSliceIndexBitmap>> BitSliceIndexBitmap::Create(
    const std::shared_ptr<InputStream>& input_stream, int32_t offset,
    const std::shared_ptr<MemoryPool>& pool) {
    const auto data_in = std::make_unique<DataInputStream>(input_stream);
    PAIMON_RETURN_NOT_OK(data_in->Seek(offset));
    PAIMON_ASSIGN_OR_RAISE(int32_t header_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int8_t version, data_in->ReadValue<int8_t>());
    if (version != kCurrentVersion) {
        return Status::Invalid(fmt::format("Unknown BitSliceBitmap Version: {}", version));
    }
    PAIMON_ASSIGN_OR_RAISE(int8_t slices_size, data_in->ReadValue<int8_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t ebm_size, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t indexes_length, data_in->ReadValue<int32_t>());
    auto indexes = Bytes::AllocateBytes(indexes_length, pool.get());
    PAIMON_RETURN_NOT_OK(data_in->Read(indexes->data(), indexes_length));
    return std::unique_ptr<BitSliceIndexBitmap>(new BitSliceIndexBitmap(
        indexes_length, std::move(indexes), ebm_size, slices_size, input_stream,
        static_cast<int32_t>(offset + sizeof(int32_t) + header_length), pool));
}

static int32_t NumberOfLeadingZeros(int64_t value) {
    if (value == 0) {
        return 64;
    }
    return __builtin_clzll(static_cast<uint64_t>(value));
}

static int32_t NumberOfTrailingZeros(int64_t value) {
    if (value == 0) {
        return 64;
    }
    return __builtin_ctzll(static_cast<uint64_t>(value));
}

BitSliceIndexBitmap::BitSliceIndexBitmap(int32_t indexes_length, PAIMON_UNIQUE_PTR<Bytes> indexes,
                                         int32_t ebm_length, int32_t slices_size,
                                         const std::shared_ptr<InputStream>& input_stream,
                                         int32_t body_offset,
                                         const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      bit_slices_(std::vector<std::optional<RoaringBitmap32>>(slices_size, {std::nullopt})),
      ebm_({std::nullopt}),
      input_stream_(input_stream),
      body_offset_(body_offset),
      indexes_(std::move(indexes)),
      ebm_length_(ebm_length),
      indexes_length_(indexes_length),
      batch_loaded_(false) {}

Result<const RoaringBitmap32*> BitSliceIndexBitmap::GetExistenceBitmap() {
    if (!ebm_.has_value()) {
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(body_offset_, FS_SEEK_SET));
        const auto bytes = Bytes::AllocateBytes(ebm_length_, pool_.get());
        PAIMON_RETURN_NOT_OK(input_stream_->Read(bytes->data(), ebm_length_));
        RoaringBitmap32 bitmap;
        PAIMON_RETURN_NOT_OK(bitmap.Deserialize(bytes->data(), ebm_length_));
        ebm_ = std::move(bitmap);
    }
    return &ebm_.value();
}

Result<const RoaringBitmap32*> BitSliceIndexBitmap::GetSliceBitmap(int32_t idx) {
    if (!bit_slices_[idx].has_value()) {
        PAIMON_RETURN_NOT_OK(LoadSlices(idx, idx + 1));
    }
    return &bit_slices_[idx].value();
}

Status BitSliceIndexBitmap::LoadSlices(int32_t start, int32_t end) {
    if (start == end) {
        return Status::OK();
    }
    auto indexes_stream = std::make_shared<ByteArrayInputStream>(indexes_->data(), indexes_length_);
    const auto data_in = std::make_unique<DataInputStream>(indexes_stream);
    const auto position = static_cast<int32_t>(2 * sizeof(int32_t) * start);
    PAIMON_RETURN_NOT_OK(data_in->Seek(position));
    PAIMON_ASSIGN_OR_RAISE(int32_t offset, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t length, data_in->ReadValue<int32_t>());
    std::vector<int32_t> lengths(end);
    lengths[start] = length;

    for (int32_t i = start + 1; i < end; ++i) {
        PAIMON_RETURN_NOT_OK(data_in->ReadValue<int32_t>());
        PAIMON_ASSIGN_OR_RAISE(int32_t slice_length, data_in->ReadValue<int32_t>());
        lengths[i] = slice_length;
        length += slice_length;
    }
    PAIMON_RETURN_NOT_OK(input_stream_->Seek(body_offset_ + ebm_length_ + offset, FS_SEEK_SET));
    const auto bytes = Bytes::AllocateBytes(length, pool_.get());
    PAIMON_RETURN_NOT_OK(input_stream_->Read(bytes->data(), length));
    int32_t byte_position = 0;
    for (int32_t i = start; i < end; ++i) {
        const int32_t slice_length = lengths[i];
        RoaringBitmap32 bitmap;
        PAIMON_RETURN_NOT_OK(bitmap.Deserialize(bytes->data() + byte_position, slice_length));
        bit_slices_[i] = std::move(bitmap);
        byte_position += slice_length;
    }
    return Status::OK();
}

Result<RoaringBitmap32> BitSliceIndexBitmap::Eq(int32_t code) {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* existence_bitmap, GetExistenceBitmap());
    auto equal = RoaringBitmap32(*existence_bitmap);
    if (!batch_loaded_) {
        PAIMON_RETURN_NOT_OK(LoadSlices(0, static_cast<int32_t>(bit_slices_.size())));
        batch_loaded_ = true;
    }
    // process slices from LSB to MSB
    for (int32_t i = 0; i < static_cast<int32_t>(bit_slices_.size()); ++i) {
        PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* slice_bitmap, GetSliceBitmap(i));
        if ((code >> i & 1) == 1) {
            // Intersect with rows that also have this bit turned on
            equal &= *slice_bitmap;
        } else {
            // Exclude rows that should NOT have bit turned on
            equal -= *slice_bitmap;
        }
    }
    return equal;
}

Result<RoaringBitmap32> BitSliceIndexBitmap::Gt(int32_t code) {
    if (code < 0) {
        return IsNotNull({});
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 found_set, IsNotNull({}));
    if (found_set.IsEmpty()) {
        return RoaringBitmap32();
    }
    auto state = RoaringBitmap32{};
    auto state_inited = false;
    // skip ones from LSB
    const auto start = NumberOfTrailingZeros(~code);
    if (!batch_loaded_) {
        PAIMON_RETURN_NOT_OK(LoadSlices(start, static_cast<int32_t>(bit_slices_.size())));
        batch_loaded_ = true;
    }
    // process slices from LSB to MSB
    for (int32_t i = start; i < static_cast<int32_t>(bit_slices_.size()); ++i) {
        if (!state_inited) {
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* slice_ptr, GetSliceBitmap(i));
            // current i_th bit of code has to be 0 after skip,
            // so the state is initialized to rows that is greater than the code's 0 to i bits
            state = *slice_ptr;
            state_inited = true;
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* slice_ptr, GetSliceBitmap(i));
        if ((code >> i & 1) == 1) {
            // when code is 1 at the higher bit
            // only rows that is 1 at the higher bit remain
            state &= *slice_ptr;
        } else {
            // when code is 0 at the higher bit
            // union rows that is 1 at the higher bit
            state |= *slice_ptr;
        }
    }
    if (!state_inited) {
        return RoaringBitmap32();
    }
    return state &= found_set;
}

Result<RoaringBitmap32> BitSliceIndexBitmap::Gte(int32_t code) {
    if (code < 0) {
        return IsNotNull({});
    }
    return Gt(code - 1);
}

Result<RoaringBitmap32> BitSliceIndexBitmap::IsNotNull(const RoaringBitmap32& found_set) {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* existence_bitmap, GetExistenceBitmap());
    return found_set.IsEmpty() ? *existence_bitmap
                               : RoaringBitmap32::And(*existence_bitmap, found_set);
}
Result<std::unique_ptr<BitSliceIndexBitmap::Appender>> BitSliceIndexBitmap::Appender::Create(
    int32_t min, int32_t max, const std::shared_ptr<MemoryPool>& pool) {
    if (min > max) {
        return Status::Invalid(fmt::format("min {} > max {}", min, max));
    }
    if (min < 0) {
        return Status::Invalid(fmt::format("min {} cannot be negative", min));
    }
    return std::unique_ptr<Appender>(new Appender(min, max, pool));
}

BitSliceIndexBitmap::Appender::Appender(int32_t min, int32_t max,
                                        const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool), min_(min), max_(max) {
    ebm_ = RoaringBitmap32{};
    const auto slices_size = std::max(64 - NumberOfLeadingZeros(max), 1);
    slices_.resize(slices_size);
}

Status BitSliceIndexBitmap::Appender::Append(int32_t key, int32_t value) {
    if (key < 0) {
        return Status::Invalid(fmt::format("key: {} cannot be negative", key));
    }
    if (value < 0) {
        return Status::Invalid(fmt::format("value: {} cannot be negative", value));
    }
    if (value < min_ || value > max_) {
        return Status::Invalid(fmt::format("value: {} not in range [{}, {}]", value, min_, max_));
    }
    int32_t bits = value;
    while (bits != 0) {
        slices_[NumberOfTrailingZeros(bits)].Add(key);
        bits &= (bits - 1);
    }
    ebm_.Add(key);
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> BitSliceIndexBitmap::Appender::Serialize() const {
    const auto indexes_length = static_cast<int32_t>(2 * sizeof(int32_t) * slices_.size());
    const auto ebm_bytes = ebm_.Serialize(pool_.get());
    const auto ebm_length = static_cast<int32_t>(ebm_bytes->size());
    int32_t header_size = 0;
    header_size += sizeof(int8_t);   // version
    header_size += sizeof(int8_t);   // slices size
    header_size += sizeof(int32_t);  // ebm length
    header_size += sizeof(int32_t);  // indexes length
    header_size += indexes_length;
    int32_t offset = 0;
    const auto data_output_stream = std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    auto slices_bytes_vector = std::vector<PAIMON_UNIQUE_PTR<Bytes>>{};
    auto indexes_vector = std::vector<std::pair<int32_t, int32_t>>{};
    for (const auto& slice : slices_) {
        auto slice_bytes = slice.Serialize(pool_.get());
        const auto length = static_cast<int32_t>(slice_bytes->size());
        indexes_vector.emplace_back(offset, length);
        offset += length;
        slices_bytes_vector.emplace_back(std::move(slice_bytes));
    }
    data_output_stream->WriteValue<int32_t>(header_size);
    data_output_stream->WriteValue<int8_t>(kCurrentVersion);
    data_output_stream->WriteValue<int8_t>(static_cast<int8_t>(slices_.size()));
    data_output_stream->WriteValue<int32_t>(ebm_length);
    data_output_stream->WriteValue<int32_t>(indexes_length);
    for (const auto& [slice_offset, length] : indexes_vector) {
        data_output_stream->WriteValue<int32_t>(slice_offset);
        data_output_stream->WriteValue<int32_t>(length);
    }
    data_output_stream->Write(ebm_bytes->data(), ebm_length);
    for (const auto& slice_bytes : slices_bytes_vector) {
        data_output_stream->Write(slice_bytes->data(), slice_bytes->size());
    }
    return MemorySegmentUtils::CopyToBytes(data_output_stream->Segments(), 0,
                                           static_cast<int32_t>(data_output_stream->CurrentSize()),
                                           pool_.get());
}
}  // namespace paimon
