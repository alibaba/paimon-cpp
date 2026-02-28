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

#include "paimon/common/file_index/rangebitmap/dictionary/fixed_length_chunk.h"

#include <algorithm>
#include <cstring>

#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/common/file_index/rangebitmap/utils/literal_serialization_utils.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

Result<bool> FixedLengthChunk::TryAdd(const Literal& key) {
    if (keys_stream_out_ == nullptr) {
        keys_stream_out_ = std::make_shared<MemorySegmentOutputStream>(
            MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
        PAIMON_ASSIGN_OR_RAISE(serializer_,
                               LiteralSerDeUtils::CreateValueWriter(factory_->GetFieldType()));
    }
    if (fixed_length_ > remaining_keys_size_) {
        return false;
    }
    PAIMON_RETURN_NOT_OK(serializer_(keys_stream_out_, key));
    remaining_keys_size_ -= fixed_length_;
    size_ += 1;
    return true;
}

Result<Literal> FixedLengthChunk::GetKey(const int32_t index) {
    if (index < 0 || index >= size_) {
        return Status::Invalid("Index out of bounds");
    }
    if (keys_ == nullptr) {
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(keys_base_offset_ + offset_, FS_SEEK_SET));
        keys_ = Bytes::AllocateBytes(keys_length_, pool_.get());
        PAIMON_RETURN_NOT_OK(input_stream_->Read(keys_->data(), keys_length_));
        PAIMON_ASSIGN_OR_RAISE(deserializer_,
                               LiteralSerDeUtils::CreateValueReader(factory_->GetFieldType()));
        keys_stream_in_ = std::make_shared<DataInputStream>(
            std::make_shared<ByteArrayInputStream>(keys_->data(), keys_length_));
    }
    PAIMON_RETURN_NOT_OK(keys_stream_in_->Seek(index * fixed_length_));
    return deserializer_(keys_stream_in_, pool_.get());
}

Result<PAIMON_UNIQUE_PTR<Bytes>> FixedLengthChunk::SerializeChunk() const {
    const auto data_out = std::make_shared<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    data_out->WriteValue<int8_t>(CURRENT_VERSION);
    PAIMON_RETURN_NOT_OK(serializer_(data_out, key_));
    data_out->WriteValue<int32_t>(code_);
    data_out->WriteValue<int32_t>(offset_);
    data_out->WriteValue<int32_t>(size_);
    data_out->WriteValue<int32_t>(static_cast<int32_t>(keys_stream_out_->CurrentSize()));
    data_out->WriteValue<int32_t>(fixed_length_);
    return MemorySegmentUtils::CopyToBytes(
        data_out->Segments(), 0, static_cast<int32_t>(data_out->CurrentSize()), pool_.get());
}

Result<PAIMON_UNIQUE_PTR<Bytes>> FixedLengthChunk::SerializeKeys() const {
    return MemorySegmentUtils::CopyToBytes(keys_stream_out_->Segments(), 0,
                                           static_cast<int32_t>(keys_stream_out_->CurrentSize()),
                                           pool_.get());
}

// Read path
FixedLengthChunk::FixedLengthChunk(const std::shared_ptr<MemoryPool>& pool, Literal key,
                                   const int32_t code, const int32_t offset, const int32_t size,
                                   const std::shared_ptr<KeyFactory>& factory,
                                   const std::shared_ptr<InputStream>& input_stream,
                                   const int32_t keys_base_offset, const int32_t keys_length,
                                   const int32_t fixed_length)
    : pool_(pool),
      key_(std::move(key)),
      code_(code),
      offset_(offset),
      size_(size),
      factory_(factory),
      input_stream_(input_stream),
      keys_base_offset_(keys_base_offset),
      keys_length_(keys_length),
      fixed_length_(fixed_length),
      deserializer_({}),
      keys_stream_in_(nullptr),
      keys_(nullptr),
      remaining_keys_size_(0) {}

// Write path
FixedLengthChunk::FixedLengthChunk(const std::shared_ptr<MemoryPool>& pool, Literal key,
                                   const int32_t code, const int32_t keys_length_limit,
                                   const std::shared_ptr<KeyFactory>& factory,
                                   const int32_t fixed_length)
    : pool_(pool),
      key_(std::move(key)),
      code_(code),
      offset_(0),
      size_(0),
      factory_(factory),
      input_stream_(nullptr),
      keys_base_offset_(0),
      keys_length_(0),
      fixed_length_(fixed_length),
      deserializer_({}),
      keys_stream_in_(nullptr),
      keys_(nullptr),
      remaining_keys_size_(keys_length_limit) {}

}  // namespace paimon
