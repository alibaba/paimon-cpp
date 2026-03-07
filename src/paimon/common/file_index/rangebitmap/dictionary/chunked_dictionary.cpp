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

#include "paimon/common/file_index/rangebitmap/dictionary/chunked_dictionary.h"

#include <algorithm>

#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/fixed_length_chunk.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

Result<int32_t> ChunkedDictionary::Find(const Literal& key) {
    int32_t low = 0;
    int32_t high = size_ - 1;
    while (low <= high) {
        const int32_t mid = low + (high - low) / 2;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Chunk> chunk, GetChunk(mid));
        PAIMON_ASSIGN_OR_RAISE(const int32_t result, factory_->CompareLiteral(chunk->Key(), key));
        if (result > 0) {
            high = mid - 1;
        } else if (result < 0) {
            low = mid + 1;
        } else {
            return chunk->Code();
        }
    }
    if (low == 0) {
        return -(low + 1);  // This makes sure the return value is negative if not found
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Chunk> prev_chunk, GetChunk(low - 1));
    return prev_chunk->Find(key);
}

Result<Literal> ChunkedDictionary::Find(int32_t code) {
    if (size_ <= 0) {
        return Status::Invalid(fmt::format("Cannot find code {} in an empty Dictionary", code));
    }
    if (code < 0) {
        return Status::Invalid(fmt::format("Invalid code: {}", code));
    }
    int32_t low = 0;
    int32_t high = size_ - 1;

    while (low <= high) {
        const int32_t mid = low + (high - low) / 2;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Chunk> chunk, GetChunk(mid));

        int32_t chunk_code = chunk->Code();
        if (chunk_code > code) {
            high = mid - 1;
        } else if (chunk_code < code) {
            low = mid + 1;
        } else {
            return {chunk->Key()};
        }
    }
    if (low == 0) {
        return Status::Invalid(fmt::format("Cannot find code {}", code));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Chunk> prev_chunk, GetChunk(low - 1));
    return prev_chunk->Find(code);
}

Result<std::shared_ptr<Chunk>> ChunkedDictionary::GetChunk(int32_t index) {
    if (index < 0 || index >= size_) {
        return Status::Invalid(fmt::format("Invalid chunk index: {}", index));
    }
    if (offsets_bytes_ == nullptr || chunks_bytes_ == nullptr) {
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(body_offset_, FS_SEEK_SET));
        auto offsets = Bytes::AllocateBytes(offsets_length_, pool_.get());
        PAIMON_RETURN_NOT_OK(input_stream_->Read(offsets->data(), offsets_length_));
        offsets_bytes_ = std::move(offsets);
        auto chunks = Bytes::AllocateBytes(chunks_length_, pool_.get());
        PAIMON_RETURN_NOT_OK(input_stream_->Read(chunks->data(), chunks_length_));
        chunks_bytes_ = std::move(chunks);
    }
    if (chunks_cache_[index]) {
        return chunks_cache_[index];
    }
    auto data_in = std::make_unique<DataInputStream>(
        std::make_shared<ByteArrayInputStream>(offsets_bytes_->data(), offsets_length_));
    PAIMON_RETURN_NOT_OK(data_in->Seek(sizeof(int32_t) * index));
    PAIMON_ASSIGN_OR_RAISE(int32_t chunk_offset, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<Chunk> chunk,
        factory_->MmapChunk(input_stream_, body_offset_ + offsets_length_ + chunk_offset,
                            body_offset_ + chunks_length_ + offsets_length_, pool_));
    chunks_cache_[index] = std::move(chunk);
    return chunks_cache_[index];
}

Result<std::unique_ptr<ChunkedDictionary::Appender>> ChunkedDictionary::Appender::Create(
    const std::shared_ptr<KeyFactory>& key_factory, int32_t chunk_size_bytes,
    const std::shared_ptr<MemoryPool>& pool) {
    return std::unique_ptr<Appender>(new Appender(key_factory, chunk_size_bytes, pool));
}

ChunkedDictionary::Appender::Appender(const std::shared_ptr<KeyFactory>& key_factory,
                                      int32_t chunk_size_bytes,
                                      const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      key_factory_(key_factory),
      chunk_size_bytes_(chunk_size_bytes),
      chunk_(nullptr),
      size_(0),
      key_offset_(0),
      chunks_offset_(0) {
    chunks_output_ = std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    keys_output_ = std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    offsets_output_ = std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
}

Status ChunkedDictionary::Appender::AppendSorted(const Literal& key, int32_t code) {
    if (key.IsNull()) {
        return Status::Invalid("key should not be null");
    }
    if (last_key_.has_value()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t compare_result,
                               key_factory_->CompareLiteral(*last_key_, key));
        if (compare_result >= 0) {
            return Status::Invalid("key must be in sorted order");
        }
    }
    if (last_code_.has_value() && code != *last_code_ + 1) {
        return Status::Invalid("code must be in sorted order and increment by one");
    }
    last_key_ = key;
    last_code_ = code;
    if (chunk_ == nullptr) {
        PAIMON_ASSIGN_OR_RAISE(chunk_,
                               key_factory_->CreateChunk(key, code, chunk_size_bytes_, pool_));
    } else {
        PAIMON_ASSIGN_OR_RAISE(bool success, chunk_->TryAdd(key));
        if (success) {
            return Status::OK();
        }
        PAIMON_RETURN_NOT_OK(Flush());
        PAIMON_ASSIGN_OR_RAISE(chunk_,
                               key_factory_->CreateChunk(key, code, chunk_size_bytes_, pool_));
    }
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> ChunkedDictionary::Appender::Serialize() {
    if (chunk_ != nullptr) {
        PAIMON_RETURN_NOT_OK(Flush());
    }
    int32_t header_size = 0;
    header_size += sizeof(int8_t);   // version
    header_size += sizeof(int32_t);  // size
    header_size += sizeof(int32_t);  // offsets length
    header_size += sizeof(int32_t);  // chunks length
    auto data_out = std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    data_out->WriteValue<int32_t>(header_size);
    data_out->WriteValue<int8_t>(kCurrentVersion);
    data_out->WriteValue<int32_t>(size_);
    data_out->WriteValue<int32_t>(static_cast<int32_t>(offsets_output_->CurrentSize()));
    data_out->WriteValue<int32_t>(static_cast<int32_t>(chunks_output_->CurrentSize()));
    PAIMON_RETURN_NOT_OK(MemorySegmentUtils::CopyToStream(
        offsets_output_->Segments(), 0, static_cast<int32_t>(offsets_output_->CurrentSize()),
        data_out.get()));
    PAIMON_RETURN_NOT_OK(MemorySegmentUtils::CopyToStream(
        chunks_output_->Segments(), 0, static_cast<int32_t>(chunks_output_->CurrentSize()),
        data_out.get()));
    PAIMON_RETURN_NOT_OK(MemorySegmentUtils::CopyToStream(
        keys_output_->Segments(), 0, static_cast<int32_t>(keys_output_->CurrentSize()),
        data_out.get()));
    return MemorySegmentUtils::CopyToBytes(
        data_out->Segments(), 0, static_cast<int32_t>(data_out->CurrentSize()), pool_.get());
}

Status ChunkedDictionary::Appender::Flush() {
    chunk_->SetOffset(key_offset_);
    PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> chunks_bytes, chunk_->SerializeChunk());
    PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> keys_bytes, chunk_->SerializeKeys());
    offsets_output_->WriteValue<int32_t>(chunks_offset_);
    chunks_offset_ += static_cast<int32_t>(chunks_bytes->size());
    key_offset_ += static_cast<int32_t>(keys_bytes->size());
    chunks_output_->Write(chunks_bytes->data(), chunks_bytes->size());
    keys_output_->Write(keys_bytes->data(), keys_bytes->size());
    size_ += 1;
    chunk_ = nullptr;
    return Status::OK();
}

Result<std::unique_ptr<ChunkedDictionary>> ChunkedDictionary::Create(
    FieldType field_type, const std::shared_ptr<InputStream>& input_stream, int32_t offset,
    const std::shared_ptr<MemoryPool>& pool) {
    auto data_in = std::make_unique<DataInputStream>(input_stream);
    PAIMON_RETURN_NOT_OK(data_in->Seek(offset));
    PAIMON_ASSIGN_OR_RAISE(int32_t header_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int8_t version, data_in->ReadValue<int8_t>());
    if (version != kCurrentVersion) {
        return Status::Invalid("Unknown version of ChunkedDictionary");
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t size, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t offsets_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t chunks_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeyFactory> factory_shared,
                           KeyFactory::Create(field_type));
    auto result = std::unique_ptr<ChunkedDictionary>(new ChunkedDictionary(
        input_stream, factory_shared, size, offsets_length, chunks_length,
        static_cast<int32_t>(offset + header_length + sizeof(int32_t)), pool));
    return result;
}

ChunkedDictionary::ChunkedDictionary(const std::shared_ptr<InputStream>& input_stream,
                                     const std::shared_ptr<KeyFactory>& factory, int32_t size,
                                     int32_t offsets_length, int32_t chunks_length,
                                     int32_t body_offset, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      factory_(factory),
      input_stream_(input_stream),
      size_(size),
      offsets_length_(offsets_length),
      chunks_length_(chunks_length),
      body_offset_(body_offset),
      offsets_bytes_(nullptr),
      chunks_bytes_(nullptr),
      chunks_cache_(std::vector<std::shared_ptr<Chunk>>(size)) {}
}  // namespace paimon
