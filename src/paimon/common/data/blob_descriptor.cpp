/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/common/data/blob_descriptor.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/byte_order.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {

Result<std::unique_ptr<BlobDescriptor>> BlobDescriptor::Create(const std::string& uri,
                                                               int64_t offset, int64_t length) {
    return Create(kCurrentVersion, uri, offset, length);
}

Result<std::unique_ptr<BlobDescriptor>> BlobDescriptor::Create(int8_t version,
                                                               const std::string& uri,
                                                               int64_t offset, int64_t length) {
    if (offset < 0) {
        return Status::Invalid(fmt::format("offset {} is less than 0", offset));
    }
    // length == -1 means it's dynamic length
    if (length < -1) {
        return Status::Invalid(fmt::format("length {} is less than -1", length));
    }
    return std::unique_ptr<BlobDescriptor>(new BlobDescriptor(version, uri, offset, length));
}

PAIMON_UNIQUE_PTR<Bytes> BlobDescriptor::Serialize(const std::shared_ptr<MemoryPool>& pool) const {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    out.WriteValue<int8_t>(kCurrentVersion);
    out.WriteValue<int64_t>(kMagic);
    out.WriteValue<int32_t>(static_cast<int32_t>(uri_.size()));

    auto uri_bytes = std::make_shared<Bytes>(uri_, pool.get());
    out.WriteBytes(uri_bytes);
    out.WriteValue<int64_t>(offset_);
    out.WriteValue<int64_t>(length_);
    return MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
}

Result<std::unique_ptr<BlobDescriptor>> BlobDescriptor::Deserialize(const char* buffer,
                                                                    uint64_t size) {
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, size);
    DataInputStream in(std::move(input_stream));
    in.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    PAIMON_ASSIGN_OR_RAISE(int8_t version, in.ReadValue<int8_t>());
    if (version > kCurrentVersion) {
        return Status::Invalid(fmt::format(
            "Expecting BlobDescriptor version to be less than or equal to {}, but found {}.",
            kCurrentVersion, version));
    }
    if (version > 1) {
        PAIMON_ASSIGN_OR_RAISE(int64_t magic, in.ReadValue<int64_t>());
        if (kMagic != magic) {
            return Status::Invalid(fmt::format(
                "Invalid BlobDescriptor: missing magic header. Expected magic: {}, but found {}",
                kMagic, magic));
        }
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t uri_length, in.ReadValue<int32_t>());
    std::string uri(uri_length, '\0');
    PAIMON_RETURN_NOT_OK(in.Read(uri.data(), uri.size()));
    PAIMON_ASSIGN_OR_RAISE(int64_t offset, in.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t length, in.ReadValue<int64_t>());
    return BlobDescriptor::Create(version, uri, offset, length);
}

Result<bool> BlobDescriptor::IsBlobDescriptor(const char* buffer, uint64_t size) {
    if (size < kMinDescriptorLength) {
        return false;
    }
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, size);
    DataInputStream in(std::move(input_stream));
    in.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    PAIMON_ASSIGN_OR_RAISE(int8_t version, in.ReadValue<int8_t>());
    if (version > kCurrentVersion) {
        return false;
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t magic, in.ReadValue<int64_t>());
    return kMagic == magic;
}

std::string BlobDescriptor::ToString() const {
    return fmt::format("BlobDescriptor{{version={}, uri='{}', offset={}, length={}}}", version_,
                       uri_, offset_, length_);
}

}  // namespace paimon
