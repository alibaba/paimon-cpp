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
#include "paimon/common/data/blob_view_struct.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/murmurhash_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/byte_order.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {
PAIMON_UNIQUE_PTR<Bytes> BlobViewStruct::Serialize(const std::shared_ptr<MemoryPool>& pool) const {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);

    out.WriteValue<int8_t>(kCurrentVersion);
    out.WriteValue<int64_t>(kMagic);
    std::string identifier = identifier_.GetFullName();
    out.WriteValue<int32_t>(static_cast<int32_t>(identifier.size()));
    auto uri_bytes = std::make_shared<Bytes>(identifier, pool.get());
    out.WriteBytes(uri_bytes);
    out.WriteValue<int32_t>(field_id_);
    out.WriteValue<int64_t>(row_id_);
    return MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
}

Result<std::unique_ptr<BlobViewStruct>> BlobViewStruct::Deserialize(const char* buffer,
                                                                    uint64_t size) {
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, size);
    DataInputStream in(std::move(input_stream));
    in.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);

    PAIMON_ASSIGN_OR_RAISE(int8_t version, in.ReadValue<int8_t>());
    if (version != kCurrentVersion) {
        return Status::Invalid(fmt::format(
            "Expecting BlobViewStruct version to be {}, but found {}.", kCurrentVersion, version));
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t magic, in.ReadValue<int64_t>());
    if (kMagic != magic) {
        return Status::Invalid(
            fmt::format("Invalid BlobViewStruct: missing magic header. Expected magic: {}, "
                        "but found {}",
                        kMagic, magic));
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t length, in.ReadValue<int32_t>());
    std::string identifier_str(length, '\0');
    PAIMON_RETURN_NOT_OK(in.Read(identifier_str.data(), identifier_str.size()));
    PAIMON_ASSIGN_OR_RAISE(int32_t field_id, in.ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t row_id, in.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(Identifier identifier, Identifier::FromString(identifier_str));
    return std::make_unique<BlobViewStruct>(identifier, field_id, row_id);
}

Result<bool> BlobViewStruct::IsBlobViewStruct(const char* buffer, uint64_t size) {
    if (size < kMinViewLength) {
        return false;
    }
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, size);
    DataInputStream in(std::move(input_stream));
    in.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);

    PAIMON_ASSIGN_OR_RAISE(int8_t version, in.ReadValue<int8_t>());
    if (version != kCurrentVersion) {
        return false;
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t magic, in.ReadValue<int64_t>());
    return kMagic == magic;
}

std::string BlobViewStruct::ToString() const {
    return fmt::format("BlobViewStruct{{identifier={}, fieldId={}, rowId={}}}",
                       identifier_.GetFullName(), field_id_, row_id_);
}

bool BlobViewStruct::operator==(const BlobViewStruct& other) const {
    if (this == &other) {
        return true;
    }
    return field_id_ == other.field_id_ && row_id_ == other.row_id_ &&
           identifier_ == other.identifier_;
}

bool BlobViewStruct::operator!=(const BlobViewStruct& other) const {
    return !(*this == other);
}

int32_t BlobViewStruct::HashCode() const {
    int32_t hash =
        MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<const void*>(&field_id_),
                                         /*offset=*/0, sizeof(field_id_), identifier_.HashCode());
    return MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<const void*>(&row_id_),
                                            /*offset=*/0, sizeof(row_id_), hash);
}
}  // namespace paimon
