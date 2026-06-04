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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon {
/// Serialized metadata for a BLOB view field.
/// A blob view only stores the coordinates needed to locate the original blob value in the
/// upstream table: identifier, field_id and row_id. The actual blob data is
/// resolved at read time by scanning the upstream table.
class BlobViewStruct {
 public:
    BlobViewStruct(const Identifier& identifier, int32_t field_id, int64_t row_id)
        : identifier_(identifier), field_id_(field_id), row_id_(row_id) {}

    const Identifier& GetIdentifier() const {
        return identifier_;
    }

    int32_t FieldId() const {
        return field_id_;
    }

    int64_t RowId() const {
        return row_id_;
    }

    static Result<std::unique_ptr<BlobViewStruct>> Deserialize(const char* buffer, uint64_t size);
    static Result<bool> IsBlobViewStruct(const char* buffer, uint64_t size);
    PAIMON_UNIQUE_PTR<Bytes> Serialize(const std::shared_ptr<MemoryPool>& pool) const;
    std::string ToString() const;
    int32_t HashCode() const;

    bool operator==(const BlobViewStruct& other) const;
    bool operator!=(const BlobViewStruct& other) const;

 private:
    static constexpr int64_t kMagic = 0x424C4F4256494557l;
    static constexpr int8_t kCurrentVersion = 1;
    /// one byte for version, eight bytes for magic number.
    static constexpr uint64_t kMinViewLength = 9;

    Identifier identifier_;
    int32_t field_id_;
    int64_t row_id_;
};

/// Resolves a BlobViewStruct into the serialized BlobDescriptor bytes stored in the upstream
/// table. Returns nullptr when the referenced source-table cell is null.
using BlobViewResolver = std::function<Result<std::shared_ptr<Bytes>>(const BlobViewStruct&)>;

}  // namespace paimon

namespace std {
template <>
struct hash<paimon::BlobViewStruct> {
    size_t operator()(const paimon::BlobViewStruct& blob_view_struct) const {
        return blob_view_struct.HashCode();
    }
};

}  // namespace std
