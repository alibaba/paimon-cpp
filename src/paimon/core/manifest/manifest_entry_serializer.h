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

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/utils/versioned_object_serializer.h"
#include "paimon/result.h"

namespace arrow {
class ArrayBuilder;
}  // namespace arrow
struct ArrowArray;

namespace paimon {
class InternalRow;
class MemoryPool;

/// Serializer for `ManifestEntry`.
class ManifestEntrySerializer : public VersionedObjectSerializer<ManifestEntry> {
 public:
    explicit ManifestEntrySerializer(const std::shared_ptr<MemoryPool>& pool)
        : VersionedObjectSerializer<ManifestEntry>(pool),
          memory_pool_(pool_),
          data_file_meta_serializer_(pool) {}

    int32_t GetVersion() const override {
        return VERSION_2;
    }

    Result<ManifestEntry> ConvertFrom(int32_t version, const InternalRow& row) const override;

    Result<BinaryRow> ToRow(const ManifestEntry& record) const override;

 private:
    static constexpr int32_t VERSION_1 = 1;
    static constexpr int32_t VERSION_2 = 2;
    std::shared_ptr<MemoryPool> memory_pool_;
    DataFileMetaSerializer data_file_meta_serializer_;
};
}  // namespace paimon
