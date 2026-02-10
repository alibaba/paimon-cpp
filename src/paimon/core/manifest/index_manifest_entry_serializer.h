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

#include "arrow/status.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/utils/versioned_object_serializer.h"
#include "paimon/result.h"

struct ArrowArray;

namespace paimon {
class InternalRow;
class MemoryPool;

/// Serializer for `IndexManifestEntry`.
class IndexManifestEntrySerializer : public VersionedObjectSerializer<IndexManifestEntry> {
 public:
    explicit IndexManifestEntrySerializer(const std::shared_ptr<MemoryPool>& pool)
        : VersionedObjectSerializer<IndexManifestEntry>(pool) {}

    int32_t GetVersion() const override {
        return VERSION;
    }

    Result<IndexManifestEntry> ConvertFrom(int32_t version, const InternalRow& row) const override;

    Result<BinaryRow> ToRow(const IndexManifestEntry& record) const override;

 private:
    static constexpr int32_t VERSION = 1;
};
}  // namespace paimon
