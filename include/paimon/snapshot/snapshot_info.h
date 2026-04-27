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
#include <optional>
#include <string>

#include "paimon/visibility.h"

namespace paimon {

/// Plain snapshot metadata returned by Catalog::ListSnapshots().
struct PAIMON_EXPORT SnapshotInfo {
    /// Commit kind exposed through the public Catalog API.
    enum class PAIMON_EXPORT CommitKind : int8_t {
        APPEND,
        COMPACT,
        OVERWRITE,
        ANALYZE,
        UNKNOWN,
    };

    /// Convert a CommitKind to its canonical string representation.
    static std::string CommitKindToString(CommitKind kind);

    int64_t snapshot_id;
    int64_t schema_id;
    std::string commit_user;
    CommitKind commit_kind;
    int64_t time_millis;
    std::optional<int64_t> total_record_count;
    std::optional<int64_t> delta_record_count;
    std::optional<int64_t> watermark;
};

}  // namespace paimon
