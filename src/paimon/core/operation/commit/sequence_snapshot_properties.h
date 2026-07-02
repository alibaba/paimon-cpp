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
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/snapshot.h"
#include "paimon/result.h"

namespace paimon {

class ManifestFile;
class ManifestFileMeta;

class SequenceSnapshotProperties {
 public:
    static constexpr const char* kMaxSequenceNumberKey = "sequence.generation.max-sequence-number";

    static Result<std::optional<int64_t>> MaxSequenceNumber(
        const std::optional<Snapshot>& snapshot);

    static std::optional<int64_t> MaxSequenceNumberFromFiles(
        const std::vector<ManifestEntry>& files);

    static std::map<std::string, std::string> MergeMaxSequenceNumber(
        const std::map<std::string, std::string>& properties,
        const std::optional<int64_t>& latest_max_sequence_number,
        const std::vector<ManifestEntry>& delta_files);

 private:
    SequenceSnapshotProperties() = delete;
};

}  // namespace paimon
