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

#include "paimon/core/operation/commit/sequence_snapshot_properties.h"

#include <fmt/format.h>

#include <algorithm>
#include <limits>

#include "paimon/core/manifest/file_kind.h"

namespace paimon {

Result<std::optional<int64_t>> SequenceSnapshotProperties::MaxSequenceNumber(
    const std::optional<Snapshot>& snapshot) {
    if (!snapshot || !snapshot.value().Properties()) {
        return std::optional<int64_t>();
    }

    const auto& properties = snapshot.value().Properties().value();
    auto iter = properties.find(kMaxSequenceNumberKey);
    if (iter == properties.end()) {
        return std::optional<int64_t>();
    }

    try {
        size_t parsed = 0;
        int64_t value = std::stoll(iter->second, &parsed);
        if (parsed != iter->second.size()) {
            return Status::Invalid(
                fmt::format("Invalid {} value '{}': trailing characters are not allowed",
                            kMaxSequenceNumberKey, iter->second));
        }
        return std::optional<int64_t>(value);
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("Invalid {} value '{}': {}", kMaxSequenceNumberKey,
                                           iter->second, e.what()));
    }
}

std::optional<int64_t> SequenceSnapshotProperties::MaxSequenceNumberFromFiles(
    const std::vector<ManifestEntry>& files) {
    int64_t max_sequence_number = std::numeric_limits<int64_t>::min();
    bool found = false;
    for (const auto& file : files) {
        if (!(file.Kind() == FileKind::Add())) {
            continue;
        }
        max_sequence_number = std::max(max_sequence_number, file.File()->max_sequence_number);
        found = true;
    }

    if (!found) {
        return std::nullopt;
    }
    return max_sequence_number;
}

std::map<std::string, std::string> SequenceSnapshotProperties::MergeMaxSequenceNumber(
    const std::map<std::string, std::string>& properties,
    const std::optional<int64_t>& latest_max_sequence_number,
    const std::vector<ManifestEntry>& delta_files) {
    std::map<std::string, std::string> snapshot_properties = properties;

    std::optional<int64_t> delta_max_sequence_number = MaxSequenceNumberFromFiles(delta_files);
    if (delta_max_sequence_number || latest_max_sequence_number) {
        int64_t merged_max_sequence_number = latest_max_sequence_number
                                                 ? latest_max_sequence_number.value()
                                                 : delta_max_sequence_number.value();
        if (delta_max_sequence_number) {
            merged_max_sequence_number =
                std::max(merged_max_sequence_number, delta_max_sequence_number.value());
        }
        snapshot_properties[kMaxSequenceNumberKey] = std::to_string(merged_max_sequence_number);
    }

    return snapshot_properties;
}

}  // namespace paimon
