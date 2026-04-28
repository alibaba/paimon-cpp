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
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
/// Specifies the startup mode for log consumer.
class PAIMON_EXPORT StartupMode {
 public:
    /// Determines actual startup mode according to other table properties. If "scan.snapshot-id" is
    /// set the actual startup mode will be "from-snapshot", otherwise the actual startup mode will
    /// be "latest-full".
    static const StartupMode Default();

    /// For streaming sources, produces the latest snapshot on the table upon first startup, and
    /// continue to read the latest changes. For batch sources, just produce the latest snapshot but
    /// does not read new changes.
    static const StartupMode LatestFull();

    /// For streaming sources, continuously reads latest changes without producing a snapshot at the
    /// beginning. For batch sources, behaves the same as the "latest-full" startup mode.
    static const StartupMode Latest();

    /// For streaming sources, continuously reads changes starting from snapshot specified by
    /// "scan.snapshot-id", without producing a snapshot at the beginning. For batch sources,
    /// produces a snapshot specified by "scan.snapshot-id" but does not read new changes.
    static const StartupMode FromSnapshot();

    /// For streaming sources, produces from snapshot specified by "scan.snapshot-id" on the table
    /// upon first startup, and continuously reads changes. For batch sources, produces a snapshot
    /// specified by "scan.snapshot-id" but does not read new changes
    static const StartupMode FromSnapshotFull();

    /// Starts from a timestamp specified by either "scan.timestamp-millis" or
    /// "scan.timestamp". For batch sources, produces the latest snapshot whose
    /// timestamp is <= the specified timestamp. For streaming sources, continuously
    /// reads changes starting from the first snapshot at or after the timestamp.
    static const StartupMode FromTimestamp();

 public:
    std::string ToString() const;
    bool operator==(const StartupMode& other) const;
    static Result<StartupMode> FromString(const std::string& str);

 private:
    explicit StartupMode(const std::string& value) : value_(value) {}

 private:
    std::string value_;
};
}  // namespace paimon
