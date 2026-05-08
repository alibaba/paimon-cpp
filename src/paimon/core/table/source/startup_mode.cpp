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

#include "paimon/table/source/startup_mode.h"

#include "fmt/format.h"
#include "paimon/status.h"

namespace paimon {

const StartupMode StartupMode::Default() {
    static const StartupMode mode = StartupMode("default");
    return mode;
}

const StartupMode StartupMode::LatestFull() {
    static const StartupMode mode = StartupMode("latest-full");
    return mode;
}
const StartupMode StartupMode::Latest() {
    static const StartupMode mode = StartupMode("latest");
    return mode;
}
const StartupMode StartupMode::FromSnapshot() {
    static const StartupMode mode = StartupMode("from-snapshot");
    return mode;
}
const StartupMode StartupMode::FromSnapshotFull() {
    static const StartupMode mode = StartupMode("from-snapshot-full");
    return mode;
}
const StartupMode StartupMode::FromTimestamp() {
    static const StartupMode mode = StartupMode("from-timestamp");
    return mode;
}

std::string StartupMode::ToString() const {
    return value_;
}

bool StartupMode::operator==(const StartupMode& other) const {
    if (this == &other) {
        return true;
    }
    return value_ == other.value_;
}

Result<StartupMode> StartupMode::FromString(const std::string& str) {
    if (str == StartupMode::Default().ToString()) {
        return StartupMode::Default();
    } else if (str == StartupMode::LatestFull().ToString()) {
        return StartupMode::LatestFull();
    } else if (str == StartupMode::Latest().ToString()) {
        return StartupMode::Latest();
    } else if (str == StartupMode::FromSnapshot().ToString()) {
        return StartupMode::FromSnapshot();
    } else if (str == StartupMode::FromSnapshotFull().ToString()) {
        return StartupMode::FromSnapshotFull();
    } else if (str == StartupMode::FromTimestamp().ToString()) {
        return StartupMode::FromTimestamp();
    } else {
        return Status::Invalid(fmt::format("invalid startup mode {}", str));
    }
}
}  // namespace paimon
