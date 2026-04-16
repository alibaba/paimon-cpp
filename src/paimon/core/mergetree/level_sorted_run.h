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

#include "fmt/format.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/mergetree/sorted_run.h"
namespace paimon {
/// A `SortedRun` with level.
struct LevelSortedRun {
    LevelSortedRun(int32_t _level, const SortedRun& _run) : level(_level), run(_run) {}

    std::string ToString() const {
        return fmt::format("LevelSortedRun{{ level={}, run={} }}", level, run.ToString());
    }

    bool operator==(const LevelSortedRun& other) const {
        if (this == &other) {
            return true;
        }
        return level == other.level && run == other.run;
    }

    int32_t level;
    SortedRun run;
};
}  // namespace paimon
