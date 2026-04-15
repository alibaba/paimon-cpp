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

namespace paimon {
/// The compact mode for lookup compaction.
enum class LookupCompactMode {
    /// Lookup compaction will use ForceUpLevel0Compaction strategy to radically compact new files.
    RADICAL = 1,
    /// Lookup compaction will use UniversalCompaction strategy to gently compact new files.
    GENTLE = 2
};
}  // namespace paimon
