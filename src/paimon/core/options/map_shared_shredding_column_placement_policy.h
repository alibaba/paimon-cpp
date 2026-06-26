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

/// Specifies how shared-shredding MAP fields choose physical columns.
enum class MapSharedShreddingColumnPlacementPolicy {
    /// Keep input field order and place fields into columns 0..K-1.
    PLAIN = 0,
    /// Sort field IDs and place fields into columns 0..K-1.
    SEQUENTIAL = 1,
    /// Choose empty columns first, then the least-recently-used physical column.
    LRU = 2
};

}  // namespace paimon
