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
    /// Keep the key order from each input MAP row and place the first K keys into columns 0..K-1.
    /// Use this when the input order is meaningful and should directly decide column placement.
    /// Example:
    ///   K=2
    ///   row [c, a, b] -> ordered [c, a, b] -> columns [c, a], overflow [b]
    ///   row [a, b, c] -> ordered [a, b, c] -> columns [a, b], overflow [c]
    PLAIN = 0,
    /// Use the shared-shredding metadata order before placing the first K keys into columns 0..K-1.
    /// Use this when each row should choose leading-column keys by a deterministic metadata
    /// order instead of the input MAP entry order, and no cross-row history should be used.
    /// Example:
    ///   K=2, metadata order [b, c, a]
    ///   row [c, a, b] -> ordered [b, c, a] -> columns [b, c], overflow [a]
    ///   row [c, a]    -> ordered [c, a]    -> columns [c, a], overflow []
    /// This does not reserve key-to-column mappings across rows.
    SEQUENTIAL = 1,
    /// Reuse columns for recently seen keys when possible; otherwise choose an empty column first,
    /// then the least-recently-used physical column.
    /// Use this when hot keys are likely to appear repeatedly across nearby rows and should stay
    /// in physical columns when possible.
    /// Example:
    ///   K=3, existing columns [0:a, 1:b, 2:d]
    ///   row [d, c, a, b] -> ordered [a, b, c, d]
    ///   columns [a, b, d], overflow [c]
    /// If a key has already been evicted, it is treated like a new key when it appears again.
    LRU = 2
};

}  // namespace paimon
