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
/// Strategy for lookup.
struct LookupStrategy {
    LookupStrategy(bool _is_first_row, bool _produce_changelog, bool _deletion_vector,
                   bool _force_lookup)
        : is_first_row(_is_first_row),
          produce_changelog(_produce_changelog),
          deletion_vector(_deletion_vector),
          need_lookup(_produce_changelog || _deletion_vector || _is_first_row || _force_lookup) {}

    bool operator==(const LookupStrategy& other) const {
        if (this == &other) {
            return true;
        }
        return is_first_row == other.is_first_row && produce_changelog == other.produce_changelog &&
               deletion_vector == other.deletion_vector && need_lookup == other.need_lookup;
    }

    bool is_first_row;
    bool produce_changelog;
    bool deletion_vector;
    bool need_lookup;
};
}  // namespace paimon
