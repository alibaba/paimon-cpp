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
#include <limits>

namespace paimon {

/// A utility class for accessing special field IDs used in metadata.
class SpecialFieldIds {
 protected:
    /// System defined constant for field id boundary. Value: INT32_MAX - 10000
    static const int32_t CPP_FIELD_ID_END;

 public:
    /// Special field ID reserved for sequence number. Value: INT32_MAX - 1
    static const int32_t SEQUENCE_NUMBER;
    /// Special field ID reserved for value kind. Value: INT32_MAX - 2
    static const int32_t VALUE_KIND;
    /// Special field ID reserved for row ID. Value: INT32_MAX - 5
    static const int32_t ROW_ID;

    /// Special field ID reserved for index score. Value: CPP_FIELD_ID_END - 1
    static const int32_t INDEX_SCORE;
};

}  // namespace paimon
