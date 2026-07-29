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

#include <memory>

#include "paimon/common/data/internal_map.h"

namespace paimon {

/// An InternalMap backed by separate key and value arrays.
class GenericMap : public InternalMap {
 public:
    /// Create a map from equally sized key and value arrays.
    ///
    /// @param key_array Keys stored in the map.
    /// @param value_array Values stored in the map.
    GenericMap(std::shared_ptr<InternalArray> key_array,
               std::shared_ptr<InternalArray> value_array);

    int32_t Size() const override;
    std::shared_ptr<InternalArray> KeyArray() const override;
    std::shared_ptr<InternalArray> ValueArray() const override;

 private:
    std::shared_ptr<InternalArray> key_array_;
    std::shared_ptr<InternalArray> value_array_;
};

}  // namespace paimon
