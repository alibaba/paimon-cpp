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

#include "paimon/common/data/generic_map.h"

#include <cassert>
#include <utility>

namespace paimon {

GenericMap::GenericMap(std::shared_ptr<InternalArray> key_array,
                       std::shared_ptr<InternalArray> value_array)
    : key_array_(std::move(key_array)), value_array_(std::move(value_array)) {
    assert(key_array_ && value_array_ && key_array_->Size() == value_array_->Size());
}

int32_t GenericMap::Size() const {
    return key_array_->Size();
}

std::shared_ptr<InternalArray> GenericMap::KeyArray() const {
    return key_array_;
}

std::shared_ptr<InternalArray> GenericMap::ValueArray() const {
    return value_array_;
}

}  // namespace paimon
