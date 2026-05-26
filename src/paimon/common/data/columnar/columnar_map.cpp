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

#include "paimon/common/data/columnar/columnar_map.h"

#include "paimon/common/data/columnar/columnar_array.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon {
class MemoryPool;

ColumnarMap::ColumnarMap(const std::shared_ptr<arrow::Array>& key_array,
                         const std::shared_ptr<arrow::Array>& value_array,
                         const std::shared_ptr<MemoryPool>& pool, int32_t offset, int32_t length)
    : pool_(pool),
      key_array_(key_array),
      value_array_(value_array),
      offset_(offset),
      length_(length) {}

std::shared_ptr<InternalArray> ColumnarMap::KeyArray() const {
    return std::make_shared<ColumnarArray>(key_array_.get(), pool_, offset_, length_);
}
std::shared_ptr<InternalArray> ColumnarMap::ValueArray() const {
    return std::make_shared<ColumnarArray>(value_array_.get(), pool_, offset_, length_);
}

}  // namespace paimon
