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

#pragma once

#include <memory>
#include <vector>

#include "arrow/array/array_base.h"

namespace arrow {
class StructArray;
}  // namespace arrow

namespace paimon {
class MemoryPool;

struct ColumnarBatchContext {
    ColumnarBatchContext(std::shared_ptr<arrow::StructArray> struct_array_in,
                         const arrow::ArrayVector& array_vec_in,
                         std::shared_ptr<MemoryPool> pool_in)
        : struct_array(std::move(struct_array_in)),
          pool(std::move(pool_in)),
          array_vec_holder(array_vec_in) {
        array_vec.reserve(array_vec_holder.size());
        for (const auto& array : array_vec_holder) {
            array_vec.push_back(array.get());
        }
    }

    std::shared_ptr<arrow::StructArray> struct_array;
    std::shared_ptr<MemoryPool> pool;
    arrow::ArrayVector array_vec_holder;
    std::vector<const arrow::Array*> array_vec;
};
}  // namespace paimon
