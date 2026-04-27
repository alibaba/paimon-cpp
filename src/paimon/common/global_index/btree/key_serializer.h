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

#include "arrow/api.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
namespace paimon {
class KeySerializer {
 public:
    KeySerializer() = delete;
    ~KeySerializer() = delete;

    static Result<std::shared_ptr<Bytes>> SerializeKey(const Literal& literal,
                                                       const std::shared_ptr<arrow::DataType>& type,
                                                       MemoryPool* pool);

    static Result<Literal> DeserializeKey(const MemorySlice& slice,
                                          const std::shared_ptr<arrow::DataType>& type,
                                          MemoryPool* pool);

    static MemorySlice::SliceComparator CreateComparator(
        const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<MemoryPool>& pool);
};
}  // namespace paimon
