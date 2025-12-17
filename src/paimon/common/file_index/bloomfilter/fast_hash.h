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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "arrow/type.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {
/// Hash literal to 64 bits hash code.
class FastHash {
 public:
    FastHash() = delete;
    ~FastHash() = delete;

    // precondition: literal is not null
    using HashFunction = std::function<int64_t(const Literal& literal)>;

    static Result<HashFunction> GetHashFunction(const std::shared_ptr<arrow::DataType>& arrow_type);

 private:
    // Thomas Wang's integer hash function
    // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
    static int64_t GetLongHash(int64_t key);
    static int64_t Hash64(const char* data, size_t length);
};
}  // namespace paimon
