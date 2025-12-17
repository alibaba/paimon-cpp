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

#include <string>
#include <vector>

#include "paimon/common/data/data_getters.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
/// Base interface of an internal data structure representing data of ArrayType.
/// @note All elements of this data structure must be internal data structures and must be of the
/// same type. See `InternalRow` for more information about internal data structures.
class PAIMON_EXPORT InternalArray : public DataGetters {
 public:
    /// @return the number of elements in this array.
    virtual int32_t Size() const = 0;

    /// ToBooleanArray return std::vector<char> rather than vector<bool>, as vector<bool> is not
    /// safe
    virtual Result<std::vector<char>> ToBooleanArray() const = 0;
    virtual Result<std::vector<char>> ToByteArray() const = 0;
    virtual Result<std::vector<int16_t>> ToShortArray() const = 0;
    virtual Result<std::vector<int32_t>> ToIntArray() const = 0;
    virtual Result<std::vector<int64_t>> ToLongArray() const = 0;
    virtual Result<std::vector<float>> ToFloatArray() const = 0;
    virtual Result<std::vector<double>> ToDoubleArray() const = 0;

    std::string ToString() const {
        assert(false);
        return "";
    }
};

}  // namespace paimon
