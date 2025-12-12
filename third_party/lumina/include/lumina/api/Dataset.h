/*
 * Copyright 2025-present Alibaba Inc.
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
#include <vector>

#include "lumina/core/NoCopyable.h"
#include "lumina/core/Result.h"
#include "lumina/core/Status.h"
#include "lumina/core/Types.h"

namespace lumina::api {

class Dataset : public core::NoCopyable
{
public:
    virtual ~Dataset() = default;

    virtual uint32_t Dim() const = 0;
    virtual uint64_t TotalSize() const = 0;

    virtual core::Result<uint64_t> GetNextBatch(std::vector<float>& vectorBuffer,
                                                std::vector<core::VectorId>& idBuffer) = 0;
};

}
