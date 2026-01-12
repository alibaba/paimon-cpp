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
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>
#include <lumina/core/Types.h>
#include <vector>

namespace lumina::api {

class Dataset : public core::NoCopyable
{
public:
    virtual ~Dataset() = default;

    /** Vector dimension. Caller must keep it consistent with the Builder dimension. */
    virtual uint32_t Dim() const = 0;
    /** Total data size (optional, for pre-allocation). Return 0 if unknown. */
    virtual uint64_t TotalSize() const = 0;
    /**
     * Fetch the next batch. Implementations should clear and fill the buffers (not append).
     * vectorBuffer size must be rows * Dim(), and idBuffer size must be rows.
     * Return value: number of vectors in this batch; return 0 at end; return Status on error.
     */
    virtual core::Result<uint64_t> GetNextBatch(std::vector<float>& vectorBuffer,
                                                std::vector<core::vector_id_t>& idBuffer) = 0;
};

} // namespace lumina::api
