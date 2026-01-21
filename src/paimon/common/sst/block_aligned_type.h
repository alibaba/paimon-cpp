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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {

/// Enumeration for stream seek origin positions.
enum class PAIMON_EXPORT BlockAlignedType { ALIGNED = 0, UNALIGNED = 1 };

inline Result<BlockAlignedType> From(int8_t v) {
    if (v == 0) {
        return BlockAlignedType::ALIGNED;
    } else if (v == 1) {
        return BlockAlignedType::UNALIGNED;
    } else {
        return Status::Invalid("Invalid block aligned type: " + std::to_string(v));
    }
}

}  // namespace paimon
