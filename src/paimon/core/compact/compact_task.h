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

#include "paimon/core/compact/compact_result.h"
#include "paimon/result.h"

namespace paimon {

/// Compact task.
class CompactTask {
 public:
    virtual ~CompactTask() = default;
    // TODO(yonghao.fyh): support metrics
    Result<std::shared_ptr<CompactResult>> Execute() {
        return DoCompact();
    }

 protected:
    virtual Result<std::shared_ptr<CompactResult>> DoCompact() = 0;
};

}  // namespace paimon
