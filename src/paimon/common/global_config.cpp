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

#include "paimon/global_config.h"

#include "arrow/util/thread_pool.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {
PAIMON_EXPORT int GetArrowCpuThreadPoolCapacity() {
    return arrow::GetCpuThreadPoolCapacity();
}

PAIMON_EXPORT Status SetArrowCpuThreadPoolCapacity(int threads) {
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::SetCpuThreadPoolCapacity(threads));
    return Status::OK();
}
}  // namespace paimon
