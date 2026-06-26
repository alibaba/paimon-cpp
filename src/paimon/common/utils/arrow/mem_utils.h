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

#include "arrow/memory_pool.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/visibility.h"

namespace paimon {

PAIMON_EXPORT std::unique_ptr<arrow::MemoryPool> GetArrowPool(
    const std::shared_ptr<MemoryPool>& pool);

}  // namespace paimon
