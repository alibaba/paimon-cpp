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

#include "paimon/core/table/system/system_table_read.h"

#include <memory>
#include <utility>
#include <vector>

#include "paimon/core/table/system/system_table.h"

namespace paimon {

SystemTableRead::SystemTableRead(std::shared_ptr<SystemTable> system_table,
                                 const std::shared_ptr<MemoryPool>& memory_pool)
    : TableRead(memory_pool), system_table_(std::move(system_table)) {}

Result<std::unique_ptr<BatchReader>> SystemTableRead::CreateReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    return system_table_->CreateBatchReader(splits, GetMemoryPool());
}

Result<std::unique_ptr<BatchReader>> SystemTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    std::vector<std::shared_ptr<Split>> splits = {split};
    return CreateReader(splits);
}

}  // namespace paimon
