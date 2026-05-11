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

#include <memory>
#include <vector>

#include "paimon/table/source/table_read.h"

namespace paimon {
class SystemTable;

class SystemTableRead : public TableRead {
 public:
    SystemTableRead(std::shared_ptr<SystemTable> system_table,
                    const std::shared_ptr<MemoryPool>& memory_pool);

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& splits) override;
    Result<std::unique_ptr<BatchReader>> CreateReader(const std::shared_ptr<Split>& split) override;

 private:
    std::shared_ptr<SystemTable> system_table_;
};

}  // namespace paimon
