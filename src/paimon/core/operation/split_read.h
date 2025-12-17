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
#include <vector>

#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/table/source/data_split.h"

namespace paimon {
/// Given a DataSplit or a list of DataSplit, generate a reader for batch reading.
class SplitRead {
 public:
    virtual ~SplitRead() = default;

    virtual Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<Split>& split) = 0;

    virtual Result<bool> Match(const std::shared_ptr<Split>& split,
                               bool force_keep_delete) const = 0;
};

}  // namespace paimon
