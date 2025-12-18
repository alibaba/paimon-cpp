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

#include "paimon/core/table/source/fallback_table_read.h"

#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/fallback_data_split.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"

namespace paimon {
Result<std::unique_ptr<BatchReader>> FallbackTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    auto fallback_data_split = std::dynamic_pointer_cast<FallbackDataSplit>(split);
    if (fallback_data_split) {
        if (fallback_data_split->IsFallback()) {
            return fallback_table_->CreateReader(fallback_data_split->GetSplit());
        } else {
            return main_table_->CreateReader(fallback_data_split->GetSplit());
        }
    }
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    if (!data_split) {
        return Status::Invalid("cannot cast split to data split");
    }
    return main_table_->CreateReader(data_split);
}

}  // namespace paimon
