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

#include "paimon/core/table/bucket_mode.h"

#include "paimon/core/schema/table_schema.h"

namespace paimon {

BucketMode ResolveBucketMode(int32_t bucket, const std::shared_ptr<TableSchema>& table_schema) {
    if (bucket == BucketModeDefine::POSTPONE_BUCKET) {
        return BucketMode::POSTPONE_MODE;
    }
    if (bucket == -1) {
        return table_schema->PrimaryKeys().empty() ? BucketMode::BUCKET_UNAWARE
                                                   : BucketMode::HASH_DYNAMIC;
    }
    if (bucket == BucketModeDefine::UNAWARE_BUCKET) {
        return BucketMode::BUCKET_UNAWARE;
    }
    return BucketMode::HASH_FIXED;
}

}  // namespace paimon
