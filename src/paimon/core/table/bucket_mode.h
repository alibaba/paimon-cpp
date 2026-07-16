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

#include <cstdint>
#include <memory>

namespace paimon {

class TableSchema;

/// Bucket mode of the table, it affects the writing process and also affects the data skipping in
/// reading.
enum class BucketMode {
    /// The fixed number of buckets configured by the user can only be modified through offline
    /// commands. The data is distributed to the corresponding buckets according to the hash value
    /// of
    /// bucket key (default is primary key), and the reading end can perform bucket skipping based
    /// on
    /// the filtering conditions of the bucket key.
    HASH_FIXED = 0,

    /// The dynamic bucket mode records which bucket the key corresponds to through the index files.
    /// The index records the correspondence between the hash value of the primary-key and the
    /// bucket. This mode cannot support multiple concurrent writes or bucket skipping for reading
    /// filter conditions. This mode only works for changelog table.
    HASH_DYNAMIC,

    /// The cross partition mode is for cross partition upsert (primary keys not contain all
    /// partition fields). It directly maintains the mapping of primary keys to partition and
    /// bucket,
    /// uses local disks, and initializes indexes by reading all existing keys in the table when
    /// starting stream write job.
    CROSS_PARTITION,

    /// Ignoring bucket concept, although all data is written to bucket-0, the parallelism of reads
    /// and writes is unrestricted. This mode only works for append-only table.
    BUCKET_UNAWARE,

    /// Configured by 'bucket' = '-2' (postpone bucket) for primary key table. This mode aims to
    /// solve the difficulty to determine a fixed number of buckets and support different buckets
    /// for
    /// different partitions. The bucket will be adaptively adjusted to the appropriate value in the
    /// background.
    POSTPONE_MODE
};

class BucketModeDefine {
 public:
    static constexpr int32_t UNAWARE_BUCKET = 0;
    static constexpr int32_t POSTPONE_BUCKET = -2;
};

BucketMode ResolveBucketMode(int32_t bucket, const std::shared_ptr<TableSchema>& table_schema);

}  // namespace paimon
