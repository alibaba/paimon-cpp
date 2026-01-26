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

#include <cstddef>
#include <cstdint>

namespace paimon::orc {
// write options
static inline const char ORC_STRIPE_SIZE[] = "orc.stripe.size";
static constexpr size_t DEFAULT_STRIPE_SIZE = 64 * 1024 * 1024;

static inline const char ORC_ROW_INDEX_STRIDE[] = "orc.row.index.stride";
static constexpr size_t DEFAULT_ROW_INDEX_STRIDE = 10000;

static inline const char ORC_COMPRESSION_BLOCK_SIZE[] = "orc.compression.block-size";
static constexpr size_t DEFAULT_COMPRESSION_BLOCK_SIZE = 64 * 1024;

static inline const char ORC_DICTIONARY_KEY_SIZE_THRESHOLD[] = "orc.dictionary-key-size-threshold";
static constexpr double DEFAULT_DICTIONARY_KEY_SIZE_THRESHOLD = 0.8;
// default value of ORC_WRITE_ENABLE_METRICS is false
static inline const char ORC_WRITE_ENABLE_METRICS[] = "orc.write.enable-metrics";
// default value of ORC_TIMESTAMP_LTZ_LEGACY_TYPE is true. This option is used to be compatible with
// the paimon-orc's old behavior for the `timestamp_ltz` data type. Details at
// https://github.com/apache/paimon/issues/5066.
static inline const char ORC_TIMESTAMP_LTZ_LEGACY_TYPE[] = "orc.timestamp-ltz.legacy.type";

// read options
// default value of ORC_READ_ENABLE_LAZY_DECODING is false
static inline const char ORC_READ_ENABLE_LAZY_DECODING[] = "orc.read.enable-lazy-decoding";
static inline const char ORC_NATURAL_READ_SIZE[] = "orc.read.natural-read-size";
static constexpr uint64_t DEFAULT_NATURAL_READ_SIZE = 1024 * 1024;
// default value of ORC_READ_ENABLE_METRICS is false
static inline const char ORC_READ_ENABLE_METRICS[] = "orc.read.enable-metrics";

static constexpr uint64_t MIN_ROW_GROUP_COUNT_IN_ONE_NATURAL_READ = 1;
static inline const char ENABLE_PREFETCH_READ_SIZE_THRESHOLD[] =
    "orc.read.enable-prefetch-read-size-threshold";
// Prefetching will not be enabled if the total amount of data queried is below this threshold, as
// prefetching for very small data sets is not beneficial.
static constexpr uint64_t DEFAULT_ENABLE_PREFETCH_READ_SIZE_THRESHOLD = 10ull * 1024 * 1024;

}  // namespace paimon::orc
