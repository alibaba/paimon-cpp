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
#include <limits>
namespace paimon::parquet {

// write
static inline const char PARQUET_BLOCK_SIZE[] = "parquet.block.size";
static inline const char PARQUET_PAGE_SIZE[] = "parquet.page.size";
static inline const char PARQUET_DICTIONARY_PAGE_SIZE[] = "parquet.dictionary.page.size";
static inline const char PARQUET_ENABLE_DICTIONARY[] = "parquet.enable-dictionary";
static inline const char PARQUET_WRITER_VERSION[] = "parquet.writer.version";
static inline const char PARQUET_WRITE_MAX_ROW_GROUP_LENGTH[] =
    "parquet.write.max-row-group-length";
static constexpr int64_t DEFAULT_PARQUET_WRITE_MAX_ROW_GROUP_LENGTH =
    std::numeric_limits<int64_t>::max();
static inline const char PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL[] =
    "parquet.compression.codec.zstd.level";
static inline const char PARQUET_COMPRESSION_CODEC_ZLIB_LEVEL[] = "zlib.compress.level";
static inline const char PARQUET_COMPRESSION_CODEC_BROTLI_LEVEL[] = "compression.brotli.quality";
static inline const char PARQUET_WRITER_MAX_MEMORY_USE[] = "parquet.writer.max.memory.use";
static constexpr uint64_t DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE = 512 * 1024 * 1024;  // 512MB

// read
static inline const char PARQUET_READ_EXECUTOR_THREAD_COUNT[] =
    "parquet.read.executor.thread-count";
static constexpr uint32_t DEFAULT_PARQUET_READ_EXECUTOR_THREAD_COUNT = 3;
static inline const char PARQUET_READ_CACHE_OPTION_LAZY[] = "parquet.read.cache-option.lazy";
static inline const char PARQUET_READ_CACHE_OPTION_PREFETCH_LIMIT[] =
    "parquet.read.cache-option.prefetch-limit";
static inline const char PARQUET_READ_CACHE_OPTION_RANGE_SIZE_LIMIT[] =
    "parquet.read.cache-option.range-size-limit";

// stack-overflow may happen while the number of predicate node is too large, limit the number of
// predicate nodes. Predicate will not be pushdown when exceed limit.
static inline const char PARQUET_READ_PREDICATE_NODE_COUNT_LIMIT[] =
    "parquet.read.predicate-node-count-limit";

// Default is true. Compaction will set to false to reduce memory consumption.
static inline const char PARQUET_READ_ENABLE_PRE_BUFFER[] = "parquet.read.enable-pre-buffer";

static constexpr uint32_t DEFAULT_PARQUET_READ_CACHE_OPTION_PREFETCH_LIMIT = 0;
static constexpr uint32_t DEFAULT_PARQUET_READ_CACHE_OPTION_RANGE_SIZE_LIMIT = 32 * 1024 * 1024;
static constexpr uint32_t DEFAULT_PARQUET_READ_PREDICATE_NODE_COUNT_LIMIT = 512;

class ParquetMetrics {
 public:
    static inline const char WRITE_RECORD_COUNT[] = "parquet.write.record.count";

    // read
    static inline const char READ_ROW_GROUPS_TOTAL[] = "parquet.read.row-groups.total";
    static inline const char READ_ROW_GROUPS_AFTER_FILTER[] =
        "parquet.read.row-groups.after-filter";
    static inline const char READ_ROWS[] = "parquet.read.rows";
    static inline const char READ_BATCH_COUNT[] = "parquet.read.batch-count";
};

}  // namespace paimon::parquet
