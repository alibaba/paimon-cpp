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

#include "fmt/format.h"
#include "paimon/status.h"

namespace paimon::parquet {

// Convert any std::exception thrown by underlying Parquet/Arrow APIs into a
// Status. Used as the trailing catch clauses of a try block in every public
// method that calls into the parquet C++ API, so the read layer never throws.
#define PAIMON_PARQUET_CATCH_AND_RETURN_STATUS(context)                           \
    catch (const std::exception& e) {                                             \
        return Status::Invalid(fmt::format("{}: {}", (context), e.what()));       \
    }                                                                             \
    catch (...) {                                                                 \
        return Status::UnknownError(fmt::format("{}: unknown error", (context))); \
    }

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

// Enable writing page index (ColumnIndex + OffsetIndex) for page-level filtering on read
static inline const char PARQUET_WRITE_ENABLE_PAGE_INDEX[] = "parquet.write.enable-page-index";
static constexpr bool DEFAULT_PARQUET_WRITE_ENABLE_PAGE_INDEX = true;

// read
static inline const char PARQUET_READ_EXECUTOR_THREAD_COUNT[] =
    "parquet.read.executor.thread-count";
static constexpr uint32_t DEFAULT_PARQUET_READ_EXECUTOR_THREAD_COUNT = 3;
static inline const char PARQUET_READ_CACHE_OPTION_LAZY[] = "parquet.read.cache-option.lazy";
static inline const char PARQUET_READ_CACHE_OPTION_PREFETCH_LIMIT[] =
    "parquet.read.cache-option.prefetch-limit";
static inline const char PARQUET_READ_CACHE_OPTION_RANGE_SIZE_LIMIT[] =
    "parquet.read.cache-option.range-size-limit";
// Strategy for refining row ranges using the selection bitmap produced by pushed-down
// predicates. Two options:
//   * "coalesce" (default): build row-level ranges from the bitmap, then merge nearby
//     ranges whose gap is small enough (see PARQUET_READ_ROW_RANGES_COALESCE_HOLE_SIZE_LIMIT).
//   * "trim": for each page with selected rows, trim its leading/trailing non-selected
//     rows (and skip pages with no selected rows). Requires page index. Its advantage
//     is stable, page-bounded ranges. After collected trimmed ranges for each column,
//     the intersection of all columns' ranges is taken to produce the final row ranges.
static inline const char PARQUET_READ_BITMAP_ROW_RANGE_REFINING_STRATEGY[] =
    "parquet.read.bitmap.row-range-refining-strategy";
// When strategy = "coalesce", adjacent bitmap row ranges whose gap (in rows) is
// <= this limit are merged into one range; larger gaps are kept as real holes.
// A larger limit means fewer (larger) ranges and more wasted rows read; a smaller
// limit keeps the selection tighter at the cost of more (smaller) ranges.
// Only takes effect when PARQUET_READ_BITMAP_ROW_RANGE_REFINING_STRATEGY = "coalesce".
static inline const char PARQUET_READ_ROW_RANGES_COALESCE_HOLE_SIZE_LIMIT[] =
    "parquet.read.bitmap.coalesce-hole-size-limit";

// stack-overflow may happen while the number of predicate node is too large, limit the number of
// predicate nodes. Predicate will not be pushdown when exceed limit.
static inline const char PARQUET_READ_PREDICATE_NODE_COUNT_LIMIT[] =
    "parquet.read.predicate-node-count-limit";

// Enable page-level filtering using column index
static inline const char PARQUET_READ_ENABLE_PAGE_INDEX_FILTER[] =
    "parquet.read.enable-page-index-filter";

// Default is true. Compaction will set to false to reduce memory consumption.
static inline const char PARQUET_READ_ENABLE_PRE_BUFFER[] = "parquet.read.enable-pre-buffer";

static constexpr uint32_t DEFAULT_PARQUET_READ_CACHE_OPTION_PREFETCH_LIMIT = 0;
static constexpr uint32_t DEFAULT_PARQUET_READ_CACHE_OPTION_RANGE_SIZE_LIMIT = 32 * 1024 * 1024;
static constexpr uint32_t DEFAULT_PARQUET_READ_PREDICATE_NODE_COUNT_LIMIT = 512;
static constexpr bool DEFAULT_PARQUET_READ_ENABLE_PAGE_INDEX_FILTER = true;
static constexpr char DEFAULT_PARQUET_READ_BITMAP_STRATEGY[] = "coalesce";
static constexpr uint32_t DEFAULT_PARQUET_READ_ROW_RANGES_COALESCE_HOLE_SIZE_LIMIT = 32;

class ParquetMetrics {
 public:
    static inline const char WRITE_RECORD_COUNT[] = "parquet.write.record.count";

    // read
    static inline const char READ_ROW_GROUPS_TOTAL[] = "parquet.read.row-groups.total";
    static inline const char READ_ROW_GROUPS_AFTER_FILTER[] =
        "parquet.read.row-groups.after-filter";
    static inline const char READ_ROWS[] = "parquet.read.rows";
    static inline const char READ_BATCH_COUNT[] = "parquet.read.batch-count";
    // Byte-level read metric. storage-read-bytes: physical bytes read from storage.
    static inline const char READ_STORAGE_BYTES[] = "parquet.read.storage-read-bytes";
};

}  // namespace paimon::parquet
