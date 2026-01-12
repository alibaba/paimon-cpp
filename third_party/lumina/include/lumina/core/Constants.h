/*
 * Copyright 2025-present Alibaba Inc.
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
#include <string_view>
namespace lumina::core {

// Use modern C++ string constant definitions.
// Provide compile-time string constants via constexpr and std::string_view.

constexpr uint32_t LUMINA_CURRENT_VERSION = 0;
// Prime magic number
constexpr uint32_t LUMINA_MAGIC_NUMBER = 19260817u;

// Basic options
constexpr std::string_view kIndexPrefix = "index.";
constexpr std::string_view kDimension = "index.dimension";
constexpr std::string_view kIndexPath = "index.path";
constexpr std::string_view kSnapshotPath = "index.snapshot_path";
constexpr std::string_view kIndexType = "index.type"; // Index type
// Available index.type values
constexpr std::string_view kIndexTypeBruteforce = "bruteforce";
constexpr std::string_view kIndexTypeDemo = "demo";
constexpr std::string_view kIndexTypeHnsw = "hnsw";
constexpr std::string_view kIndexTypeIvf = "ivf";
constexpr std::string_view kIndexTypeDiskANN = "diskann";

// Distance-related options
constexpr std::string_view kDistancePrefix = "distance.";
constexpr std::string_view kDistanceMetric = "distance.metric"; // Distance metric
// Available distance.metric values
constexpr std::string_view kDistanceL2 = "l2";
constexpr std::string_view kDistanceCosine = "cosine";
constexpr std::string_view kDistanceInnerProduct = "inner_product";

// Vector encoding options
constexpr std::string_view kEncodingPrefix = "encoding.";
constexpr std::string_view kEncodingType = "encoding.type"; // Encoding type
// Available encoding.type values
constexpr std::string_view kEncodingRawf32 = "rawf32";
constexpr std::string_view kEncodingSQ8 = "sq8";
constexpr std::string_view kEncodingPQ = "pq";
constexpr std::string_view kEncodingDummy = "dummy";

// IO options
constexpr std::string_view kIOPrefix = "io.";
constexpr std::string_view kFileReaderType = "io.reader.type"; // File reader type
constexpr std::string_view kIOVerifyCrc = "io.verify_crc";     // Enable section CRC verification
// Available io.reader.type values
constexpr std::string_view kFileReaderLocal = "local";
constexpr std::string_view kFileReaderMmap = "mmap";
constexpr std::string_view kFileReaderMmapLockMode = "io.reader.mmap.lock_mode"; // mmap lock mode
// Available mmap lock modes
constexpr std::string_view kMmapLockModeNone = "none";
constexpr std::string_view kMmapLockModeMlock = "mlock";
constexpr std::string_view kMmapLockModePopulate = "populate";

// Build options
constexpr std::string_view kPretrainSampleRatio = "pretrain.sample_ratio";
constexpr std::string_view kThresholdOfLog = "build.log_threshold";

// Search options
constexpr std::string_view kSearchPrefix = "search.";
constexpr std::string_view kTopK = "search.topk";
constexpr std::string_view kSearchParallelNumber = "search.parallel_number";
constexpr std::string_view kSearchThreadSafeFilter = "search.thread_safe_filter";
// IVF search option (may be reused by other indexes)
constexpr std::string_view kSearchNprobe = "search.nprobe";

// Extension options
constexpr std::string_view kExtensionPrefix = "extension.";
constexpr std::string_view kExtensionSearchWithFilter = "extension.search_with_filter";
constexpr std::string_view kExtensionCkptThreshold = "extension.build.ckpt.threshold";
constexpr std::string_view kExtensionCkptCount = "extension.build.ckpt.count";

/* constexpr std::string_view kExtensionFilterDsl = "filter.dsl"; */
/* constexpr std::string_view kExtensionFilterTags = "filter.tags"; */
/* constexpr std::string_view kExtensionFilterMode = "filter.mode"; */
/* constexpr std::string_view kExtensionTimeoutMs = "timeout.ms"; */

// Query stats options (session-level)
constexpr std::string_view kQueryStatsPrefix = "query_stats.";
constexpr std::string_view kQueryStatsDistanceCalculateCount = "query_stats.distance_calculate_count";
constexpr std::string_view kQueryStatsFilteredCount = "query_stats.filtered_count";

/* // Other common options */

// Vector math constants
constexpr double CAL_EPS = 1e-8;

} // namespace lumina::core
