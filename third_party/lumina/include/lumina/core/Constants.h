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
#include <cstdint>
#include <string_view>
namespace lumina::core {

// Use modern C++ string constant definitions.
// Provide compile-time string constants via constexpr and std::string_view.

constexpr uint32_t LUMINA_CURRENT_VERSION = 0;
// Prime magic number
constexpr uint32_t LUMINA_MAGIC_NUMBER = 19260817u;

// Recommended file suffix for Lumina persisted artifacts (not enforced by IO).
constexpr std::string_view kLuminaFileSuffix = ".lmi";

// Basic options
constexpr std::string_view kIndexPrefix = "index.";
constexpr std::string_view kDimension = "index.dimension";
constexpr std::string_view kIndexPath = "index.path";
constexpr std::string_view kSnapshotPath = "index.snapshot_path";
constexpr std::string_view kIndexType = "index.type"; // Index type
// Available index.type values
constexpr std::string_view kIndexTypeBruteforce = "bruteforce";
constexpr std::string_view kIndexTypeDemo = "demo";
constexpr std::string_view kIndexTypeFlatnsw = "flatnsw";
constexpr std::string_view kIndexTypeIvf = "ivf";
constexpr std::string_view kIndexTypeDiskANN = "diskann";

// Experimental index types
// These index types are under development and may change without notice.
// Users must explicitly enable experimental features via kEnableExperimental option.
inline constexpr std::string_view kEnableExperimental = "index.enable_experimental";

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
constexpr std::string_view kEncodingRabitQ = "rabitq";
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
constexpr std::string_view kExtensionSearchWithTag = "extension.search_with_tag";
constexpr std::string_view kExtensionCkptThreshold = "extension.build.ckpt.threshold";
constexpr std::string_view kExtensionCkptCount = "extension.build.ckpt.count";
constexpr std::string_view kExtensionGetVector = "extension.search.get_vector";
constexpr std::string_view kExtensionTagSchema = "extension.build.tag.tag_schema";
constexpr std::string_view kExtensionTagMaxRangeLabelRatio = "extension.build.tag.max_range_label_ratio";
// Available extension.build.tag.tag_schema keys
constexpr std::string_view kExtensionTagKName = "key_name";
constexpr std::string_view kExtensionTagType = "type";
constexpr std::string_view kExtensionTagVType = "value_type";
// Maximum length of a extension.build.tag.tag_schema key_name
constexpr uint16_t kMaxTagKNameLength = UINT16_MAX;
// Available extension.build.tag.tag_schema type values
constexpr std::string_view kExtensionTagTypeEnum = "enum";
constexpr std::string_view kExtensionTagTypeRange = "range";
// Available extension.build.tag.tag_schema value_type values
constexpr std::string_view kExtensionTagVTypeString = "string";
constexpr std::string_view kExtensionTagVTypeInt64 = "int64";
constexpr std::string_view kExtensionTagVTypeDouble = "double";
// DistributeBuildCombinedExtension
constexpr std::string_view kExtensionDistributeBuildPrefix = "extension.build.distribute_build.";
constexpr std::string_view kExtensionDistributeBuildDispatchCount =
    "extension.build.distribute_build.partition.dispatch_count";
constexpr std::string_view kExtensionDistributeBuildPartitionCount =
    "extension.build.distribute_build.partition.partition_count";
constexpr std::string_view kExtensionDistributeBuildCentroidCount =
    "extension.build.distribute_build.partition.centroid_count";
constexpr std::string_view kExtensionDistributeBuildPartitionType = "extension.build.distribute_build.partition.type";
constexpr std::string_view kExtensionDistributeBuildKmeans = "kmeans";
constexpr std::string_view kExtensionDistributeBuildMaxKmeansEpoch =
    "extension.build.distribute_build.partition.kmeans.max_epoch";
constexpr std::string_view kExtensionDistributeBuildKmeansThreadCount =
    "extension.build.distribute_build.partition.kmeans.thread_count";
// Query stats options (session-level)
constexpr std::string_view kQueryStatsPrefix = "query_stats.";
constexpr std::string_view kQueryStatsDistanceCalculateCount = "query_stats.distance_calculate_count";
constexpr std::string_view kQueryStatsFilteredCount = "query_stats.filtered_count";

// Vector math constants
constexpr double CAL_EPS = 1e-8;

} // namespace lumina::core
