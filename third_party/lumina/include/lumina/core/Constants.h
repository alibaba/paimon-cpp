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


constexpr uint32_t LUMINA_CURRENT_VERSION = 0;

constexpr std::string_view kDimension = "dimension";
constexpr std::string_view kTopK = "topk";
constexpr std::string_view kIndexType = "indextype";
constexpr std::string_view kSearchParallelNumber = "search.parallelnumber";
constexpr std::string_view kSearchThreadCount = "search.threadcount";
constexpr std::string_view kSearchThreadSafeFilter = "search.threadsafefilter";

constexpr std::string_view kIndexTypeBruteforce = "bruteforce";
constexpr std::string_view kIndexTypeDemo = "demo";
constexpr std::string_view kIndexTypeHnsw = "hnsw";
constexpr std::string_view kIndexTypeIvf = "ivf";
constexpr std::string_view kIndexTypeDiskANN = "diskann";

constexpr std::string_view kIvfBuildNumLists = "ivf.build.numlists";
constexpr std::string_view kIvfBuildThreadCount = "ivf.build.threadcount";
constexpr std::string_view kIvfBuildMaxEpoch = "ivf.build.maxepoch";
constexpr std::string_view kSearchNprobe = "search.nprobe";

constexpr std::string_view kDiskannBuildThreadCount = "diskann.build.threadcount";
constexpr std::string_view kDiskannBuildEfConstruction = "diskann.build.ef_construction";
constexpr std::string_view kDiskannBuildSlackPruningFactor = "diskann.build.slack_pruning_factor";
constexpr std::string_view kDiskannBuildNeighborCount = "diskann.build.neighbor_count";
constexpr std::string_view kDiskannSearchNumNodesToCache = "diskann.search.numnodestocache";
constexpr std::string_view kDiskannSearchListSize = "diskann.search.listsize";
constexpr std::string_view kDiskannSearchBeamWidth = "diskann.search.beamwidth";
constexpr std::string_view kDiskannSearchIOLimit = "diskann.search.iolimit";
constexpr std::string_view kDiskannSearchUseReorderData = "diskann.search.usereorderdata";


constexpr std::string_view kPretrainSampleRatio = "pretrain.sampleratio";

constexpr std::string_view kDistancePrefix = "distance.";
constexpr std::string_view kDistanceMetric = "distance.metric";
constexpr std::string_view kDistanceL2 = "l2";
constexpr std::string_view kDistanceCosine = "cosine";
constexpr std::string_view kDistanceInnerProduct = "innerproduct";

constexpr std::string_view kEncodingType = "encoding.type";
constexpr std::string_view kEncodingRawf32 = "encoding.rawf32";
constexpr std::string_view kEncodingSQ8 = "encoding.sq8";
constexpr std::string_view kEncodingPQ = "encoding.pq";
constexpr std::string_view kEncodingDummy = "encoding.dummy";

constexpr std::string_view kIsaScalar = "scalar";
constexpr std::string_view kIsaAvx = "avx";
constexpr std::string_view kIsaAvx2 = "avx2";
constexpr std::string_view kIsaAvx512 = "avx512";
constexpr std::string_view kIsaSse = "sse";
constexpr std::string_view kIsaNeon = "neon";

constexpr std::string_view kExtensionSearchWithFilter = "SearchWithFilter";
constexpr std::string_view kOverrideCpuFeatures = "LuminaOverrideCpuFeatures";


constexpr std::string_view kSnapshotPath = "snapshot.path";
constexpr std::string_view kIndexPath = "index.path";

constexpr std::string_view kFileReaderType = "io.reader.type";
constexpr std::string_view kFileReaderLocal = "local";
constexpr std::string_view kFileReaderMmap = "mmap";
constexpr std::string_view kFileReaderMmapLockMode = "io.reader.mmap.lockmode";
constexpr std::string_view kMmapLockModeNone = "none";
constexpr std::string_view kMmapLockModeMlock = "mlock";
constexpr std::string_view kMmapLockModePopulate = "populate";

constexpr std::string_view kEncodingPrefix = "encoding.";
constexpr std::string_view kEncodingPQThreadCount = "encoding.pq.threadcount";
constexpr std::string_view kEncodingPQM = "encoding.pq.m";
constexpr std::string_view kEncodingPQMaxEpoch = "encoding.pq.maxepoch";
constexpr std::string_view kEncodingPQUseOpq = "encoding.pq.useopq";
constexpr std::string_view kEncodingPQMakeZeroMean = "encoding.pq.makezeromean";
constexpr std::string_view kEncodingPQDiskM = "encoding.pq.disk.m";
constexpr std::string_view kBruteforceBuildThreadCount = "bruteforce.build.threadcount";

constexpr double CAL_EPS = 1e-8;

constexpr std::string_view kQueryStatsPrefix = "querystats.";
constexpr std::string_view kQueryStatsDistanceCalculateCount = "querystats.distancecalculatecount";
constexpr std::string_view kQueryStatsFilteredCount = "querystats.filteredcount";
}
