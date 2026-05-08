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

#pragma once

#include <cstddef>
#include <cstdint>

namespace paimon {
struct BtreeDefs {
    BtreeDefs() = delete;
    ~BtreeDefs() = delete;
    static inline const char kIdentifier[] = "btree";
    /// "btree-index.compression" - The compression algorithm to use for BTreeIndex.
    /// Default value is "none".
    static inline const char kBtreeIndexCompression[] = "btree-index.compression";
    /// "btree-index.compression-level" - The compression level of the compression algorithm.
    /// Default value is 1.
    static inline const char kBtreeIndexCompressionLevel[] = "btree-index.compression-level";
    /// "btree-index.block-size" - The block size to use for BTreeIndex.
    /// Default value is 64 KB.
    static inline const char kBtreeIndexBlockSize[] = "btree-index.block-size";
    /// "btree-index.cache-size" - The cache size to use for BTreeIndex.
    /// Default value is 128 MB.
    static inline const char kBtreeIndexCacheSize[] = "btree-index.cache-size";
    /// "btree-index.high-priority-pool-ratio" - The high priority pool ratio to use for BTreeIndex.
    /// Default value is 0.1.
    static inline const char kBtreeIndexHighPriorityPoolRatio[] =
        "btree-index.high-priority-pool-ratio";

    /// "btree-index.read-buffer-size" - Optional. Specifies the read buffer size for the B-tree
    /// index. This setting can be tuned based on query patterns:
    ///   - For range queries (e.g., `VisitLessThan`, `VisitGreaterOrEqual`), increasing the buffer
    ///   size (e.g., to 1MB) may improve I/O bandwidth and sequential read performance.
    ///   - For point queries (e.g., `VisitEqual`), buffering can introduce negative effects due to
    ///   read amplification; it is recommended to leave this option unset.
    ///
    /// If specified, read block with `BufferedInputStream`.
    static inline const char kBtreeIndexReadBufferSize[] = "btree-index.read-buffer-size";

    static inline const char kDefaultBtreeIndexBlockSize[] = "64KB";
    static inline const char kDefaultBtreeIndexCompression[] = "none";
    static inline const char kDefaultBtreeIndexCacheSize[] = "128MB";
    static inline const int32_t kDefaultBtreeIndexCompressionLevel = 1;
    static inline const double kDefaultBtreeIndexHighPriorityPoolRatio = 0.1;
};
}  // namespace paimon
