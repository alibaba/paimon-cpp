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

#include <string>

namespace paimon {

/// Utility class for resolving compacted changelog file paths.
///
/// This class provides functionality to resolve fake compacted changelog file paths to their real
/// file paths.
///
/// File Name Protocol
///
/// There are two kinds of file name. In the following description, `bid1` and `bid2` are bucket
/// id, `off` is offset, `len1` and `len2` are lengths.
///
/// - `bucket-bid1/compacted-changelog-xxx$bid1-len1`: This is the real file name. If this file
///   name is recorded in manifest file meta, reader should read the bytes of this file starting
///   from offset `0` with length `len1`.
/// - `bucket-bid2/compacted-changelog-xxx$bid1-len1-off-len2`: This is the fake file name. Reader
///   should read the bytes of file `bucket-bid1/compacted-changelog-xxx$bid1-len1` starting from
///   offset `off` with length `len2`.
class CompactedChangelogPathResolver {
 public:
    /// Resolves a file path, handling compacted changelog file path resolution if applicable.
    ///
    /// For compacted changelog files, resolves fake file paths to their real file paths as
    /// described in the protocol above. For non-compacted changelog files, returns the path
    /// unchanged.
    ///
    /// @param path The file path to resolve.
    /// @return The resolved real file path for compacted changelog files, or the original path
    ///     unchanged for other files.
    static std::string Resolve(const std::string& path);

 private:
    /// Checks if the given path is a compacted changelog file path.
    ///
    /// @param path The file path to check.
    /// @return true if the path is a compacted changelog file, false otherwise.
    static bool IsCompactedChangelogPath(const std::string& path);
};

}  // namespace paimon
