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

/// Utils for vector-store files.
///
/// A vector-store file is identified by the `.vector.` marker in its name.
class VectorStoreUtils {
 public:
    VectorStoreUtils() = delete;
    ~VectorStoreUtils() = delete;

    /// Returns true if `file_name` is a vector-store file (contains the `.vector.` marker).
    static bool IsVectorStoreFile(const std::string& file_name) {
        return file_name.find(".vector.") != std::string::npos;
    }
};

}  // namespace paimon
