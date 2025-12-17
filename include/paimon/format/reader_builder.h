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

#include <memory>
#include <string>

#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/type_fwd.h"

namespace paimon {

/// Create a file batch reader based on the file path. Allows you to specify memory pool.
class PAIMON_EXPORT ReaderBuilder {
 public:
    virtual ~ReaderBuilder() = default;

    /// Set memory pool to use.
    virtual ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) = 0;

    /// Build a file batch reader based on the created `InputStream`.
    virtual Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& path) const = 0;

    /// Build a file batch reader based on the file path.
    virtual Result<std::unique_ptr<FileBatchReader>> Build(const std::string& path) const = 0;
};

}  // namespace paimon
