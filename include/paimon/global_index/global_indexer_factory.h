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

#include <map>
#include <memory>
#include <string>

#include "paimon/factories/factory.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class GlobalIndexer;

/// Factory for creating `GlobalIndexer` instances based on index type identifiers.
class PAIMON_EXPORT GlobalIndexerFactory : public Factory {
 public:
    ~GlobalIndexerFactory() override = default;

    /// Suffix used to distinguish global index identifiers (e.g., "btree-global").
    static const char GLOBAL_INDEX_IDENTIFIER_SUFFIX[];

    /// Creates a `GlobalIndexer` instance by looking up a registered factory using an identifier.
    ///
    /// The provided `identifier` is automatically appended with `GLOBAL_INDEX_IDENTIFIER_SUFFIX`
    /// (e.g., "-global") to form the full key used for factory lookup. This ensures namespace
    /// separation between file and global index types.
    ///
    /// @param identifier The base name of the index type (e.g., "btree").
    /// @param options    Configuration parameters for the indexer.
    /// @return A `Result` containing a unique pointer to the created `GlobalIndexer`,
    ///         or an error if creation fails.
    /// @return nullptr if no matching factory exists or the index type is temporarily unsupported.
    static Result<std::unique_ptr<GlobalIndexer>> Get(
        const std::string& identifier, const std::map<std::string, std::string>& options);

    /// Creates a `GlobalIndexer` using the current factory's implementation and the given options.
    virtual Result<std::unique_ptr<GlobalIndexer>> Create(
        const std::map<std::string, std::string>& options) const = 0;
};

}  // namespace paimon
