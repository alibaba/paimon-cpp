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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;

/// An input split for reading operation. Needed by most batch computation engines. Support
/// Serialize and Deserialize, compatible with java version.
/// This split can be either a `DataSplit` (for direct data file reads) or an `IndexedSplit`
/// (for reads leveraging global indexes).
class PAIMON_EXPORT Split {
 public:
    virtual ~Split() = default;

    /// Deserialize a `Split` from a binary buffer.
    ///
    /// Creates a `Split` instance from its serialized binary representation.
    /// This is typically used in distributed computing scenarios where splits
    /// are transmitted between different nodes or processes.
    ///
    /// @param buffer Const pointer to the binary data containing the serialized `Split`.
    /// @param length Size of the buffer in bytes.
    /// @param pool Memory pool for allocating objects during deserialization.
    /// @return Result containing the deserialized `Split` or an error status.
    static Result<std::shared_ptr<Split>> Deserialize(const char* buffer, size_t length,
                                                      const std::shared_ptr<MemoryPool>& pool);

    /// Serialize a `Split` to a binary string.
    ///
    /// Converts a `Split` instance to its binary representation for storage
    /// or transmission. The serialized data can later be deserialized using
    /// the Deserialize method.
    ///
    /// @param split The `Split` instance to serialize.
    /// @param pool Memory pool for allocating temporary objects during serialization.
    /// @return Result containing the serialized binary data as a string or an error status.
    static Result<std::string> Serialize(const std::shared_ptr<Split>& split,
                                         const std::shared_ptr<MemoryPool>& pool);
};
}  // namespace paimon
