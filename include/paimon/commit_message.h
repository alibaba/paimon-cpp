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
#include <memory>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class CommitMessageSerializer;
class MemoryPool;

/// Commit message for partition and bucket. Supports serialization and deserialization compatible
/// with Java Paimon.
///
/// @note Serialized payloads do not embed their serialization version. Transport
/// `CurrentVersion()` alongside the payload and pass it explicitly to `Deserialize()` or
/// `DeserializeList()`.
// TODO(yonghao.fyh): to add some statistics of write (e.g., write bytes)
class PAIMON_EXPORT CommitMessage {
 public:
    /// Gets the version with which this serializer serializes.
    static int32_t CurrentVersion();

    virtual ~CommitMessage();

    /// Serializes a single commit message to a binary string format.
    /// The serialized format is compatible with Java Paimon.
    /// The serialization version is not included in the returned payload.
    /// @param commit_message The commit message to serialize.
    /// @param pool Memory pool for memory allocation during serialization.
    /// @return Result containing the serialized string data, or an error if serialization fails.
    static Result<std::string> Serialize(const std::shared_ptr<CommitMessage>& commit_message,
                                         const std::shared_ptr<MemoryPool>& pool);

    /// Serializes a list of commit messages to a binary string format.
    /// The serialization version is not included in the returned payload.
    /// @param commit_messages Vector of commit messages to serialize.
    /// @param pool Memory pool for memory allocation during serialization.
    /// @return Result containing the serialized string data, or an error if serialization fails.
    static Result<std::string> SerializeList(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
        const std::shared_ptr<MemoryPool>& pool);

    /// Deserializes a single commit message from binary data.
    /// @param version The serialization format version used when the data was serialized.
    /// @param buffer Pointer to the binary data buffer.
    /// @param length Length of the binary data in bytes.
    /// @param pool Memory pool for memory allocation during deserialization.
    /// @return Result containing the deserialized CommitMessage, or an error if deserialization
    /// fails.
    static Result<std::shared_ptr<CommitMessage>> Deserialize(
        int32_t version, const char* buffer, int32_t length,
        const std::shared_ptr<MemoryPool>& pool);

    /// Deserializes a list of commit messages from binary data.
    /// This is the counterpart to SerializeList() for batch processing.
    /// @param version The serialization format version used when the data was serialized.
    /// @param buffer Pointer to the binary data buffer.
    /// @param length Length of the binary data in bytes.
    /// @param pool Memory pool for memory allocation during deserialization.
    /// @return Result containing a vector of deserialized CommitMessages, or an error if
    /// deserialization fails.
    static Result<std::vector<std::shared_ptr<CommitMessage>>> DeserializeList(
        int32_t version, const char* buffer, int32_t length,
        const std::shared_ptr<MemoryPool>& pool);

    /// Converts a commit message to a human-readable debug string.
    /// This is useful for logging, debugging, and troubleshooting purposes.
    /// @param commit_message The commit message to convert to string.
    /// @return Result containing the debug string representation, or an error if conversion fails.
    static Result<std::string> ToDebugString(const std::shared_ptr<CommitMessage>& commit_message);
};

}  // namespace paimon
