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
#include <unordered_map>

#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

/// Represents a binary large object (BLOB) that can be read from various sources.
///
/// The Blob class provides a unified interface for handling binary data from different
/// sources including file path and blob descriptor. It supports reading data through input
/// streams and provides descriptor-based serialization.
class PAIMON_EXPORT Blob {
 public:
    ~Blob();

    /// Creates a Blob from a file path.
    ///
    /// @param path The file path to create the blob from.
    /// @return A result containing the created blob or an error.
    static Result<std::unique_ptr<Blob>> FromPath(const std::string& path);

    /// Creates a Blob from a file path with specified offset and length.
    ///
    /// @param path The file path to create the blob from.
    /// @param offset The starting offset within the file.
    /// @param length The length of data to read from the file.
    /// @return A result containing the created blob or an error.
    static Result<std::unique_ptr<Blob>> FromPath(const std::string& path, int64_t offset,
                                                  int64_t length);

    /// Creates a Blob from a blob descriptor.
    ///
    /// @param buffer The buffer of the blob descriptor.
    /// @param length The length of the buffer.
    /// @return A result containing the created blob or an error.
    static Result<std::unique_ptr<Blob>> FromDescriptor(const char* buffer, uint64_t length);

    /// Converts the blob to a blob descriptor.
    ///
    /// @param pool The memory pool to use for allocation.
    /// @return A blob descriptor bytes representing the blob.
    PAIMON_UNIQUE_PTR<Bytes> ToDescriptor(const std::shared_ptr<MemoryPool>& pool) const;

    /// Gets the URI of the blob.
    const std::string& Uri() const;

    /// Creates an input stream for reading the blob data.
    ///
    /// @param fs The file system to use for reading.
    /// @return A result containing the input stream or an error.
    Result<std::unique_ptr<InputStream>> NewInputStream(
        const std::shared_ptr<FileSystem>& fs) const;

    /// Reads the blob data to bytes.
    ///
    /// @param fs The file system to use for reading.
    /// @param pool The memory pool to use for allocation.
    /// @return A result containing the blob data bytes or an error.
    Result<PAIMON_UNIQUE_PTR<Bytes>> ToData(const std::shared_ptr<FileSystem>& fs,
                                            const std::shared_ptr<MemoryPool>& pool) const;

    /// Creates an Arrow field definition for the Blob type.
    ///
    /// This function constructs an Arrow Field (internally using `arrow::large_binary()`)
    /// and exports it to the C data interface structure `::ArrowSchema`.
    /// It automatically injects Paimon-specific metadata to identify the field as a BLOB.
    ///
    /// @param field_name The name of the Arrow field.
    /// @param metadata A map of key-value metadata to be attached to the field.
    /// @return A result containing a unique pointer to the generated `ArrowSchema` or an error.
    static Result<std::unique_ptr<::ArrowSchema>> ArrowField(
        const std::string& field_name, std::unordered_map<std::string, std::string> metadata = {});

 private:
    class Impl;

    explicit Blob(std::unique_ptr<Impl>&& impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
