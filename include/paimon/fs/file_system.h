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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {

/// Enumeration for stream seek origin positions.
enum PAIMON_EXPORT SeekOrigin {
    /// Seek from the beginning of the stream.
    FS_SEEK_SET,
    /// Seek from the current position in the stream.
    FS_SEEK_CUR,
    /// Seek from the end of the stream.
    FS_SEEK_END
};

/// Abstract base class for all stream operations.
class PAIMON_EXPORT Stream {
 public:
    virtual ~Stream() = default;
    /// Close the stream.
    virtual Status Close() = 0;
};

/// Abstract class for input stream operations.
class PAIMON_EXPORT InputStream : public Stream {
 public:
    InputStream() = default;
    ~InputStream() override = default;

    /// Seek to a specified position in the input stream.
    ///
    /// @param offset The byte offset relative to the origin position.
    /// @param origin The reference point for seeking (`::FS_SEEK_SET`, `::FS_SEEK_CUR`,
    /// `::FS_SEEK_END`).
    /// @return Status indicating success (OK) or failure with appropriate error information.
    virtual Status Seek(int64_t offset, SeekOrigin origin) = 0;

    /// Get the current position in the input stream.
    ///
    /// @return Current position in the input stream.
    /// @return IOError returned if an I/O error occurred in the underlying stream.
    /// implementation while accessing the stream's position.
    virtual Result<int64_t> GetPos() const = 0;

    /// Read data from the current position in the stream.
    /// @param[out] buffer Pointer to the buffer where read data will be stored.
    /// @param size Maximum number of bytes to read.
    /// @return Result containing the actual number of bytes read on success, or an error status on
    ///         failure.
    /// @note The stream position advances by the number of bytes actually read.
    virtual Result<int64_t> Read(char* buffer, int64_t size) = 0;

    /// Read data from given position in the stream.
    ///
    /// Read with offset performs like `pread()` function, which will not change the position in the
    /// input stream.
    ///
    /// @param[out] buffer The buffer to store the read content.
    /// @param size The number of bytes to read.
    /// @param offset The position in the stream to read from.
    virtual Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) = 0;

    /// Asynchronously read data from the input stream.
    ///
    /// This function initiates an asynchronous read operation. The specified number of bytes
    /// will be read from the stream starting at the given offset and stored in the provided buffer.
    /// Once the read operation is complete, the provided callback function will be invoked with
    /// the status of the read operation.
    ///
    /// @param[out] buffer The buffer to store the read content.
    /// @param size The number of bytes to read.
    /// @param offset The position in the stream to read from.
    /// @param callback The callback function to be invoked upon completion of the read operation.
    ///                 The callback will receive a Status object indicating the success or failure
    ///                 of the read operation.
    virtual void ReadAsync(char* buffer, int64_t size, int64_t offset,
                           std::function<void(Status)>&& callback) = 0;

    /// Get an identifier that uniquely identify the underlying content.
    /// @return An uri if the underlying content can be uniquely identified.
    /// @return Empty string if the underlying content cannot be uniquely identified.
    virtual Result<std::string> GetUri() const = 0;

    /// Get the total length of the file in bytes.
    virtual Result<int64_t> Length() const = 0;
};

/// Abstract class for output stream operations.
class PAIMON_EXPORT OutputStream : public Stream {
 public:
    OutputStream() = default;

    /// Write data to the output stream.
    /// @param buffer Pointer to the data buffer to write.
    /// @param size Number of bytes to write from the buffer.
    /// @return Result containing the actual number of bytes written on success, or an error status
    ///         on failure.
    /// @note The stream position advances by the number of bytes actually written.
    virtual Result<int64_t> Write(const char* buffer, int64_t size) = 0;

    /// Flush pending data to the disk.
    virtual Status Flush() = 0;
    /// Get the write position.
    virtual Result<int64_t> GetPos() const = 0;
    /// Get the uri of the output stream.
    virtual Result<std::string> GetUri() const = 0;
};

/// Basic file status information interface.
///
/// This class provides fundamental file system metadata for files and directories. It serves as a
/// lightweight interface for basic file operations that only require path information and directory
/// status.
class PAIMON_EXPORT BasicFileStatus {
 public:
    BasicFileStatus() = default;
    virtual ~BasicFileStatus() = default;

    /// Check if this entry represents a directory.
    virtual bool IsDir() const = 0;

    /// Get the path of this file or directory.
    virtual std::string GetPath() const = 0;
};

/// Extended file status information interface.
///
/// This class extends BasicFileStatus to provide comprehensive file system metadata including file
/// size, modification time, and other attributes. It's used for operations that require detailed
/// file information.
class PAIMON_EXPORT FileStatus {
 public:
    FileStatus() = default;
    virtual ~FileStatus() = default;

    /// Get the size of the file in bytes.
    /// @note For directories, this method is undefined behavior.
    virtual int64_t GetLen() const = 0;

    /// Check if this entry represents a directory.
    virtual bool IsDir() const = 0;

    /// Get the path of this file or directory.
    virtual std::string GetPath() const = 0;

    /// Get the last modification time of the file.
    ///
    /// @return A long value representing the time the file was last modified, measured in
    /// milliseconds since the epoch (UTC January 1, 1970).
    virtual int64_t GetModificationTime() const = 0;
};

/// Abstract file system interface.
class PAIMON_EXPORT FileSystem {
 public:
    virtual ~FileSystem();

    /// Check if the given path represents an object store.
    /// @note Object stores typically have different semantics than traditional file systems.
    static Result<bool> IsObjectStore(const std::string& path_str);

    /// Open an existing file for reading.
    /// @param path The file path to open.
    /// @return Result containing a unique pointer to `InputStream` on success, or error status on
    ///         failure (e.g., file not found, permission denied).
    virtual Result<std::unique_ptr<InputStream>> Open(const std::string& path) const = 0;

    /// Create a new file for writing.
    /// @param path The file path to create.
    /// @param overwrite If true, overwrite existing file; if false, fail if file exists.
    /// @return Result containing a unique pointer to `OutputStream` on success, or error status on
    ///         failure (e.g., I/O error, permission denied).
    virtual Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                         bool overwrite) const = 0;

    /// Create directories recursively.
    /// @param path The directory path to create (including all parent directories).
    /// @return Status indicating success (OK) or failure with error information.
    virtual Status Mkdirs(const std::string& path) const = 0;

    /// Rename or move a file or directory.
    /// @param src The source path (file or directory to rename/move).
    /// @param dst The destination path.
    /// @return Status indicating success (OK) or failure with error information.
    virtual Status Rename(const std::string& src, const std::string& dst) const = 0;

    /// Delete a file or directory.
    /// @param path The path to delete.
    /// @param recursive If true, delete directories and their contents recursively;
    ///                  if false, only delete empty directories.
    /// @return Status indicating success (OK) or failure with error information.
    virtual Status Delete(const std::string& path, bool recursive = true) const = 0;
    /// Get detailed status information for a file or directory.
    /// @param path The file or directory path to query.
    /// @return Result containing a unique pointer to `FileStatus` on success, or error status on
    ///         failure (e.g., path not found, permission denied).
    virtual Result<std::unique_ptr<FileStatus>> GetFileStatus(const std::string& path) const = 0;

    /// List files of a directory (basic information only).
    /// @param directory The directory path to list.
    /// @param[out] file_status_list Output vector to store `BasicFileStatus` objects.
    /// @return Status indicating success (OK) or failure with error information.
    virtual Status ListDir(
        const std::string& directory,
        std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) const = 0;

    /// List file status with detailed information.
    /// @param path The file or directory path to list.
    /// @param[out] file_status_list Output vector to store `FileStatus` objects.
    /// @return Status indicating success (OK) or failure with error information.
    virtual Status ListFileStatus(
        const std::string& path,
        std::vector<std::unique_ptr<FileStatus>>* file_status_list) const = 0;

    /// Check if a file or directory exists.
    /// @param path The file or directory path to check.
    /// @return Result containing true if path exists, false if not found; or error status if
    ///         I/O error occurs during check.
    virtual Result<bool> Exists(const std::string& path) const = 0;

    /// Read entire file content into a string at once.
    /// @param path The file path to read.
    /// @param[out] content Output string to store the file content.
    /// @return Status indicating success (OK) or failure with error information.
    /// @note Virtual only for mock testing purposes.
    virtual Status ReadFile(const std::string& path, std::string* content);

    /// Write the entire content to a file at once.
    /// @param path The file path to write to.
    /// @param content The string content to write.
    /// @param overwrite If true, overwrite existing file; if false, fail if file exists.
    /// @return Status indicating success (OK) or failure with error information.
    /// @note Virtual only for mock testing purposes.
    virtual Status WriteFile(const std::string& path, const std::string& content, bool overwrite);

    /// Write content to a file atomically. Atomic operation: writes to temporary hidden file first,
    /// then renames to target.
    /// @param path The target file path.
    /// @param content The string content to write.
    /// @return Status indicating success (OK) or failure with error information.
    /// @note Virtual only for mock testing purposes.
    virtual Status AtomicStore(const std::string& path, const std::string& content);
};

}  // namespace paimon
