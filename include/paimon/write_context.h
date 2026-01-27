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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class MemoryPool;

/// `WriteContext` is some configuration for write operations.
///
/// Please do not use this class directly, use `WriteContextBuilder` to build a `WriteContext` which
/// has input validation.
/// @see WriteContextBuilder
class PAIMON_EXPORT WriteContext {
 public:
    WriteContext(const std::string& root_path, const std::string& commit_user,
                 bool is_streaming_mode, bool ignore_num_bucket_check, bool ignore_previous_files,
                 const std::optional<int32_t>& write_id, const std::string& branch,
                 const std::vector<std::string>& write_schema,
                 const std::shared_ptr<MemoryPool>& memory_pool,
                 const std::shared_ptr<Executor>& executor,
                 const std::shared_ptr<FileSystem>& specific_file_system,
                 const std::map<std::string, std::string>& fs_scheme_to_identifier_map,
                 const std::map<std::string, std::string>& options);

    ~WriteContext();

    const std::string& GetRootPath() const {
        return root_path_;
    }

    const std::string& GetCommitUser() const {
        return commit_user_;
    }

    const std::map<std::string, std::string>& GetFileSystemSchemeToIdentifierMap() const {
        return fs_scheme_to_identifier_map_;
    }

    const std::map<std::string, std::string>& GetOptions() const {
        return options_;
    }

    bool IsStreamingMode() const {
        return is_streaming_mode_;
    }

    bool IgnoreNumBucketCheck() const {
        return ignore_num_bucket_check_;
    }

    bool IgnorePreviousFiles() const {
        return ignore_previous_files_;
    }

    const std::optional<int32_t> GetWriteId() const {
        return write_id_;
    }

    const std::string& GetBranch() const {
        return branch_;
    }

    const std::vector<std::string>& GetWriteSchema() const {
        return write_schema_;
    }

    std::shared_ptr<MemoryPool> GetMemoryPool() const {
        return memory_pool_;
    }

    std::shared_ptr<Executor> GetExecutor() const {
        return executor_;
    }

    std::shared_ptr<FileSystem> GetSpecificFileSystem() const {
        return specific_file_system_;
    }

 private:
    std::string root_path_;
    std::string commit_user_;
    std::string branch_;
    bool is_streaming_mode_;
    bool ignore_num_bucket_check_;
    bool ignore_previous_files_;
    std::optional<int32_t> write_id_;
    std::vector<std::string> write_schema_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileSystem> specific_file_system_;
    std::map<std::string, std::string> fs_scheme_to_identifier_map_;
    std::map<std::string, std::string> options_;
};

/// `WriteContextBuilder` used to build a `WriteContext`, has input validation.
class PAIMON_EXPORT WriteContextBuilder {
 public:
    /// Constructs a `WriteContextBuilder` with required parameters.
    /// @param root_path The root path of the table.
    /// @param commit_user The user identifier for commit operations.
    WriteContextBuilder(const std::string& root_path, const std::string& commit_user);

    ~WriteContextBuilder();

    /// Set a configuration options map to set some option entries which are not defined in the
    /// table schema or whose values you want to overwrite.
    /// @note The options map will clear the options added by `AddOption()` before.
    /// @param options The configuration options map.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& SetOptions(const std::map<std::string, std::string>& options);

    /// Add a single configuration option which is not defined in the table schema or whose value
    /// you want to overwrite.
    ///
    /// If you want to add multiple options, call `AddOption()` multiple times or use `SetOptions()`
    /// instead.
    /// @param key The option key.
    /// @param value The option value.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& AddOption(const std::string& key, const std::string& value);

    /// Set whether to enable streaming mode (default is false)
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithStreamingMode(bool is_streaming_mode);

    /// Set whether the write operation should ignore previously stored files. (default is false)
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithIgnorePreviousFiles(bool ignore_previous_files);

    /// Set custom memory pool for memory management.
    /// @param memory_pool The memory pool to use.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& memory_pool);

    /// Set custom executor for task execution.
    /// @param executor The executor to use.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);

    /// For postpone bucket mode in pk table, `WithWriteId()` supposed to be used.
    ///
    /// Each worker must have its own unique `write_id` within a task, which is used as the prefix
    /// for its data files. This ensures that files from the same worker share the same prefix and
    /// can be consumed by the same compaction reader to preserve input order.
    ///
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithWriteId(int32_t write_id);

    /// Write to specific branch, default is main.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithBranch(const std::string& branch);

    /// For data evolution, user can write partial specific fields from table schema.
    /// If not set, write all fields in table.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithWriteSchema(const std::vector<std::string>& write_schema);

    /// Sets a custom file system instance to be used for all file operations in this write context.
    /// This bypasses the global file system registry and uses the provided implementation directly.
    ///
    /// @param file_system The file system to use.
    /// @return Reference to this builder for method chaining.
    /// @note If not set, use default file system (configured in `Options::FILE_SYSTEM`)
    WriteContextBuilder& WithFileSystem(const std::shared_ptr<FileSystem>& file_system);

    /// Sets a mapping from URI schemes (e.g., "file", "oss") to registered file system
    /// identifiers. This allows selecting different pre-registered file system implementations
    /// based on the URI scheme at runtime.
    ///
    /// @param fs_scheme_to_identifier_map Map from URI scheme (like "oss") to the corresponding
    /// file system identifier.
    /// @return Reference to this builder for method chaining.
    /// @note
    ///   - This method is intended for environments where multiple file systems are pre-registered.
    ///   - The specified identifiers must correspond to file systems that have been registered at
    ///   compile time or initialization.
    ///   - Cannot be used together with `WithFileSystem()`.
    ///   - If not set, use default file system (configured in `Options::FILE_SYSTEM`).
    /// Example:
    ///   builder.WithFileSystemSchemeToIdentifierMap({{"oss", "jindo"}, {"file", "local"}});
    ///
    WriteContextBuilder& WithFileSystemSchemeToIdentifierMap(
        const std::map<std::string, std::string>& fs_scheme_to_identifier_map);

    /// Build and return a `WriteContext` instance with input validation.
    /// @return Result containing the constructed `WriteContext` or an error status.
    Result<std::unique_ptr<WriteContext>> Finish();

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
