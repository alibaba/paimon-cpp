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
#include <map>
#include <memory>
#include <set>
#include <string>

#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class MemoryPool;

/// `CleanContext` is some configuration for orphan files clean operations.
///
/// Please do not use this class directly, use `CleanContextBuilder` to build a `CleanContext` which
/// has input validation.
/// @see CleanContextBuilder
class PAIMON_EXPORT CleanContext {
 public:
    CleanContext(const std::string& root_path, const std::map<std::string, std::string>& options,
                 int64_t older_than_ms, const std::shared_ptr<MemoryPool>& pool,
                 const std::shared_ptr<Executor>& executor,
                 const std::shared_ptr<FileSystem>& specific_file_system,
                 std::function<bool(const std::string&)> should_be_retained);
    ~CleanContext();

    const std::string& GetRootPath() const {
        return root_path_;
    }

    const std::map<std::string, std::string>& GetOptions() const {
        return options_;
    }

    int64_t GetOlderThanMs() const {
        return older_than_ms_;
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

    std::function<bool(const std::string&)> GetFileRetainCondition() const {
        return should_be_retained_;
    }

 private:
    std::string root_path_;
    std::map<std::string, std::string> options_;
    int64_t older_than_ms_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileSystem> specific_file_system_;
    std::function<bool(const std::string&)> should_be_retained_;
};

/// `CleanContextBuilder` used to build a `CleanContext`, has input validation.
class PAIMON_EXPORT CleanContextBuilder {
 public:
    /// Constructs a `CleanContextBuilder` with required parameters.
    /// @param root_path The root path of the table.
    explicit CleanContextBuilder(const std::string& root_path);

    ~CleanContextBuilder();

    /// Set a configuration options map to set some option entries which are not defined in the
    /// table schema or whose values you want to overwrite.
    /// @note The options map will clear the options added by `AddOption()` before.
    /// @param options The configuration options map.
    /// @return Reference to this builder for method chaining.
    CleanContextBuilder& SetOptions(const std::map<std::string, std::string>& options);

    /// Add a single configuration option which is not defined in the table schema or whose value
    /// you want to overwrite.
    ///
    /// If you want to add multiple options, call `AddOption()` multiple times or use `SetOptions()`
    /// instead.
    /// @param key The option key.
    /// @param value The option value.
    /// @return Reference to this builder for method chaining.
    CleanContextBuilder& AddOption(const std::string& key, const std::string& value);

    /// An optional time threshold in milliseconds for filtering. If not provided, defaults to the
    /// current time minus one day.
    CleanContextBuilder& WithOlderThanMs(int64_t older_than_ms);

    /// Specifies a custom condition to determine which files should be retained.
    /// @param should_be_retained A callable object that takes a filename and returns `true` if the
    ///                           file should be kept, or `false` if it can be deleted.
    /// @return Reference to this builder for method chaining.
    CleanContextBuilder& WithFileRetainCondition(
        std::function<bool(const std::string&)> should_be_retained);

    /// Set custom memory pool for memory management.
    /// @param pool The memory pool to use.
    /// @return Reference to this builder for method chaining.
    CleanContextBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& pool);

    /// Set custom executor for task execution.
    /// @param executor The executor to use.
    /// @return Reference to this builder for method chaining.
    CleanContextBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);

    /// Sets a custom file system instance to be used for all file operations in this clean context.
    /// This bypasses the global file system registry and uses the provided implementation directly.
    ///
    /// @param file_system The file system to use.
    /// @return Reference to this builder for method chaining.
    /// @note If not set, use default file system (configured in `Options::FILE_SYSTEM`)
    CleanContextBuilder& WithFileSystem(const std::shared_ptr<FileSystem>& file_system);

    /// Build and return a `CleanContext` instance with input validation.
    /// @return Result containing the constructed `CleanContext` or an error status.
    Result<std::unique_ptr<CleanContext>> Finish();

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

/// To remove the data files and metadata files that are not used by table (so-called "orphan
/// files").
///
/// It will ignore exception when listing all files because it's OK to not delete unread files.
///
/// To avoid deleting newly written files, it only deletes orphan files older than `olderThanMillis`
/// (1 day by default).
///
/// To avoid deleting files that are used but not read by mistaken, it will stop removing process
/// when failed to read used files.
///
/// To avoid deleting files that were newly added to the Paimon Java protocol but are unrecognized
/// by Paimon C++, we implemented a strong pattern-matching validation, deleting only files in
/// patterns we recognize.
///
/// @note `OrphanFilesCleaner` in Paimon C++ only support cleaning append table, do not support
/// cleaning table with tag, table with external paths, table with branch, table with index, table
/// with changelog, and primary key table.
class PAIMON_EXPORT OrphanFilesCleaner {
 public:
    virtual ~OrphanFilesCleaner() = default;

    /// Create an instance of `OrphanFilesCleaner`.
    ///
    /// @param context A unique pointer to the `CleanContext` used for cleanup tasks.
    ///
    /// @return A Result containing a unique pointer to the `OrphanFilesCleaner` instance.
    static Result<std::unique_ptr<OrphanFilesCleaner>> Create(
        std::unique_ptr<CleanContext>&& context);

    /// Cleans orphan files.
    ///
    /// @return A Result object containing a set of strings representing the paths of the cleaned
    /// files.
    virtual Result<std::set<std::string>> Clean() = 0;

 protected:
    OrphanFilesCleaner() = default;
};
}  // namespace paimon
