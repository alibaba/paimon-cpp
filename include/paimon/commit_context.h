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

#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class MemoryPool;

/// `CommitContext` is some configuration for commit operations.
///
/// Please do not use this class directly, use `CommitContextBuilder` to build a `CommitContext`
/// which has input validation.
/// @see CommitContextBuilder
class PAIMON_EXPORT CommitContext {
 public:
    CommitContext(const std::string& root_path, const std::string& commit_user,
                  bool ignore_empty_commit, bool use_rest_catalog_commit,
                  const std::shared_ptr<MemoryPool>& memory_pool,
                  const std::shared_ptr<Executor>& executor,
                  const std::shared_ptr<FileSystem>& specific_file_system,
                  const std::map<std::string, std::string>& options);
    ~CommitContext();

    const std::string& GetRootPath() const {
        return root_path_;
    }

    const std::string& GetCommitUser() const {
        return commit_user_;
    }

    bool IgnoreEmptyCommit() const {
        return ignore_empty_commit_;
    }

    bool UseRESTCatalogCommit() const {
        return use_rest_catalog_commit_;
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

    const std::map<std::string, std::string>& GetOptions() const {
        return options_;
    }

 private:
    std::string root_path_;
    std::string commit_user_;
    bool ignore_empty_commit_;
    bool use_rest_catalog_commit_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileSystem> specific_file_system_;
    std::map<std::string, std::string> options_;
};

/// `CommitContextBuilder` used to build a `CommitContext`, has input validation.
class PAIMON_EXPORT CommitContextBuilder {
 public:
    /// Constructs a `CommitContextBuilder` with required parameters.
    /// @param root_path The root path of the Paimon table.
    /// @param commit_user The user identifier for the commit operation.
    CommitContextBuilder(const std::string& root_path, const std::string& commit_user);

    ~CommitContextBuilder();

    /// Set a configuration options map to set some option entries which are not defined in the
    /// table schema or whose values you want to overwrite.
    /// @note The options map will clear the options added by `AddOption()` before.
    /// @param options The configuration options map.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& SetOptions(const std::map<std::string, std::string>& options);

    /// Add a single configuration option which is not defined in the table schema or whose value
    /// you want to overwrite.
    ///
    /// If you want to add multiple options, call `AddOption()` multiple times or use `SetOptions()`
    /// instead.
    /// @param key The option key.
    /// @param value The option value.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& AddOption(const std::string& key, const std::string& value);

    /// Sets whether to ignore empty commits (default is true).
    /// When set to true, commits that don't contain any actual data changes will be ignored.
    /// @param ignore_empty_commit True to ignore empty commits, false otherwise.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& IgnoreEmptyCommit(bool ignore_empty_commit);

    /// Sets whether to use REST catalog commit (default is false).
    /// @note Temporary interface, will be removed in the future.
    /// @param use_rest_catalog_commit True to use REST catalog commit, false otherwise.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& UseRESTCatalogCommit(bool use_rest_catalog_commit);

    /// Sets the memory pool to be used for memory allocation during commit operations.
    /// @param memory_pool Shared pointer to the memory pool instance.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& memory_pool);

    /// Sets the executor to be used for asynchronous operations during commit.
    /// @param executor Shared pointer to the executor instance.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);

    /// Sets a custom file system instance to be used for all file operations in this commit
    /// context.
    /// This bypasses the global file system registry and uses the provided implementation directly.
    ///
    /// @param file_system The file system to use.
    /// @return Reference to this builder for method chaining.
    /// @note If not set, use default file system (configured in `Options::FILE_SYSTEM`)
    CommitContextBuilder& WithFileSystem(const std::shared_ptr<FileSystem>& file_system);

    /// Build and return a `CommitContext` instance with input validation.
    /// @return Result containing the constructed `CommitContext` or an error status.
    Result<std::unique_ptr<CommitContext>> Finish();

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
