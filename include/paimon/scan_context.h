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

#include "paimon/global_index/global_index_result.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"
namespace paimon {
class ScanContextBuilder;
class ScanFilter;
class Executor;
class MemoryPool;
class Predicate;

/// `ScanContext` is some configuration for table scan operations.
///
/// Please do not use this class directly, use `ScanContextBuilder` to build a `ScanContext` which
/// has input validation.
/// @see ScanContextBuilder
class PAIMON_EXPORT ScanContext {
 public:
    ScanContext(const std::string& path, bool is_streaming_mode, std::optional<int32_t> limit,
                const std::shared_ptr<ScanFilter>& scan_filter,
                const std::shared_ptr<GlobalIndexResult>& global_index_result,
                const std::shared_ptr<MemoryPool>& memory_pool,
                const std::shared_ptr<Executor>& executor,
                const std::shared_ptr<FileSystem>& specific_file_system,
                const std::map<std::string, std::string>& options);

    ~ScanContext();

    const std::string& GetPath() const {
        return path_;
    }

    bool IsStreamingMode() const {
        return is_streaming_mode_;
    }

    std::optional<int32_t> GetLimit() const {
        return limit_;
    }

    std::shared_ptr<ScanFilter> GetScanFilters() const {
        return scan_filters_;
    }
    const std::map<std::string, std::string>& GetOptions() const {
        return options_;
    }

    std::shared_ptr<MemoryPool> GetMemoryPool() const {
        return memory_pool_;
    }

    std::shared_ptr<Executor> GetExecutor() const {
        return executor_;
    }
    std::shared_ptr<GlobalIndexResult> GetGlobalIndexResult() const {
        return global_index_result_;
    }

    std::shared_ptr<FileSystem> GetSpecificFileSystem() const {
        return specific_file_system_;
    }

 private:
    std::string path_;
    bool is_streaming_mode_;
    std::optional<int32_t> limit_;
    std::shared_ptr<ScanFilter> scan_filters_;
    std::shared_ptr<GlobalIndexResult> global_index_result_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileSystem> specific_file_system_;
    std::map<std::string, std::string> options_;
};

/// Filter configuration for table scan operations
class PAIMON_EXPORT ScanFilter {
 public:
    ScanFilter(const std::shared_ptr<Predicate>& predicate,
               const std::vector<std::map<std::string, std::string>>& partition_filters,
               const std::optional<int32_t>& bucket_filter,
               const std::shared_ptr<VectorSearch>& vector_search)
        : predicates_(predicate),
          vector_search_(vector_search),
          bucket_filter_(bucket_filter),
          partition_filters_(partition_filters) {}

    std::shared_ptr<Predicate> GetPredicate() const {
        return predicates_;
    }
    std::shared_ptr<VectorSearch> GetVectorSearch() const {
        return vector_search_;
    }
    std::optional<int32_t> GetBucketFilter() const {
        return bucket_filter_;
    }
    const std::vector<std::map<std::string, std::string>>& GetPartitionFilters() const {
        return partition_filters_;
    }

 private:
    std::shared_ptr<Predicate> predicates_;
    std::shared_ptr<VectorSearch> vector_search_;
    std::optional<int32_t> bucket_filter_;
    std::vector<std::map<std::string, std::string>> partition_filters_;
};

/// `ScanContextBuilder` used to build a `ScanContext`, has input validation.
class PAIMON_EXPORT ScanContextBuilder {
 public:
    /// Constructs a `ScanContextBuilder` with required parameters.
    /// @param path The root path of the table.
    explicit ScanContextBuilder(const std::string& path);
    ~ScanContextBuilder();
    /// If limit is not set, it defaults to unlimited.
    ScanContextBuilder& SetLimit(int32_t limit);
    /// Set a bucket filter to scan only specific bucket.
    ScanContextBuilder& SetBucketFilter(int32_t bucket_filter);
    /// partition_filters in vector is supposed to be OR, filter in map is supposed to be AND, e.g.,
    /// partition_filters is {{k1=1,k2=10}, {k1=2,k2=20}} => OR(AND(k1=1,k2=10), AND(k1=2,k2=20))
    ScanContextBuilder& SetPartitionFilter(
        const std::vector<std::map<std::string, std::string>>& partition_filters);
    /// Set a predicate for filtering data.
    ScanContextBuilder& SetPredicate(const std::shared_ptr<Predicate>& predicate);

    /// Sets the result of a global index search (e.g., row ids (may with scores) from a distributed
    /// index lookup). This is used to push down index-filtered row ids into the scan for efficient
    /// data retrieval.
    ScanContextBuilder& SetGlobalIndexResult(
        const std::shared_ptr<GlobalIndexResult>& global_index_result);

    /// Set vector search for similarity search.
    ScanContextBuilder& SetVectorSearch(const std::shared_ptr<VectorSearch>& vector_search);
    /// The options added or set in `ScanContextBuilder` have high priority and will be merged with
    /// the options in table schema.
    ScanContextBuilder& AddOption(const std::string& key, const std::string& value);
    /// Set a configuration options map to set some option entries which are not defined in the
    /// table schema or whose values you want to overwrite.
    /// @note The options map will clear the options added by `AddOption()` before.
    /// @param options The configuration options map.
    /// @return Reference to this builder for method chaining.
    ScanContextBuilder& SetOptions(const std::map<std::string, std::string>& options);

    /// Set whether the scan is in streaming mode.
    /// @note if not set, is_streaming_mode = false
    ScanContextBuilder& WithStreamingMode(bool is_streaming_mode);
    /// Set custom memory pool for memory management.
    /// @param memory_pool The memory pool to use.
    /// @return Reference to this builder for method chaining.
    /// @note if not set, memory_pool is default pool
    ScanContextBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& memory_pool);
    /// Set custom executor for task execution.
    /// @param executor The executor to use.
    /// @return Reference to this builder for method chaining.
    /// @note if not set, executor is default executor
    ScanContextBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);
    /// Sets a custom file system instance to be used for all file operations in this scan context.
    /// This bypasses the global file system registry and uses the provided implementation directly.
    /// @param file_system The file system to use.
    /// @return Reference to this builder for method chaining.
    /// @note If not set, use default file system (configured in `Options::FILE_SYSTEM`)
    ScanContextBuilder& WithFileSystem(const std::shared_ptr<FileSystem>& file_system);

    /// Build and return a `ScanContext` instance with input validation.
    /// @return Result containing the constructed `ScanContext` or an error status.
    Result<std::unique_ptr<ScanContext>> Finish();

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};
}  // namespace paimon
