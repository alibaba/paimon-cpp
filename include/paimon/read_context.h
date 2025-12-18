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

#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class MemoryPool;
class Predicate;

/// `ReadContext` is some configuration for read operations.
///
/// Please do not use this class directly, use `ReadContextBuilder` to build a `ReadContext` which
/// has input validation.
/// @see ReadContextBuilder
class PAIMON_EXPORT ReadContext {
 public:
    ReadContext(const std::string& path, const std::string& branch,
                const std::vector<std::string>& read_schema,
                const std::shared_ptr<Predicate>& predicate, bool enable_predicate_filter,
                bool enable_prefetch, uint32_t prefetch_batch_count,
                uint32_t prefetch_max_parallel_num, bool enable_multi_thread_row_to_batch,
                uint32_t row_to_batch_thread_number, const std::optional<std::string>& table_schema,
                const std::shared_ptr<MemoryPool>& memory_pool,
                const std::shared_ptr<Executor>& executor,
                const std::map<std::string, std::string>& fs_scheme_to_identifier_map,
                const std::map<std::string, std::string>& options);
    ~ReadContext();

    const std::string& GetPath() const {
        return path_;
    }

    const std::string& GetBranch() const {
        return branch_;
    }

    const std::map<std::string, std::string>& GetFileSystemSchemeToIdentifierMap() const {
        return fs_scheme_to_identifier_map_;
    }

    const std::map<std::string, std::string>& GetOptions() const {
        return options_;
    }

    const std::vector<std::string>& GetReadSchema() const {
        return read_schema_;
    }

    const std::shared_ptr<Predicate>& GetPredicate() const {
        return predicate_;
    }

    bool EnablePredicateFilter() const {
        return enable_predicate_filter_;
    }
    bool EnablePrefetch() const {
        return enable_prefetch_;
    }
    uint32_t GetPrefetchBatchCount() const {
        return prefetch_batch_count_;
    }
    uint32_t GetPrefetchMaxParallelNum() const {
        return prefetch_max_parallel_num_;
    }
    bool EnableMultiThreadRowToBatch() const {
        return enable_multi_thread_row_to_batch_;
    }
    uint32_t GetRowToBatchThreadNumber() const {
        return row_to_batch_thread_number_;
    }
    const std::optional<std::string>& GetSpecificTableSchema() {
        return table_schema_;
    }
    std::shared_ptr<MemoryPool> GetMemoryPool() const {
        return memory_pool_;
    }
    std::shared_ptr<Executor> GetExecutor() const {
        return executor_;
    }

 private:
    std::string path_;
    std::string branch_;
    std::vector<std::string> read_schema_;
    std::shared_ptr<Predicate> predicate_;
    bool enable_predicate_filter_;
    bool enable_prefetch_;
    uint32_t prefetch_batch_count_;
    uint32_t prefetch_max_parallel_num_;
    bool enable_multi_thread_row_to_batch_;
    uint32_t row_to_batch_thread_number_;
    std::optional<std::string> table_schema_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::map<std::string, std::string> fs_scheme_to_identifier_map_;
    std::map<std::string, std::string> options_;
};

/// `ReadContextBuilder` used to build a `ReadContext`, has input validation.
class PAIMON_EXPORT ReadContextBuilder {
 public:
    /// Constructs a `ReadContextBuilder` with required parameters.
    /// @param path The root path of the table.
    explicit ReadContextBuilder(const std::string& path);

    ~ReadContextBuilder();

    /// Set the schema fields to read from the table.
    ///
    /// If not set, all fields from the table schema will be read. This is useful for
    /// projection pushdown to reduce I/O and improve performance by reading only
    /// the required columns.
    ///
    /// @param read_field_names Vector of field names to read from the table.
    /// @return Reference to this builder for method chaining.
    /// @note Currently supports top-level field selection. Future versions may support
    ///       nested field selection using ArrowSchema for more granular projection
    ReadContextBuilder& SetReadSchema(const std::vector<std::string>& read_field_names);

    /// Set a configuration options map to set some option entries which are not defined in the
    /// table schema or whose values you want to overwrite.
    /// @note The options map will clear the options added by `AddOption()` before.
    /// @param options The configuration options map.
    /// @return Reference to this builder for method chaining.
    ReadContextBuilder& SetOptions(const std::map<std::string, std::string>& options);

    /// Add a single configuration option which is not defined in the table schema or whose value
    /// you want to overwrite.
    ///
    /// If you want to add multiple options, call `AddOption()` multiple times or use `SetOptions()`
    /// instead.
    /// @param key The option key.
    /// @param value The option value.
    /// @return Reference to this builder for method chaining.
    ReadContextBuilder& AddOption(const std::string& key, const std::string& value);

    /// Set a predicate for filtering data during reading.
    ///
    /// The predicate is used for both partition pruning and data filtering.
    /// It can significantly improve performance by reducing the amount of data
    /// that needs to be read and processed.
    ///
    /// @param predicate Shared pointer to the predicate for data filtering.
    /// @return Reference to this builder for method chaining.
    ReadContextBuilder& SetPredicate(const std::shared_ptr<Predicate>& predicate);

    /// Whether to perform precise filtering according to predicates for data read from format
    /// reader.
    /// @param enabled Whether to enable precise filtering (default: false)
    /// @return Reference to this builder for method chaining.
    ReadContextBuilder& EnablePredicateFilter(bool enabled);

    /// Enable or disable prefetching of data batches from individual files.
    ///
    /// When enabled, the reader will prefetch multiple batches in parallel to
    /// improve throughput by overlapping I/O with computation. This is particularly
    /// beneficial for high-latency storage systems.
    ///
    /// @param enabled Whether to enable prefetching (default: false)
    /// @return Reference to this builder for method chaining.
    ReadContextBuilder& EnablePrefetch(bool enabled);

    /// Set the total number of batches to prefetch across all files.
    ///
    /// This controls the memory usage and parallelism of the prefetching mechanism.
    /// Higher values can improve throughput but consume more memory.
    ///
    /// @param batch_count Total number of batches to prefetch (default: 600)
    /// @return Reference to this builder for method chaining.
    ReadContextBuilder& SetPrefetchBatchCount(uint32_t batch_count);

    /// Set the maximum number of parallel prefetch operations.
    ///
    /// This limits the number of concurrent I/O operations to prevent overwhelming
    /// the storage system or consuming excessive system resources.
    ///
    /// @param parallel_num Maximum parallel prefetch operations (default: 3)
    /// @return Reference to this builder for method chaining.
    ReadContextBuilder& SetPrefetchMaxParallelNum(uint32_t parallel_num);

    /// Enable or disable multi-threaded row-to-batch conversion in merge-on-read scenarios.
    ///
    /// When enabled, multiple threads are used to convert row data to batch format
    /// during merge operations, which can improve performance for CPU-intensive
    /// merge operations.
    ///
    /// @param enabled Whether to enable multi-threaded conversion (default: false)
    /// @return Reference to this builder for method chaining.
    ReadContextBuilder& EnableMultiThreadRowToBatch(bool enabled);

    /// Set the number of threads for row-to-batch conversion in merge-on-read scenarios.
    ///
    /// This controls the parallelism of row-to-batch conversion during merge operations.
    /// Higher values can improve performance but may affect result ordering.
    ///
    /// @param thread_number Number of conversion threads (default: 1)
    /// @return Reference to this builder for method chaining.
    /// @note If thread_number > 1, Arrow batches from the reader may not be in primary key order.
    ReadContextBuilder& SetRowToBatchThreadNumber(uint32_t thread_number);

    /// Set custom memory pool for memory management.
    /// @param memory_pool The memory pool to use.
    /// @return Reference to this builder for method chaining.
    /// @note If not set, the default system memory pool will be used.
    ReadContextBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& memory_pool);

    /// Set custom executor for task execution.
    /// @param executor The executor to use.
    /// @return Reference to this builder for method chaining.
    /// @note If not set, the default system executor will be used.
    ReadContextBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);

    /// Set the table schema as a string to avoid schema loading I/O operations.
    ///
    /// This optimization allows the reader to use a pre-loaded schema instead of
    /// reading it from the table metadata, which can improve performance especially
    /// in scenarios with many small read operations.
    ///
    /// @param table_schema String representation of the table schema.
    /// @return Reference to this builder for method chaining.
    /// @note The user must ensure that the schema string is valid and matches the table.
    /// @note If not set, the schema will be loaded from the table path.
    ReadContextBuilder& SetTableSchema(const std::string& table_schema);

    /// Set the specific branch to read from in a versioned table.
    ///
    /// Paimon supports branching for data versioning and time travel queries.
    /// This method allows reading from a specific branch instead of the main branch.
    ///
    /// @param branch Name of the branch to read from.
    /// @return Reference to this builder for method chaining.
    /// @note Default branch is "main" if not specified.
    ReadContextBuilder& WithBranch(const std::string& branch);

    /// Set the file system scheme to identifier mapping for custom file system configurations.
    /// This allows using different file system implementations for different URI schemes.
    ///
    /// @param fs_scheme_to_identifier_map Map from URI scheme to file system identifier.
    /// @return Reference to this builder for method chaining.
    /// @note If not set, use default file system (configured in `Options::FILE_SYSTEM`).
    ReadContextBuilder& WithFileSystemSchemeToIdentifierMap(
        const std::map<std::string, std::string>& fs_scheme_to_identifier_map);

    /// Build and return a `ReadContext` instance with input validation.
    /// @return Result containing the constructed `ReadContext` or an error status.
    Result<std::unique_ptr<ReadContext>> Finish();

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};
}  // namespace paimon
