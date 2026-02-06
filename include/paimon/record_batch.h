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
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowArray;

namespace paimon {
/// `RecordBatch` encapsulates a batch of data with the same schema, supporting different types such
/// as `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, and `DELETE`. It is typically used in streaming
/// write or batch processing scenarios, with underlying data stored in the Apache Arrow format.
/// @note Do not use this class directly, use `RecordBatchBuilder` to build a `RecordBatch` which
/// has input validation.
class PAIMON_EXPORT RecordBatch {
 public:
    enum class PAIMON_EXPORT RowKind : int8_t {
        INSERT = 0,
        UPDATE_BEFORE = 1,
        UPDATE_AFTER = 2,
        DELETE = 3,
    };

    /// @note 1. Data cannot be reused, as it will be released after Write. 2. If a partition
    /// field's value is null, it should be represented as "__DEFAULT_PARTITION__"(or a user-defined
    /// default value) in the partition map. However, in the Arrow array, partition column values
    /// MUST NOT be set to "__DEFAULT_PARTITION__" (or a user-defined default value). Instead, they
    /// should be properly set as actual nulls. If used, it may lead to behavioral inconsistencies
    /// between C++ Paimon and Java Paimon.
    RecordBatch(const std::map<std::string, std::string>& partition, int32_t bucket,
                const std::vector<RowKind>& row_kinds, ArrowArray* data);
    ~RecordBatch();

    RecordBatch(RecordBatch&&);
    RecordBatch& operator=(RecordBatch&&);

    RecordBatch(const RecordBatch&) = delete;
    RecordBatch& operator=(const RecordBatch&) = delete;

    const std::map<std::string, std::string>& GetPartition() const {
        return partition_;
    }

    int32_t GetBucket() const {
        return bucket_;
    }

    ArrowArray* GetData() const {
        return data_;
    }

    const std::vector<RecordBatch::RowKind>& GetRowKind() const {
        return row_kinds_;
    }

    void SetBucket(int32_t bucket) {
        bucket_ = bucket;
    }

    bool HasSpecifiedBucket() const;

 private:
    std::map<std::string, std::string> partition_;
    int32_t bucket_;
    std::vector<RecordBatch::RowKind> row_kinds_;
    ::ArrowArray* data_;
};

/// Builder for constructing `RecordBatch` instances.
///
/// This class provides a convenient way to build `RecordBatch` objects by setting
/// various properties such as data, row kinds, partition information, and bucket id.
class PAIMON_EXPORT RecordBatchBuilder {
 public:
    /// Constructs a `RecordBatchBuilder` with Arrow data
    ///
    /// @note The `data` must conform to table schema:
    ///       - Each array in `data` corresponds to a field in table schema.
    ///       - If a field in table schema is marked as non-nullable (`nullable = false`),
    ///         the corresponding array in `data` must have zero null entries.
    ///
    /// @note Consistency between `data` and table schema will be validated during the write
    /// process.
    ///
    /// @param data ArrowArray struct containing the columnar data (via C Data Interface)
    explicit RecordBatchBuilder(::ArrowArray* data);

    ~RecordBatchBuilder();

    /// Move new Arrow data into the builder, replacing existing data.
    /// @param data New Arrow array data.
    RecordBatchBuilder& MoveData(::ArrowArray* data);

    /// Set the row kinds for each record in the batch.
    /// @param row_kinds A vector of row kinds, including INSERT, UPDATE_BEFORE, UPDATE_AFTER and
    /// DELETE. If not set, default value is `INSERT`.
    /// @note `row_kinds` must have the same length as the number of records in the data.
    RecordBatchBuilder& SetRowKinds(const std::vector<RecordBatch::RowKind>& row_kinds);

    /// Set the partition information for this record batch.
    /// @param data Map of partition column names to their string values.
    RecordBatchBuilder& SetPartition(const std::map<std::string, std::string>& data);

    /// Set the bucket id for this record batch. If not set, default value is `-1`.
    /// @param bucket The bucket id for data distribution.
    RecordBatchBuilder& SetBucket(int32_t bucket);

    /// Build and return the final `RecordBatch` instance.
    ///
    /// This method validates the configuration and creates `RecordBatch` with all
    /// the specified properties.
    Result<std::unique_ptr<RecordBatch>> Finish();

    class Impl;

 private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
