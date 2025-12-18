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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

/// This interface provides access to TableSchema-related information.
class PAIMON_EXPORT Schema {
 public:
    virtual ~Schema() = default;

    /// Get the Arrow C schema representation of this table schema.
    /// @return A result containing an ArrowSchema, or an error status if conversion fails.
    virtual Result<std::unique_ptr<::ArrowSchema>> GetArrowSchema() const = 0;

    /// Get the names of all fields in the table schema.
    /// @return A vector of field names.
    virtual std::vector<std::string> FieldNames() const = 0;

    /// Get the unique identifier of this table schema.
    /// @return The schema ID
    virtual int64_t Id() const = 0;

    /// Get the list of primary key field names.
    /// @return A reference to the vector of primary key names; empty if no primary keys are
    /// defined.
    virtual const std::vector<std::string>& PrimaryKeys() const = 0;

    /// Get the list of partition key field names.
    /// @return A reference to the vector of partition key names; empty if the table is not
    /// partitioned.
    virtual const std::vector<std::string>& PartitionKeys() const = 0;

    /// Get the list of bucket key field names used for bucketing.
    /// @return A reference to the vector of bucket key names.
    virtual const std::vector<std::string>& BucketKeys() const = 0;

    /// Get the number of buckets configured for this table.
    /// @return The number of buckets.
    virtual int32_t NumBuckets() const = 0;

    /// Get the highest field ID assigned in this schema.
    /// @return The maximum field ID.
    virtual int32_t HighestFieldId() const = 0;

    /// Get the table-level options associated with this schema.
    /// @return A reference to the map of option key-value pairs (e.g., file format, filesystem).
    virtual const std::map<std::string, std::string>& Options() const = 0;

    /// Get an optional comment describing the table.
    /// @return The table comment if set, or std::nullopt otherwise.
    virtual std::optional<std::string> Comment() const = 0;
};

}  // namespace paimon
