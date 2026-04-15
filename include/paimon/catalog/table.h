/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/catalog/identifier.h"
#include "paimon/result.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {

/// A table provides basic abstraction for table type.
class PAIMON_EXPORT Table {
 public:
    /// Creates a table by loading the latest schema from the specified table path.
    ///
    /// @param file_system File system used to access the table directory and schema files.
    /// @param table_path Root path of the table in the file system.
    /// @param identifier Logical table identifier used for naming and error messages.
    /// @return A table initialized with the latest available schema.
    static Result<std::shared_ptr<Table>> Create(const std::shared_ptr<FileSystem>& file_system,
                                                 const std::string& table_path,
                                                 const Identifier& identifier);

    Table(const std::shared_ptr<Schema>& schema, const std::string& database,
          const std::string& table_name)
        : schema_(schema), database_(database), table_name_(table_name) {}

    ~Table() = default;

    /// A name to identify this table.
    std::string Name() const {
        return table_name_;
    }

    /// Full name of the table, default is database.tableName.
    std::string FullName() const;

    /// UUID of the table, metastore can provide the true UUID of this table, default is the full
    /// name.
    std::string Uuid() const {
        return FullName();
    }

    /// Loads the latest schema of table.
    std::shared_ptr<Schema> LatestSchema() const {
        return schema_;
    }

 private:
    std::shared_ptr<Schema> schema_;
    std::string database_;
    std::string table_name_;
};

}  // namespace paimon
