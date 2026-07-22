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

#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/status.h"

namespace paimon {

/// Checks shared by the catalog implementations. Every check takes an `action` naming
/// the rejected operation in the error message, e.g. "dropTable".
class CatalogUtils {
 public:
    CatalogUtils() = delete;
    ~CatalogUtils() = delete;

    /// Returns whether `db_name` is the reserved system database "sys".
    static bool IsSystemDatabase(const std::string& db_name);

    /// Fails when `db_name` is the system database.
    static Status CheckNotSystemDatabase(const std::string& db_name, const std::string& action);

    /// Fails when `identifier` denotes a system table or any table of the system database.
    static Status CheckNotSystemTable(const Identifier& identifier, const std::string& action);

    /// Fails when `identifier` carries a "$branch_" suffix.
    static Status CheckNotBranch(const Identifier& identifier, const std::string& action);
};

}  // namespace paimon
