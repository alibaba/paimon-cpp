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

#include <string>

#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {

/// An identifier for a table containing database and table name.
class PAIMON_EXPORT Identifier {
 public:
    static const char kUnknownDatabase[];

    explicit Identifier(const std::string& table);
    Identifier(const std::string& database, const std::string& table);

    bool operator==(const Identifier& other);
    const std::string& GetDatabaseName() const;
    const std::string& GetTableName() const;
    std::string ToString() const;

 private:
    const std::string database_;
    const std::string table_;
};

}  // namespace paimon
