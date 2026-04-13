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

#include "paimon/catalog/identifier.h"

#include "fmt/format.h"

namespace paimon {

const char Identifier::kUnknownDatabase[] = "unknown";

Identifier::Identifier(const std::string& table)
    : Identifier(std::string(kUnknownDatabase), table) {}

Identifier::Identifier(const std::string& database, const std::string& table)
    : database_(database), table_(table) {}

bool Identifier::operator==(const Identifier& other) {
    return (database_ == other.database_ && table_ == other.table_);
}

const std::string& Identifier::GetDatabaseName() const {
    return database_;
}

const std::string& Identifier::GetTableName() const {
    return table_;
}

std::string Identifier::ToString() const {
    return fmt::format("Identifier{{database='{}', table='{}'}}", database_, table_);
}

}  // namespace paimon
