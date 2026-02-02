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

#include "paimon/result.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

/// A table provides basic abstraction for table type.
class PAIMON_EXPORT Table {
 public:
    /// A name to identify this table.
    virtual std::string Name() = 0;

    /// Full name of the table, default is database.tableName.
    virtual std::string FullName() {
        return Name();
    }

    /// UUID of the table, metastore can provide the true UUID of this table, default is the full
    /// name.
    virtual std::string Uuid() {
        return FullName();
    }

    /// Loads the latest schema of table.
    virtual std::shared_ptr<Schema> LatestSchema() = 0;
};

}  // namespace paimon
