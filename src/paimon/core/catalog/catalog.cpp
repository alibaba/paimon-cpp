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

#include "paimon/catalog/catalog.h"

#include <utility>

#include "paimon/core/catalog/file_system_catalog.h"
#include "paimon/core/core_options.h"

namespace paimon {

const char Catalog::SYSTEM_DATABASE_NAME[] = "sys";
const char Catalog::SYSTEM_TABLE_SPLITTER[] = "$";
const char Catalog::DB_SUFFIX[] = ".db";
const char Catalog::DB_LOCATION_PROP[] = "location";

Result<std::unique_ptr<Catalog>> Catalog::Create(const std::string& root_path,
                                                 const std::map<std::string, std::string>& options,
                                                 const std::shared_ptr<FileSystem>& file_system) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options, file_system));
    return std::make_unique<FileSystemCatalog>(core_options.GetFileSystem(), root_path);
}

}  // namespace paimon
