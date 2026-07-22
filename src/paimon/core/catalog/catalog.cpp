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

#include "paimon/catalog_options.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/catalog/file_system_catalog.h"
#include "paimon/core/core_options.h"
#ifdef PAIMON_ENABLE_REST
#include "paimon/rest/rest_catalog.h"
#endif

namespace paimon {

const char Catalog::SYSTEM_DATABASE_NAME[] = "sys";
const char Catalog::SYSTEM_TABLE_SPLITTER[] = "$";
const char Catalog::DB_SUFFIX[] = ".db";
const char Catalog::DB_LOCATION_PROP[] = "location";

Result<std::unique_ptr<Catalog>> Catalog::Create(const std::string& root_path,
                                                 const std::map<std::string, std::string>& options,
                                                 const std::shared_ptr<FileSystem>& file_system) {
    std::string metastore = "filesystem";
    auto metastore_iter = options.find(CatalogOptions::METASTORE);
    if (metastore_iter != options.end()) {
        // Matched leniently in lower case; the Java catalog factory looks the metastore
        // up by its exact identifier, so only the exact spelling is portable.
        metastore = StringUtils::ToLowerCase(metastore_iter->second);
    }
    if (metastore == "rest") {
#ifdef PAIMON_ENABLE_REST
        return RestCatalog::Create(root_path, options, file_system);
#else
        return Status::NotImplemented(
            "the rest catalog requires building paimon with PAIMON_ENABLE_REST=ON");
#endif
    }
    if (metastore != "filesystem") {
        return Status::Invalid("unsupported metastore: ", metastore);
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options, file_system));
    return std::make_unique<FileSystemCatalog>(core_options.GetFileSystem(), root_path, options);
}

}  // namespace paimon
