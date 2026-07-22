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

#include "paimon/visibility.h"

namespace paimon {

/// Catalog-level configuration option keys; table-level keys live in `Options`.
struct PAIMON_EXPORT CatalogOptions {
    /// "metastore" - Metastore of the paimon catalog.
    /// Supported values are "filesystem" (default) and "rest".
    static const char METASTORE[];

    /// "uri" - Server url of the REST catalog. Only used when METASTORE is "rest".
    static const char URI[];

    /// "token" - Token of the "bear" token provider of the REST catalog.
    static const char TOKEN[];

    /// "token.provider" - Authentication provider of the REST catalog. Only "bear" is
    /// supported ("bear" is the protocol's historical spelling of "bearer", do not "fix" it).
    static const char TOKEN_PROVIDER[];

    /// "table-default." - Prefix of the catalog options that provide table option
    /// defaults: "table-default.<key>=<value>" applies "<key>=<value>" to a created
    /// table when the caller left "<key>" unset.
    static const char TABLE_DEFAULT_OPTION_PREFIX[];
};

}  // namespace paimon
