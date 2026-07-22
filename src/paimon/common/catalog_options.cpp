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

#include "paimon/catalog_options.h"

namespace paimon {

const char CatalogOptions::METASTORE[] = "metastore";
const char CatalogOptions::URI[] = "uri";
const char CatalogOptions::TOKEN[] = "token";
const char CatalogOptions::TOKEN_PROVIDER[] = "token.provider";
const char CatalogOptions::TABLE_DEFAULT_OPTION_PREFIX[] = "table-default.";

}  // namespace paimon
