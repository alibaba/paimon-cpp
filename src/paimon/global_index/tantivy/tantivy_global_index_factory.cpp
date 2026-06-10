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

#include "paimon/global_index/tantivy/tantivy_global_index_factory.h"

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "paimon/factories/factory.h"
#include "paimon/global_index/tantivy/tantivy_global_index.h"

namespace paimon::tantivy {

/// Identifier convention: lucene-fts uses "lucene-fts-global"; we use
/// "tantivy-fulltext-global" so `GlobalIndexerFactory::Get("tantivy-fulltext", ...)`
/// (which appends "-global") routes to us. Keeps both backends discoverable
/// via the same lookup path.
const char TantivyGlobalIndexFactory::IDENTIFIER[] = "tantivy-fulltext-global";

Result<std::unique_ptr<GlobalIndexer>> TantivyGlobalIndexFactory::Create(
    const std::map<std::string, std::string>& options) const {
    return std::make_unique<TantivyGlobalIndex>(options);
}

REGISTER_PAIMON_FACTORY(TantivyGlobalIndexFactory);

}  // namespace paimon::tantivy
