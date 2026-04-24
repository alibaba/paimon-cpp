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

#include "paimon/global_index/global_indexer_factory.h"

#include <cassert>
#include <utility>

#include "paimon/factories/factory_creator.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/status.h"

namespace paimon {
const char GlobalIndexerFactory::GLOBAL_INDEX_IDENTIFIER_SUFFIX[] = "-global";

Result<std::unique_ptr<GlobalIndexer>> GlobalIndexerFactory::Get(
    const std::string& identifier, const std::map<std::string, std::string>& options) {
    // Compatibility: "lumina-vector-ann" was the old identifier for lumina global index.
    std::string final_identifier = (identifier == "lumina-vector-ann" ? "lumina" : identifier);
    std::string global_index_identifier = final_identifier + GLOBAL_INDEX_IDENTIFIER_SUFFIX;
    auto factory_creator = FactoryCreator::GetInstance();
    if (factory_creator == nullptr) {
        assert(false);
        return Status::Invalid("factory creator is null pointer");
    }
    auto global_indexer_factory =
        dynamic_cast<GlobalIndexerFactory*>(factory_creator->Create(global_index_identifier));
    if (global_indexer_factory == nullptr) {
        // if an index type is not found, return nullptr to skip this index instead of return error
        return std::unique_ptr<GlobalIndexer>();
    }
    return global_indexer_factory->Create(options);
}
}  // namespace paimon
