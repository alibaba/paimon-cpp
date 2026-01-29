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

#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
namespace paimon::lucene {
/// Factory for creating lucene global indexers.
class LuceneGlobalIndexFactory : public GlobalIndexerFactory {
 public:
    static const char IDENTIFIER[];

    const char* Identifier() const override {
        return IDENTIFIER;
    }

    Result<std::unique_ptr<GlobalIndexer>> Create(
        const std::map<std::string, std::string>& options) const override;
};

}  // namespace paimon::lucene
