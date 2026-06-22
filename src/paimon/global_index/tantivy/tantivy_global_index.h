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

#include "arrow/type.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/tantivy/tantivy_defs.h"

namespace paimon::tantivy {

/// `GlobalIndexer` implementation backed by tantivy-fulltext. Counterpart to
/// `LuceneGlobalIndex`; the two coexist and are NOT cross-readable. Selection
/// between them happens at the factory layer via the `index_type` identifier.
class TantivyGlobalIndex : public GlobalIndexer {
 public:
    explicit TantivyGlobalIndex(const std::map<std::string, std::string>& options);

    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::vector<GlobalIndexIOMeta>& files,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    /// Options after the `tantivy-fulltext.` prefix has been stripped.
    std::map<std::string, std::string> options_;
};

}  // namespace paimon::tantivy
