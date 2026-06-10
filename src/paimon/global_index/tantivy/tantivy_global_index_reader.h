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

#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/tantivy/tantivy_defs.h"
#include "paimon/global_index/tantivy/tantivy_ffi_handle.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/full_text_search.h"

namespace paimon::tantivy {

/// Tantivy-backed implementation of `GlobalIndexReader`.
///
/// Mirrors LuceneGlobalIndexReader's surface but delegates query construction
/// + execution into Rust over FFI. Stage 6 supports the 5 FullTextSearch
/// SearchTypes (MATCH_ALL, MATCH_ANY, PHRASE, PREFIX, WILDCARD) without limit
/// or pre_filter — both of which Stage 7 layers on.
///
/// All non-FullTextSearch visit methods return nullptr (matches
/// LuceneGlobalIndexReader): the FTS index has no contribution for non-FTS
/// predicates, framework treats nullptr as "no filter constraint".
class TantivyGlobalIndexReader : public GlobalIndexReader {
 public:
    static Result<std::shared_ptr<TantivyGlobalIndexReader>> Create(
        const std::string& field_name, const GlobalIndexIOMeta& io_meta,
        const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    // === FunctionVisitor surface — non-FTS predicates fall back to full range. ===

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(const Literal&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(const std::vector<Literal>&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(const std::vector<Literal>&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal&) override {
        return CreateAllResult();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal&) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>&) override {
        return Status::Invalid(
            "TantivyGlobalIndexReader is not supposed to handle vector search query");
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override;

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return kIdentifier;
    }

 private:
    TantivyGlobalIndexReader(ReaderPtr reader, std::shared_ptr<MemoryPool> pool)
        : reader_(std::move(reader)), pool_(std::move(pool)) {}

    std::shared_ptr<GlobalIndexResult> CreateAllResult() const {
        return nullptr;
    }

    /// Owning handle to the Rust-side reader.
    ReaderPtr reader_;
    /// MemoryPool used for serializing pre-filter bitmaps to bytes for FFI.
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon::tantivy
