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

#include <memory>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "cppjieba/Jieba.hpp"
#include "lucene++/LuceneHeaders.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/predicate/full_text_search.h"

namespace paimon::lucene {
class LuceneGlobalIndexReader : public GlobalIndexReader {
 public:
    static Result<std::shared_ptr<LuceneGlobalIndexReader>> Create(
        const std::string& field_name, const GlobalIndexIOMeta& io_meta,
        const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override {
        return CreateAllResult();
    }

    Result<std::shared_ptr<VectorSearchGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        return Status::Invalid(
            "LuceneGlobalIndexReader is not supposed to handle vector search query");
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search);

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return kIdentifier;
    }

 private:
    LuceneGlobalIndexReader(const std::wstring& wfield_name, int64_t range_end,
                            const Lucene::IndexSearcherPtr& searcher,
                            const std::string& tokenize_mode,
                            const std::shared_ptr<cppjieba::Jieba>& jieba)
        : range_end_(range_end),
          wfield_name_(wfield_name),
          searcher_(searcher),
          tokenize_mode_(tokenize_mode),
          jieba_(jieba) {}

    std::vector<std::wstring> TokenizeQuery(const std::string& query) const;

    std::shared_ptr<GlobalIndexResult> CreateAllResult() const {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Lucene::QueryPtr ConstructMatchQuery(
        const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false);

    Lucene::QueryPtr ConstructPhraseQuery(
        const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false);

    Lucene::QueryPtr ConstructPrefixQuery(
        const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false);

    Lucene::QueryPtr ConstructWildCardQuery(
        const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false);

    Result<std::shared_ptr<GlobalIndexResult>> SearchWithLimit(
        const Lucene::QueryPtr& query,
        const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false);

    std::shared_ptr<GlobalIndexResult> SearchWithNoLimit(
        const Lucene::QueryPtr& query,
        const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false);

 private:
    int64_t range_end_;
    std::wstring wfield_name_;
    Lucene::IndexSearcherPtr searcher_;
    std::string tokenize_mode_;
    std::shared_ptr<cppjieba::Jieba> jieba_;
};
}  // namespace paimon::lucene
