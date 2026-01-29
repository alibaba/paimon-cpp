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
#include "lucene++/LuceneHeaders.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/predicate/full_text_search.h"
namespace paimon::lucene {
class LuceneGlobalIndex : public GlobalIndexer {
 public:
    explicit LuceneGlobalIndex(const std::map<std::string, std::string>& options);

    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::vector<GlobalIndexIOMeta>& files,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    std::map<std::string, std::string> options_;
};

class LuceneGlobalIndexWriter : public GlobalIndexWriter {
 public:
    struct LuceneWriteContext {
        LuceneWriteContext(const std::string& _tmp_index_path,
                           const Lucene::FSDirectoryPtr& _lucene_dir,
                           const Lucene::IndexWriterPtr& _index_writer,
                           const Lucene::DocumentPtr& _doc, const Lucene::FieldPtr& _field,
                           const Lucene::FieldPtr& _row_id_field);

        LuceneWriteContext(LuceneWriteContext&&) = default;
        LuceneWriteContext& operator=(LuceneWriteContext&&) = default;

        std::string tmp_index_path;
        Lucene::FSDirectoryPtr lucene_dir;
        Lucene::IndexWriterPtr index_writer;
        Lucene::DocumentPtr doc;
        Lucene::FieldPtr field;
        Lucene::FieldPtr row_id_field;
    };

    static Result<std::shared_ptr<LuceneGlobalIndexWriter>> Create(
        const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    ~LuceneGlobalIndexWriter() override;

    Status AddBatch(::ArrowArray* c_arrow_array) override;

    Result<std::vector<GlobalIndexIOMeta>> Finish() override;

 private:
    LuceneGlobalIndexWriter(const std::string& field_name,
                            const std::shared_ptr<arrow::DataType>& arrow_type,
                            LuceneWriteContext&& write_context,
                            const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
                            const std::map<std::string, std::string>& options,
                            const std::shared_ptr<MemoryPool>& pool);

    Result<std::string> FlushIndexToFinal() const;

 private:
    std::shared_ptr<MemoryPool> pool_;
    int32_t row_id_ = 0;
    std::string field_name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    LuceneWriteContext write_context_;
    std::shared_ptr<GlobalIndexFileWriter> file_writer_;
    std::map<std::string, std::string> options_;
};

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

    Result<std::shared_ptr<VectorSearchGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        return Status::Invalid(
            "LuceneGlobalIndexReader is not supposed to handle vector search query");
    }

    Result<std::shared_ptr<VectorSearchGlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search);

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return kIdentifier;
    }

 private:
    LuceneGlobalIndexReader(const std::wstring& wfield_name, int64_t range_end,
                            const Lucene::IndexSearcherPtr& searcher)
        : range_end_(range_end), wfield_name_(wfield_name), searcher_(searcher) {}

    static std::vector<std::wstring> TokenizeQuery(const std::string& query);

    std::shared_ptr<GlobalIndexResult> CreateAllResult() const {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

 private:
    int64_t range_end_;
    std::wstring wfield_name_;
    Lucene::IndexSearcherPtr searcher_;
};
}  // namespace paimon::lucene
