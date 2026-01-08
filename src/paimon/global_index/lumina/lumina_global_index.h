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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/api.h"
#include "lumina/api/Options.h"
#include "lumina/extensions/SearchWithFilterExtension.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/lumina/lumina_memory_pool.h"

namespace paimon::lumina {
class LuminaGlobalIndex : public GlobalIndexer {
 public:
    explicit LuminaGlobalIndex(const std::map<std::string, std::string>& options)
        : options_(options) {}

    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::vector<GlobalIndexIOMeta>& files,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    static std::unordered_map<std::string, std::string> FetchLuminaOptions(
        const std::map<std::string, std::string>& options);

    static constexpr char kOptionKeyPrefix[] = "lumina.";

    std::map<std::string, std::string> options_;
};

class LuminaIndexWriter : public GlobalIndexWriter {
 public:
    LuminaIndexWriter(const std::string& field_name,
                      const std::shared_ptr<arrow::DataType>& arrow_type, uint32_t dimension,
                      const std::shared_ptr<GlobalIndexFileWriter>& file_manager,
                      ::lumina::api::BuilderOptions&& builder_options,
                      ::lumina::api::IOOptions&& io_options,
                      const std::shared_ptr<LuminaMemoryPool>& pool);

    Status AddBatch(::ArrowArray* arrow_array) override;

    Result<std::vector<GlobalIndexIOMeta>> Finish() override;

 private:
    static constexpr char kIdentifier[] = "lumina";

    int64_t count_ = 0;
    std::shared_ptr<LuminaMemoryPool> pool_;
    std::string field_name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    uint32_t dimension_;
    std::shared_ptr<GlobalIndexFileWriter> file_manager_;
    ::lumina::api::BuilderOptions builder_options_;
    ::lumina::api::IOOptions io_options_;
    std::vector<std::shared_ptr<arrow::FloatArray>> array_vec_;
};

class LuminaIndexReader : public GlobalIndexReader {
 public:
    LuminaIndexReader(
        int64_t range_end, ::lumina::api::SearchOptions&& search_options,
        std::unique_ptr<::lumina::api::LuminaSearcher>&& searcher,
        std::unique_ptr<::lumina::extensions::SearchWithFilterExtension>&& searcher_with_filter,
        const std::shared_ptr<LuminaMemoryPool>& pool);

    ~LuminaIndexReader() override {
        [[maybe_unused]] auto status = searcher_->Close();
    }

    Result<std::shared_ptr<VectorSearchGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override {
        return BitmapGlobalIndexResult::FromRanges({Range(0, range_end_)});
    }

 private:
    int64_t range_end_;
    std::shared_ptr<LuminaMemoryPool> pool_;
    ::lumina::api::SearchOptions search_options_;
    std::unique_ptr<::lumina::api::LuminaSearcher> searcher_;
    std::unique_ptr<::lumina::extensions::SearchWithFilterExtension> searcher_with_filter_;
};
}  // namespace paimon::lumina
