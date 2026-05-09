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

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/global_index/btree/btree_defs.h"
#include "paimon/common/global_index/btree/btree_file_meta_selector.h"
#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/common/sst/block_cache.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/executor.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
class LazyFilteredBTreeReader : public GlobalIndexReader {
 public:
    LazyFilteredBTreeReader(std::optional<int32_t> read_buffer_size,
                            const std::vector<GlobalIndexIOMeta>& files,
                            const std::shared_ptr<arrow::DataType>& key_type,
                            const std::shared_ptr<GlobalIndexFileReader>& file_reader,
                            const std::shared_ptr<CacheManager>& cache_manager,
                            const std::shared_ptr<MemoryPool>& pool,
                            const std::shared_ptr<Executor>& executor);

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override;

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override;

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return BtreeDefs::kIdentifier;
    }

 private:
    using SelectAction = std::function<Result<std::vector<GlobalIndexIOMeta>>()>;
    using ReaderAction = std::function<Result<std::shared_ptr<GlobalIndexResult>>(
        const std::shared_ptr<GlobalIndexReader>&)>;

    Result<std::shared_ptr<GlobalIndexResult>> DispatchVisit(SelectAction select_files,
                                                             ReaderAction action);
    Result<std::shared_ptr<GlobalIndexReader>> CreateUnionReader(
        const std::vector<GlobalIndexIOMeta>& files);
    Result<std::shared_ptr<GlobalIndexReader>> GetOrCreateReader(const GlobalIndexIOMeta& meta);
    Result<std::shared_ptr<GlobalIndexReader>> CreateSingleReader(const GlobalIndexIOMeta& meta);
    Result<RoaringBitmap64> ReadNullBitmap(const std::shared_ptr<BlockCache>& cache,
                                           const std::optional<BlockHandle>& block_handle);

 private:
    std::optional<int32_t> read_buffer_size_;
    std::shared_ptr<MemoryPool> pool_;
    BTreeFileMetaSelector file_selector_;
    std::shared_ptr<arrow::DataType> key_type_;
    std::shared_ptr<GlobalIndexFileReader> file_reader_;
    std::shared_ptr<CacheManager> cache_manager_;
    std::map<std::string, std::shared_ptr<GlobalIndexReader>> reader_cache_;
    std::shared_ptr<Executor> executor_;
};

}  // namespace paimon
