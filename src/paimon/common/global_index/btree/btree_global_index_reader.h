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
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/sst/sst_file_reader.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

/// Reader for BTree Global Index files.
/// This reader evaluates filter predicates against a BTree-based SST file
/// where each key maps to a list of row IDs.
class BTreeGlobalIndexReader : public GlobalIndexReader {
 public:
    BTreeGlobalIndexReader(
        const std::shared_ptr<SstFileReader>& sst_file_reader,
        const std::shared_ptr<RoaringBitmap64>& null_bitmap, const MemorySlice& min_key,
        const MemorySlice& max_key, bool has_min_key, const std::vector<GlobalIndexIOMeta>& files,
        const std::shared_ptr<MemoryPool>& pool,
        std::function<int32_t(const MemorySlice&, const MemorySlice&)> comparator,
        int32_t ts_precision);

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

    Result<std::shared_ptr<GlobalIndexResult>> VisitBetween(const Literal& from,
                                                            const Literal& to) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotBetween(const Literal& from,
                                                               const Literal& to) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitAnd(
        const std::vector<Result<std::shared_ptr<GlobalIndexResult>>>& children) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitOr(
        const std::vector<Result<std::shared_ptr<GlobalIndexResult>>>& children) override;

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override;

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return "btree";
    }

 private:
    Result<RoaringBitmap64> RangeQuery(const MemorySlice& lower_bound,
                                       const MemorySlice& upper_bound, bool lower_inclusive,
                                       bool upper_inclusive);

    Result<RoaringBitmap64> AllNonNullRows();

    std::shared_ptr<SstFileReader> sst_file_reader_;
    std::shared_ptr<RoaringBitmap64> null_bitmap_;
    MemorySlice min_key_;
    MemorySlice max_key_;
    bool has_min_key_;
    std::vector<GlobalIndexIOMeta> files_;
    std::shared_ptr<MemoryPool> pool_;
    std::function<int32_t(const MemorySlice&, const MemorySlice&)> comparator_;
    int32_t ts_precision_;
};

}  // namespace paimon
