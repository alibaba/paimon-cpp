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
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/global_index/btree/btree_defs.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/common/sst/sst_file_reader.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/utils/roaring_bitmap64.h"
namespace paimon {

/// Reader for BTree Global Index files.
/// This reader evaluates filter predicates against a BTree-based SST file
/// where each key maps to a list of row IDs.
class BTreeGlobalIndexReader : public GlobalIndexReader,
                               public std::enable_shared_from_this<BTreeGlobalIndexReader> {
 public:
    static Result<std::shared_ptr<BTreeGlobalIndexReader>> Create(
        const std::shared_ptr<SstFileReader>& sst_file_reader, RoaringBitmap64&& null_bitmap,
        const std::optional<MemorySlice>& min_key_slice,
        const std::optional<MemorySlice>& max_key_slice,
        const std::shared_ptr<arrow::DataType>& key_type, const std::shared_ptr<MemoryPool>& pool);

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
    BTreeGlobalIndexReader(const std::shared_ptr<SstFileReader>& sst_file_reader,
                           RoaringBitmap64&& null_bitmap, std::optional<Literal> min_key,
                           std::optional<Literal> max_key, std::optional<MemorySlice> min_key_slice,
                           std::optional<MemorySlice> max_key_slice,
                           const std::shared_ptr<arrow::DataType>& key_type,
                           const std::shared_ptr<MemoryPool>& pool);

    Result<RoaringBitmap64> RangeQuery(const std::optional<Literal>& from,
                                       const std::optional<Literal>& to, bool from_inclusive,
                                       bool to_inclusive);

    Status DeserializeRowIds(const MemorySlice& slice, std::vector<int64_t>* out) const;

    Result<RoaringBitmap64> AllNonNullRows();

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<SstFileReader> sst_file_reader_;
    RoaringBitmap64 null_bitmap_;
    std::optional<Literal> min_key_;
    std::optional<Literal> max_key_;
    /// Cached serialized min/max key slices to avoid repeated serialization in RangeQuery.
    std::optional<MemorySlice> min_key_slice_;
    std::optional<MemorySlice> max_key_slice_;

    std::shared_ptr<arrow::DataType> key_type_;
    MemorySlice::SliceComparator comparator_;
};

}  // namespace paimon
