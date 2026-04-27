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
#include <utility>
#include <vector>

#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/predicate/function_visitor.h"

namespace paimon {

/// Selects candidate BTree index files based on filter predicates.
class BTreeFileMetaSelector : public FunctionVisitor<std::vector<GlobalIndexIOMeta>> {
 public:
    BTreeFileMetaSelector(const std::vector<GlobalIndexIOMeta>& files,
                          const std::shared_ptr<arrow::DataType>& key_type,
                          const std::shared_ptr<MemoryPool>& pool);

    Result<std::vector<GlobalIndexIOMeta>> VisitIsNotNull() override;
    Result<std::vector<GlobalIndexIOMeta>> VisitIsNull() override;
    Result<std::vector<GlobalIndexIOMeta>> VisitEqual(const Literal& literal) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitNotEqual(const Literal& literal) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitLessThan(const Literal& literal) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitLessOrEqual(const Literal& literal) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitGreaterThan(const Literal& literal) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitGreaterOrEqual(const Literal& literal) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitIn(const std::vector<Literal>& literals) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitNotIn(
        const std::vector<Literal>& literals) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitStartsWith(const Literal& prefix) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitEndsWith(const Literal& suffix) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitContains(const Literal& literal) override;
    Result<std::vector<GlobalIndexIOMeta>> VisitLike(const Literal& literal) override;

 private:
    using MetaPredicate = std::function<Result<bool>(const BTreeIndexMeta&)>;

    Result<std::vector<GlobalIndexIOMeta>> Filter(const MetaPredicate& predicate) const;

    Result<Literal> DeserializeKey(const std::shared_ptr<Bytes>& key_bytes) const;

    std::vector<std::pair<GlobalIndexIOMeta, std::shared_ptr<BTreeIndexMeta>>> files_;
    std::shared_ptr<arrow::DataType> key_type_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
