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

#include "paimon/common/global_index/btree/btree_file_meta_selector.h"

#include "paimon/common/memory/memory_slice.h"

namespace paimon {
BTreeFileMetaSelector::BTreeFileMetaSelector(const std::vector<GlobalIndexIOMeta>& files,
                                             const std::shared_ptr<arrow::DataType>& key_type,
                                             const std::shared_ptr<MemoryPool>& pool)
    : key_type_(key_type),
      pool_(pool),
      comparator_(KeySerializer::CreateComparator(key_type, pool)) {
    files_.reserve(files.size());
    for (const auto& file : files) {
        auto index_meta = BTreeIndexMeta::Deserialize(file.metadata, pool.get());
        files_.emplace_back(file, std::move(index_meta));
    }
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitIsNotNull() {
    return Filter([](const BTreeIndexMeta& meta) -> Result<bool> { return !meta.OnlyNulls(); });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitIsNull() {
    return Filter([](const BTreeIndexMeta& meta) -> Result<bool> { return meta.HasNulls(); });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitEqual(const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const BTreeIndexMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        MemorySlice min_key_slice = WrapKeySlice(meta.FirstKey());
        MemorySlice max_key_slice = WrapKeySlice(meta.LastKey());
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp_min, comparator_(literal_slice, min_key_slice));
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp_max, comparator_(literal_slice, max_key_slice));
        return cmp_min >= 0 && cmp_max <= 0;
    });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitNotEqual(
    const Literal& literal) {
    return Filter([](const BTreeIndexMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitLessThan(
    const Literal& literal) {
    // file.minKey < literal
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const BTreeIndexMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        MemorySlice min_key_slice = WrapKeySlice(meta.FirstKey());
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, comparator_(min_key_slice, literal_slice));
        return cmp < 0;
    });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitLessOrEqual(
    const Literal& literal) {
    // file.minKey <= literal
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const BTreeIndexMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        MemorySlice min_key_slice = WrapKeySlice(meta.FirstKey());
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, comparator_(min_key_slice, literal_slice));
        return cmp <= 0;
    });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitGreaterThan(
    const Literal& literal) {
    // file.maxKey > literal
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const BTreeIndexMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        MemorySlice max_key_slice = WrapKeySlice(meta.LastKey());
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, comparator_(max_key_slice, literal_slice));
        return cmp > 0;
    });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitGreaterOrEqual(
    const Literal& literal) {
    // file.maxKey >= literal
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const BTreeIndexMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        MemorySlice max_key_slice = WrapKeySlice(meta.LastKey());
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, comparator_(max_key_slice, literal_slice));
        return cmp >= 0;
    });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitIn(
    const std::vector<Literal>& literals) {
    std::vector<MemorySlice> literal_slices;
    literal_slices.reserve(literals.size());
    for (const auto& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(MemorySlice slice, SerializeLiteral(literal));
        literal_slices.push_back(std::move(slice));
    }
    return Filter([this, &literal_slices](const BTreeIndexMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        MemorySlice min_key_slice = WrapKeySlice(meta.FirstKey());
        MemorySlice max_key_slice = WrapKeySlice(meta.LastKey());
        for (const auto& literal_slice : literal_slices) {
            PAIMON_ASSIGN_OR_RAISE(int32_t cmp_min, comparator_(literal_slice, min_key_slice));
            PAIMON_ASSIGN_OR_RAISE(int32_t cmp_max, comparator_(literal_slice, max_key_slice));
            if (cmp_min >= 0 && cmp_max <= 0) {
                return true;
            }
        }
        return false;
    });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitNotIn(
    const std::vector<Literal>& literals) {
    // Cannot filter any file by NOT IN condition
    return Filter([](const BTreeIndexMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitStartsWith(
    const Literal& prefix) {
    return Filter([](const BTreeIndexMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitEndsWith(const Literal& suffix) {
    return Filter([](const BTreeIndexMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitContains(
    const Literal& literal) {
    return Filter([](const BTreeIndexMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::VisitLike(const Literal& literal) {
    return Filter([](const BTreeIndexMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> BTreeFileMetaSelector::Filter(
    const MetaPredicate& predicate) const {
    std::vector<GlobalIndexIOMeta> result;
    for (const auto& [io_meta, index_meta] : files_) {
        PAIMON_ASSIGN_OR_RAISE(bool matched, predicate(*index_meta));
        if (matched) {
            result.push_back(io_meta);
        }
    }
    return result;
}

MemorySlice BTreeFileMetaSelector::WrapKeySlice(const std::shared_ptr<Bytes>& key) {
    return MemorySlice::Wrap(MemorySegment::WrapView(key->data(), key->size()));
}

Result<MemorySlice> BTreeFileMetaSelector::SerializeLiteral(const Literal& literal) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes,
                           KeySerializer::SerializeKey(literal, key_type_, pool_.get()));
    return MemorySlice::Wrap(bytes);
}

}  // namespace paimon
