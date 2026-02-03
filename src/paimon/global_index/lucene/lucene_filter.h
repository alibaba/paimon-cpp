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
#include "lucene++/LuceneHeaders.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::lucene {
class BitmapDocIdSetIterator : public Lucene::DocIdSetIterator {
 public:
    explicit BitmapDocIdSetIterator(const RoaringBitmap64* ids)
        : Lucene::DocIdSetIterator(), ids_(ids), iter_(ids->Begin()) {}

    int32_t advance(int32_t target) override {
        iter_.EqualOrLarger(static_cast<int64_t>(target));
        if (iter_ == ids_->End()) {
            return Lucene::DocIdSetIterator::NO_MORE_DOCS;
        }
        return static_cast<int32_t>(*iter_);
    }

    int32_t docID() override {
        if (iter_ == ids_->End()) {
            return Lucene::DocIdSetIterator::NO_MORE_DOCS;
        }
        return static_cast<int32_t>(*iter_);
    }

    int32_t nextDoc() override {
        if (iter_ == ids_->End()) {
            return Lucene::DocIdSetIterator::NO_MORE_DOCS;
        }
        auto id = static_cast<int32_t>(*iter_);
        ++iter_;
        return id;
    }

 private:
    const RoaringBitmap64* ids_;
    RoaringBitmap64::Iterator iter_;
};

class BitmapDocIdSet : public Lucene::DocIdSet {
 public:
    explicit BitmapDocIdSet(const RoaringBitmap64* ids) : DocIdSet(), ids_(ids) {}

    Lucene::DocIdSetIteratorPtr iterator() override {
        return Lucene::newLucene<BitmapDocIdSetIterator>(ids_);
    }

    bool isCacheable() override {
        return true;
    }

 private:
    const RoaringBitmap64* ids_;
};

class LuceneFilter : public Lucene::Filter {
 public:
    explicit LuceneFilter(const RoaringBitmap64* ids) : ids_(ids) {}

    Lucene::DocIdSetPtr getDocIdSet(const Lucene::IndexReaderPtr& reader) override {
        return Lucene::newLucene<BitmapDocIdSet>(ids_);
    }

 private:
    const RoaringBitmap64* ids_;
};

}  // namespace paimon::lucene
