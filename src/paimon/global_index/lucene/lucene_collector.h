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
class LuceneCollector : public Lucene::Collector {
 public:
    LuceneCollector() : Lucene::Collector() {}
    void setScorer(const Lucene::ScorerPtr& scorer) override {
        // ignore scorer
    }
    void collect(int32_t doc) override {
        bitmap_.Add(doc_base_ + doc);
    }
    void setNextReader(const Lucene::IndexReaderPtr& reader, int32_t doc_base) override {
        doc_base_ = doc_base;
    }
    bool acceptsDocsOutOfOrder() override {
        return true;
    }
    const RoaringBitmap64& GetBitmap() const {
        return bitmap_;
    }

 private:
    RoaringBitmap64 bitmap_;
    int64_t doc_base_ = 0;
};
}  // namespace paimon::lucene
