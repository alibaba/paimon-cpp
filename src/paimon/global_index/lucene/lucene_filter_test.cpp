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
#include "paimon/global_index/lucene/lucene_filter.h"

#include "gtest/gtest.h"
#include "lucene++/LuceneHeaders.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::lucene::test {
TEST(LuceneFilterTest, TestSimple) {
    RoaringBitmap64 roaring = RoaringBitmap64::From({1l, 3l, 5l, 100l});
    LuceneFilter filter(&roaring);

    auto doc_id_set = filter.getDocIdSet(/*reader=*/Lucene::IndexReaderPtr());
    ASSERT_TRUE(doc_id_set);

    auto doc_iter = doc_id_set->iterator();
    ASSERT_TRUE(doc_iter);
    ASSERT_TRUE(doc_id_set->isCacheable());

    ASSERT_EQ(1, doc_iter->nextDoc());

    ASSERT_EQ(5, doc_iter->advance(4));
    ASSERT_EQ(5, doc_iter->docID());

    ASSERT_EQ(100, doc_iter->advance(100));

    ASSERT_EQ(Lucene::DocIdSetIterator::NO_MORE_DOCS, doc_iter->advance(1000));
    ASSERT_EQ(Lucene::DocIdSetIterator::NO_MORE_DOCS, doc_iter->docID());
    ASSERT_EQ(Lucene::DocIdSetIterator::NO_MORE_DOCS, doc_iter->nextDoc());
}

}  // namespace paimon::lucene::test
