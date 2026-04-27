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

#include "paimon/global_index/global_indexer_factory.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/global_index/bitmap/bitmap_global_index.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(GlobalIndexerFactoryTest, TestSimple) {
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexer> indexer,
                         GlobalIndexerFactory::Get("bitmap", options));

    auto bitmap_global_index = dynamic_cast<BitmapGlobalIndex*>(indexer.get());
    ASSERT_TRUE(bitmap_global_index);
}

TEST(GlobalIndexerFactoryTest, TestNonExist) {
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexer> indexer,
                         GlobalIndexerFactory::Get("nonexist", options));
    ASSERT_FALSE(indexer);
}

TEST(GlobalIndexerFactoryTest, TestLuminaVectorAnnCompatibility) {
    // "lumina-vector-ann" should be treated as "lumina" for backward compatibility.
    // Both identifiers should produce the same result (either both succeed or both return nullptr
    // depending on whether the lumina module is linked).
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexer> lumina_vector_ann_indexer,
                         GlobalIndexerFactory::Get("lumina-vector-ann", options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexer> lumina_indexer,
                         GlobalIndexerFactory::Get("lumina", options));

    ASSERT_EQ(static_cast<bool>(lumina_vector_ann_indexer), static_cast<bool>(lumina_indexer));
}
}  // namespace paimon::test
