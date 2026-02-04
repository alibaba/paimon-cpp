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

#include "paimon/common/file_index/empty/empty_file_index_reader.h"

#include <cstdint>

#include "gtest/gtest.h"
#include "paimon/predicate/literal.h"

namespace paimon::test {
TEST(EmptyFileIndexReaderTest, TestSimple) {
    Literal lit0(static_cast<int32_t>(0));
    EmptyFileIndexReader reader;

    ASSERT_FALSE(reader.VisitIsNotNull().value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitEqual(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitStartsWith(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitEndsWith(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitContains(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitLike(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitLessThan(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitGreaterOrEqual(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitLessOrEqual(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitGreaterThan(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitIn({lit0}).value()->IsRemain().value());

    ASSERT_TRUE(reader.VisitIsNull().value()->IsRemain().value());
    ASSERT_TRUE(reader.VisitNotEqual(lit0).value()->IsRemain().value());
    ASSERT_TRUE(reader.VisitNotIn({lit0}).value()->IsRemain().value());
}
}  // namespace paimon::test
