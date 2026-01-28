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

#include "paimon/common/utils/crc32c.h"

#include <cstring>
#include <limits>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "arrow/util/crc32.h"
#include "gtest/gtest.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {

TEST(CRC32CTest, TestSimple) {
    char a = 'a';
    ASSERT_EQ(CRC32C::calculate(&a, 1), 3904355907);

    std::string data = "hello paimon c++";
    ASSERT_EQ(CRC32C::calculate(data.c_str(), data.size()), 1311805437);
}

}  // namespace paimon::test
