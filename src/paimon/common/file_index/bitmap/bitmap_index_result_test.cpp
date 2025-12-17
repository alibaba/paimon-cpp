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

#include "paimon/file_index/bitmap_index_result.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class BitmapIndexResultTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    std::shared_ptr<BitmapIndexResult> CreateResult(const std::vector<int32_t>& values) const {
        return std::make_shared<BitmapIndexResult>(
            [=]() -> Result<RoaringBitmap32> { return RoaringBitmap32::From(values); });
    }

    void CheckResult(const std::shared_ptr<FileIndexResult>& result,
                     const std::vector<int32_t>& expected) const {
        auto typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_EQ(*(typed_result->GetBitmap().value()), RoaringBitmap32::From(expected));
    }
};

TEST_F(BitmapIndexResultTest, TestSimple) {
    auto res1 = CreateResult({10, 100});
    auto res2 = CreateResult({10, 20, 200});
    auto res3 = CreateResult({});

    ASSERT_EQ(res1->ToString(), "{10,100}");
    ASSERT_EQ(res3->ToString(), "{}");

    ASSERT_TRUE(res1->IsRemain().value());
    ASSERT_FALSE(res3->IsRemain().value());
    CheckResult(res1->And(res2).value(), {10});
    CheckResult(res1->Or(res2).value(), {10, 20, 100, 200});

    CheckResult(res1->And(res3).value(), {});
    CheckResult(res1->Or(res3).value(), {10, 100});
}

TEST_F(BitmapIndexResultTest, TestCapture) {
    auto res1 = CreateResult({10, 100});
    auto res2 = CreateResult({10, 20, 200});

    ASSERT_OK_AND_ASSIGN(auto res, res1->And(res2));
    res1.reset();
    res2.reset();
    CheckResult(res, {10});
}

TEST_F(BitmapIndexResultTest, TestCompoundIndexResult) {
    auto res1 = CreateResult({1, 3, 5});
    auto res2 = CreateResult({});
    ASSERT_TRUE(FileIndexResult::Remain()->IsRemain().value());
    ASSERT_FALSE(FileIndexResult::Skip()->IsRemain().value());

    ASSERT_TRUE(res1->IsRemain().value());
    ASSERT_FALSE(res2->IsRemain().value());

    ASSERT_FALSE(res1->And(FileIndexResult::Skip()).value()->IsRemain().value());
    ASSERT_TRUE(res1->And(FileIndexResult::Remain()).value()->IsRemain().value());
    ASSERT_TRUE(res1->Or(FileIndexResult::Skip()).value()->IsRemain().value());
    ASSERT_TRUE(res1->Or(FileIndexResult::Remain()).value()->IsRemain().value());

    ASSERT_FALSE(res2->And(FileIndexResult::Skip()).value()->IsRemain().value());
    ASSERT_FALSE(res2->And(FileIndexResult::Remain()).value()->IsRemain().value());
    ASSERT_FALSE(res2->Or(FileIndexResult::Skip()).value()->IsRemain().value());
    ASSERT_TRUE(res2->Or(FileIndexResult::Remain()).value()->IsRemain().value());
}

}  // namespace paimon::test
