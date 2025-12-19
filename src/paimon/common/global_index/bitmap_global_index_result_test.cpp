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
#include "paimon/global_index/bitmap_global_index_result.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"
namespace paimon::test {
class BitmapGlobalIndexResultTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class FakeGlobalIndexResult : public GlobalIndexResult {
     public:
        explicit FakeGlobalIndexResult(const std::vector<int64_t>& values) : values_(values) {}
        class Iterator : public GlobalIndexResult::Iterator {
         public:
            Iterator(const std::vector<int64_t>* values,
                     const std::vector<int64_t>::const_iterator& iter)
                : values_(values), iter_(iter) {}
            bool HasNext() const override {
                return iter_ != values_->end();
            }
            int64_t Next() override {
                int64_t value = *iter_;
                iter_++;
                return value;
            }
            const std::vector<int64_t>* values_;
            std::vector<int64_t>::const_iterator iter_;
        };

        Result<std::unique_ptr<GlobalIndexResult::Iterator>> CreateIterator() const override {
            auto iter = values_.begin();
            return std::make_unique<Iterator>(&values_, iter);
        }

        std::string ToString() const override {
            return "fake";
        }

        Result<bool> IsEmpty() const override {
            return values_.empty();
        }

     private:
        std::vector<int64_t> values_;
    };
};
TEST_F(BitmapGlobalIndexResultTest, TestIterator) {
    auto check_iterator = [](const std::vector<int64_t>& expected_ids) {
        auto bitmap_supplier = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(expected_ids);
        };
        auto index_result = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier);
        if (expected_ids.empty()) {
            ASSERT_TRUE(index_result->IsEmpty().value());
        }

        ASSERT_OK_AND_ASSIGN(auto iter, index_result->CreateIterator());
        std::vector<int64_t> result_ids;
        while (iter->HasNext()) {
            result_ids.push_back(iter->Next());
        }
        ASSERT_EQ(result_ids, expected_ids);
    };

    check_iterator({});
    check_iterator({1, 4, 7, RoaringBitmap32::MAX_VALUE, RoaringBitmap64::MAX_VALUE});
    check_iterator({100, 101, 102, 103});
}

TEST_F(BitmapGlobalIndexResultTest, TestAnd) {
    auto check_and_result = [](const std::vector<int64_t>& left, const std::vector<int64_t>& right,
                               const std::string& expected_str) {
        auto bitmap_supplier1 = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(left);
        };
        auto index_result1 = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier1);
        auto bitmap_supplier2 = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(right);
        };
        auto index_result2 = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier2);

        ASSERT_OK_AND_ASSIGN(auto result, index_result1->And(index_result2));
        ASSERT_EQ(result->ToString(), expected_str);
    };
    check_and_result({1, 2, 3}, {1, 2, 7}, "{1,2}");
    check_and_result({1, 2, 3}, {1, 2, 3}, "{1,2,3}");
    check_and_result({1, 2, 3}, {100, 200, 300}, "{}");
    check_and_result({1, 2, 3}, {}, "{}");
    check_and_result({}, {}, "{}");
    check_and_result({1, 2, 3, RoaringBitmap64::MAX_VALUE}, {1, RoaringBitmap64::MAX_VALUE},
                     "{1,9223372036854775807}");
}

TEST_F(BitmapGlobalIndexResultTest, TestBitmapResultAndOtherResult) {
    auto bitmap_supplier1 = [&]() -> Result<RoaringBitmap64> {
        return RoaringBitmap64::From({1, 2, 3});
    };
    auto index_result1 = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier1);

    auto fake_result = std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({1l, 2l, 7l}));

    ASSERT_OK_AND_ASSIGN(auto result, index_result1->And(fake_result));
    ASSERT_EQ(result->ToString(), "{1,2}");
}

TEST_F(BitmapGlobalIndexResultTest, TestOr) {
    auto check_and_result = [](const std::vector<int64_t>& left, const std::vector<int64_t>& right,
                               const std::string& expected_str) {
        auto bitmap_supplier1 = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(left);
        };
        auto index_result1 = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier1);
        auto bitmap_supplier2 = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(right);
        };
        auto index_result2 = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier2);

        ASSERT_OK_AND_ASSIGN(auto result, index_result1->Or(index_result2));
        ASSERT_EQ(result->ToString(), expected_str);
    };
    check_and_result({1, 2, 3}, {1, 2, 7}, "{1,2,3,7}");
    check_and_result({1, 2, 3}, {1, 2, 3}, "{1,2,3}");
    check_and_result({1, 2, 3}, {100, 200, 300}, "{1,2,3,100,200,300}");
    check_and_result({1, 2, 3}, {}, "{1,2,3}");
    check_and_result({}, {}, "{}");
    check_and_result({1, 2, 3, RoaringBitmap64::MAX_VALUE}, {1, RoaringBitmap64::MAX_VALUE},
                     "{1,2,3,9223372036854775807}");
}

TEST_F(BitmapGlobalIndexResultTest, TestBitmapResultOrOtherResult) {
    auto bitmap_supplier1 = [&]() -> Result<RoaringBitmap64> {
        return RoaringBitmap64::From({1, 2, 3});
    };
    auto index_result1 = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier1);

    auto fake_result = std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({1l, 2l, 7l}));

    ASSERT_OK_AND_ASSIGN(auto result, index_result1->Or(fake_result));
    ASSERT_EQ(result->ToString(), "{1,2,3,7}");
}

TEST_F(BitmapGlobalIndexResultTest, TestInvalidBitmapResult) {
    auto bitmap_supplier = [&]() -> Result<RoaringBitmap64> {
        return Status::Invalid("invalid supplier");
    };
    auto result = std::make_shared<BitmapGlobalIndexResult>(bitmap_supplier);
    ASSERT_TRUE(result->ToString().find("Invalid: invalid supplier") != std::string::npos);
}

TEST_F(BitmapGlobalIndexResultTest, TestFromRanges) {
    {
        auto result = BitmapGlobalIndexResult::FromRanges({Range(0, 5)});
        ASSERT_EQ(result->ToString(), "{0,1,2,3,4,5}");
    }
    {
        auto result = BitmapGlobalIndexResult::FromRanges({Range(10, 10)});
        ASSERT_EQ(result->ToString(), "{10}");
    }
    {
        auto result = BitmapGlobalIndexResult::FromRanges({Range(0, 5), Range(10, 10)});
        ASSERT_EQ(result->ToString(), "{0,1,2,3,4,5,10}");
    }
}
}  // namespace paimon::test
