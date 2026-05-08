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

#include "paimon/common/global_index/union_global_index_reader.h"

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/executor.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::test {
class FakeReader : public GlobalIndexReader {
 public:
    /// Sets the result returned by all Visit* methods (default behavior).
    /// Pass an empty vector for an empty bitmap.
    void SetDefaultResult(const std::vector<int64_t>& row_ids) {
        default_result_ = row_ids;
        return_nullptr_ = false;
        return_error_ = false;
    }

    /// Configures this reader to return nullptr for all Visit* methods.
    void SetReturnNullptr() {
        return_nullptr_ = true;
        return_error_ = false;
    }

    /// Configures this reader to return an error Status for all Visit* methods.
    void SetReturnError(const std::string& message) {
        return_error_ = true;
        return_nullptr_ = false;
        error_message_ = message;
    }

    /// Sets a scored result returned by VisitVectorSearch.
    void SetScoredResult(const std::vector<int64_t>& row_ids, const std::vector<float>& scores) {
        scored_row_ids_ = row_ids;
        scored_scores_ = scores;
        has_scored_result_ = true;
    }

    /// Counts how many times any Visit* method was invoked. Useful to assert all readers
    /// are exercised by UnionGlobalIndexReader.
    int InvocationCount() const {
        return invocation_count_.load();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        invocation_count_++;
        if (return_error_) {
            return Status::Invalid(error_message_);
        }
        if (!has_scored_result_) {
            return std::shared_ptr<ScoredGlobalIndexResult>(nullptr);
        }
        auto bitmap = RoaringBitmap64::From(scored_row_ids_);
        auto scores = scored_scores_;
        return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap),
                                                               std::move(scores));
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override {
        return MakeResult();
    }

    bool IsThreadSafe() const override {
        return true;
    }

    std::string GetIndexType() const override {
        return "fake";
    }

 private:
    Result<std::shared_ptr<GlobalIndexResult>> MakeResult() {
        invocation_count_++;
        if (return_error_) {
            return Status::Invalid(error_message_);
        }
        if (return_nullptr_) {
            return std::shared_ptr<GlobalIndexResult>(nullptr);
        }
        auto ids = default_result_;
        return std::make_shared<BitmapGlobalIndexResult>(
            [ids]() { return RoaringBitmap64::From(ids); });
    }

 private:
    std::vector<int64_t> default_result_;
    bool return_nullptr_ = false;
    bool return_error_ = false;
    std::string error_message_;
    std::vector<int64_t> scored_row_ids_;
    std::vector<float> scored_scores_;
    bool has_scored_result_ = false;
    std::atomic<int32_t> invocation_count_{0};
};

class UnionGlobalIndexReaderTest : public ::testing::Test {
 public:
    static void CheckResult(const std::shared_ptr<GlobalIndexResult>& result,
                            const std::vector<int64_t>& expected) {
        ASSERT_TRUE(result);
        auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected).ToString();
    }

    static void CheckScoredResult(const std::shared_ptr<ScoredGlobalIndexResult>& result,
                                  const std::vector<int64_t>& expected_row_ids,
                                  const std::vector<float>& expected_scores) {
        ASSERT_TRUE(result);
        auto typed_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected_row_ids))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected_row_ids).ToString();
        ASSERT_EQ(typed_result->GetScores(), expected_scores);
    }
};

TEST_F(UnionGlobalIndexReaderTest, TestSingleReaderUnion) {
    auto reader = std::make_shared<FakeReader>();
    reader->SetDefaultResult({1, 2, 3});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitIsNotNull());
    CheckResult(result, {1, 2, 3});
    ASSERT_EQ(reader->InvocationCount(), 1);
}

TEST_F(UnionGlobalIndexReaderTest, TestMultipleReadersUnionSequential) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    auto reader3 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1, 2});
    reader2->SetDefaultResult({3, 4});
    reader3->SetDefaultResult({5});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2, reader3};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitIsNotNull());
    // {1,2} OR {3,4} OR {5} -> {1,2,3,4,5}
    CheckResult(result, {1, 2, 3, 4, 5});
    ASSERT_EQ(reader1->InvocationCount(), 1);
    ASSERT_EQ(reader2->InvocationCount(), 1);
    ASSERT_EQ(reader3->InvocationCount(), 1);
}

TEST_F(UnionGlobalIndexReaderTest, TestMultipleReadersUnionOverlappingIds) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1, 2, 3});
    reader2->SetDefaultResult({2, 3, 4});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitIsNotNull());
    // {1,2,3} OR {2,3,4} -> {1,2,3,4}
    CheckResult(result, {1, 2, 3, 4});
}

TEST_F(UnionGlobalIndexReaderTest, TestMultipleReadersUnionWithExecutor) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    auto reader3 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({10});
    reader2->SetDefaultResult({20});
    reader3->SetDefaultResult({30});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2, reader3};
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    UnionGlobalIndexReader union_reader(std::move(readers), executor);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitIsNotNull());
    CheckResult(result, {10, 20, 30});
    ASSERT_EQ(reader1->InvocationCount(), 1);
    ASSERT_EQ(reader2->InvocationCount(), 1);
    ASSERT_EQ(reader3->InvocationCount(), 1);
}

TEST_F(UnionGlobalIndexReaderTest, TestEmptyReaderList) {
    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    // No readers means no results to merge -> nullptr
    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitIsNotNull());
    ASSERT_FALSE(result);
}

TEST_F(UnionGlobalIndexReaderTest, TestAllReadersReturnNullptr) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetReturnNullptr();
    reader2->SetReturnNullptr();

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitIsNotNull());
    // All readers return nullptr -> merged result is nullptr
    ASSERT_FALSE(result);
}

TEST_F(UnionGlobalIndexReaderTest, TestPartialReadersReturnNullptr) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    auto reader3 = std::make_shared<FakeReader>();
    reader1->SetReturnNullptr();
    reader2->SetDefaultResult({1, 2});
    reader3->SetReturnNullptr();

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2, reader3};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitIsNotNull());
    // Nullptrs are skipped, only reader2's result is used
    CheckResult(result, {1, 2});
}

TEST_F(UnionGlobalIndexReaderTest, TestErrorPropagationSequential) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1, 2});
    reader2->SetReturnError("Unknown error for reader2");

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_NOK_WITH_MSG(union_reader.VisitIsNotNull(), "Unknown error for reader2");
}

TEST_F(UnionGlobalIndexReaderTest, TestErrorPropagationWithExecutor) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1});
    reader2->SetReturnError("Unknown error for reader2");

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    UnionGlobalIndexReader union_reader(std::move(readers), executor);

    ASSERT_NOK_WITH_MSG(union_reader.VisitIsNotNull(), "Unknown error for reader2");
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitEqualUnion) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1});
    reader2->SetDefaultResult({2});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    Literal literal_42(42);
    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitEqual(literal_42));
    CheckResult(result, {1, 2});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitNotEqualUnion) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1});
    reader2->SetDefaultResult({2});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    Literal literal_42(42);
    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitNotEqual(literal_42));
    CheckResult(result, {1, 2});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitRangeQueriesUnion) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1});
    reader2->SetDefaultResult({2});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto lt, union_reader.VisitLessThan(literal_5));
    CheckResult(lt, {1, 2});
    ASSERT_OK_AND_ASSIGN(auto le, union_reader.VisitLessOrEqual(literal_5));
    CheckResult(le, {1, 2});
    ASSERT_OK_AND_ASSIGN(auto gt, union_reader.VisitGreaterThan(literal_5));
    CheckResult(gt, {1, 2});
    ASSERT_OK_AND_ASSIGN(auto ge, union_reader.VisitGreaterOrEqual(literal_5));
    CheckResult(ge, {1, 2});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitInUnion) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1, 3});
    reader2->SetDefaultResult({2, 4});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    std::vector<Literal> literals = {Literal(10), Literal(20)};
    ASSERT_OK_AND_ASSIGN(auto in_result, union_reader.VisitIn(literals));
    CheckResult(in_result, {1, 2, 3, 4});

    ASSERT_OK_AND_ASSIGN(auto not_in_result, union_reader.VisitNotIn(literals));
    CheckResult(not_in_result, {1, 2, 3, 4});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitStringQueriesUnion) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1});
    reader2->SetDefaultResult({2});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    Literal literal_str(FieldType::STRING, "abc", 3);
    ASSERT_OK_AND_ASSIGN(auto starts, union_reader.VisitStartsWith(literal_str));
    CheckResult(starts, {1, 2});
    ASSERT_OK_AND_ASSIGN(auto ends, union_reader.VisitEndsWith(literal_str));
    CheckResult(ends, {1, 2});
    ASSERT_OK_AND_ASSIGN(auto contains, union_reader.VisitContains(literal_str));
    CheckResult(contains, {1, 2});
    ASSERT_OK_AND_ASSIGN(auto like, union_reader.VisitLike(literal_str));
    CheckResult(like, {1, 2});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitIsNullUnion) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({100, 200});
    reader2->SetDefaultResult({300});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitIsNull());
    CheckResult(result, {100, 200, 300});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitFullTextSearchUnion) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetDefaultResult({1, 5});
    reader2->SetDefaultResult({2, 6});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitFullTextSearch(nullptr));
    CheckResult(result, {1, 2, 5, 6});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitVectorSearchAllNullptr) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    // Neither reader has SetScoredResult -> VisitVectorSearch returns nullptr
    reader1->SetDefaultResult({1});
    reader2->SetDefaultResult({2});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitVectorSearch(nullptr));
    ASSERT_FALSE(result);
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitVectorSearchSingleReader) {
    auto reader = std::make_shared<FakeReader>();
    reader->SetScoredResult({1, 3, 5}, {0.9f, 0.7f, 0.5f});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitVectorSearch(nullptr));
    CheckScoredResult(result, {1, 3, 5}, {0.9f, 0.7f, 0.5f});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitVectorSearchMultipleReadersUnion) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetScoredResult({1, 3}, {0.9f, 0.7f});
    reader2->SetScoredResult({2, 4}, {0.8f, 0.6f});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitVectorSearch(nullptr));
    // {1,3} OR {2,4} -> {1,2,3,4}, scores merged in row id order
    CheckScoredResult(result, {1, 2, 3, 4}, {0.9f, 0.8f, 0.7f, 0.6f});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitVectorSearchPartialNullptr) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    auto reader3 = std::make_shared<FakeReader>();
    reader1->SetScoredResult({1, 2}, {0.9f, 0.8f});
    // reader2 has no scored result -> returns nullptr
    reader2->SetDefaultResult({10});
    reader3->SetScoredResult({5, 6}, {0.5f, 0.4f});

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2, reader3};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_OK_AND_ASSIGN(auto result, union_reader.VisitVectorSearch(nullptr));
    // reader2 nullptr is skipped, {1,2} OR {5,6} -> {1,2,5,6}
    CheckScoredResult(result, {1, 2, 5, 6}, {0.9f, 0.8f, 0.5f, 0.4f});
}

TEST_F(UnionGlobalIndexReaderTest, TestVisitVectorSearchErrorPropagation) {
    auto reader1 = std::make_shared<FakeReader>();
    auto reader2 = std::make_shared<FakeReader>();
    reader1->SetScoredResult({1}, {0.9f});
    reader2->SetReturnError("vector search failure");

    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader1, reader2};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_NOK_WITH_MSG(union_reader.VisitVectorSearch(nullptr), "vector search failure");
}

TEST_F(UnionGlobalIndexReaderTest, TestIsThreadSafeAlwaysFalse) {
    auto reader = std::make_shared<FakeReader>();
    std::vector<std::shared_ptr<GlobalIndexReader>> readers = {reader};
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    // UnionGlobalIndexReader is not thread-safe regardless of inner readers
    ASSERT_FALSE(union_reader.IsThreadSafe());
}

TEST_F(UnionGlobalIndexReaderTest, TestGetIndexTypeReturnsUnion) {
    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    UnionGlobalIndexReader union_reader(std::move(readers), nullptr);

    ASSERT_EQ(union_reader.GetIndexType(), "union");
}

}  // namespace paimon::test
