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

// Adapted from Apache Arrow:
// https://github.com/apache/arrow/blob/apache-arrow-17.0.0/cpp/src/arrow/status_test.cc

#include "paimon/status.h"

#include <cstdint>
#include <iosfwd>
#include <sstream>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class TestStatusDetail : public StatusDetail {
 public:
    const char* type_id() const override {
        return "type_id";
    }
    std::string ToString() const override {
        return "a specific detail message";
    }
};

}  // namespace

TEST(StatusTest, TestCodeAndMessage) {
    Status ok = Status::OK();
    ASSERT_EQ(StatusCode::OK, ok.code());
    Status file_error = Status::IOError("file error");
    ASSERT_EQ(StatusCode::IOError, file_error.code());
    ASSERT_EQ("file error", file_error.message());
}

TEST(StatusTest, TestToString) {
    Status file_error = Status::IOError("file error");
    ASSERT_EQ("IOError: file error", file_error.ToString());

    std::stringstream ss;
    ss << file_error;
    ASSERT_EQ(file_error.ToString(), ss.str());
}

TEST(StatusTest, TestToStringWithDetail) {
    Status status(StatusCode::IOError, "summary", std::make_shared<TestStatusDetail>());
    ASSERT_EQ("IOError: summary. Detail: a specific detail message", status.ToString());

    std::stringstream ss;
    ss << status;
    ASSERT_EQ(status.ToString(), ss.str());
}

TEST(StatusTest, TestCodeAsString) {
    ASSERT_EQ("OK", Status::CodeAsString(StatusCode::OK));
    ASSERT_EQ("Out of memory", Status::CodeAsString(StatusCode::OutOfMemory));
    ASSERT_EQ("Key error", Status::CodeAsString(StatusCode::KeyError));
    ASSERT_EQ("Type error", Status::CodeAsString(StatusCode::TypeError));
    ASSERT_EQ("Invalid", Status::CodeAsString(StatusCode::Invalid));
    ASSERT_EQ("IOError", Status::CodeAsString(StatusCode::IOError));
    ASSERT_EQ("Capacity error", Status::CodeAsString(StatusCode::CapacityError));
    ASSERT_EQ("Index error", Status::CodeAsString(StatusCode::IndexError));
    ASSERT_EQ("Cancelled", Status::CodeAsString(StatusCode::Cancelled));
    ASSERT_EQ("Unknown error", Status::CodeAsString(StatusCode::UnknownError));
    ASSERT_EQ("NotImplemented", Status::CodeAsString(StatusCode::NotImplemented));
    ASSERT_EQ("Serialization error", Status::CodeAsString(StatusCode::SerializationError));
    ASSERT_EQ("Not exist", Status::CodeAsString(StatusCode::NotExist));
    ASSERT_EQ("Exist", Status::CodeAsString(StatusCode::Exist));

    // An out-of-range code falls into the default branch. The cast is intentional to
    // exercise the defensive default, so the enum-range analyzer check is suppressed.
    // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
    ASSERT_EQ("Unknown", Status::CodeAsString(static_cast<StatusCode>(99)));

    // The instance overload returns "OK" for a success status.
    ASSERT_EQ("OK", Status::OK().CodeAsString());
}

TEST(StatusDeathTest, TestAbort) {
    // Death tests fork(); with the default "fast" style, forking in a multi-threaded
    // process is unsafe and under ThreadSanitizer the child aborts with a sanitizer
    // message before Abort() runs. The "threadsafe" style re-execs the test binary in
    // a clean process so Abort()'s own output is produced and can be matched.
    const std::string prev_style = testing::GTEST_FLAG(death_test_style);
    testing::GTEST_FLAG(death_test_style) = "threadsafe";
    ASSERT_DEATH(Status::IOError("boom").Abort(), "Paimon Fatal Error");
    ASSERT_DEATH(Status::IOError("boom").Abort("custom prefix"), "custom prefix");
    testing::GTEST_FLAG(death_test_style) = prev_style;
}

TEST(StatusTest, TestWithDetail) {
    Status status(StatusCode::IOError, "summary");
    auto detail = std::make_shared<TestStatusDetail>();
    Status new_status = status.WithDetail(detail);

    ASSERT_EQ(new_status.code(), status.code());
    ASSERT_EQ(new_status.message(), status.message());
    ASSERT_EQ(new_status.detail(), detail);
}

TEST(StatusTest, AndStatus) {
    Status a = Status::OK();
    Status b = Status::OK();
    Status c = Status::Invalid("invalid value");
    Status d = Status::IOError("file error");

    Status res;
    res = a & b;
    ASSERT_OK(res);
    res = a & c;
    ASSERT_TRUE(res.IsInvalid());
    res = d & c;
    ASSERT_TRUE(res.IsIOError());

    res = Status::OK();
    res &= c;
    ASSERT_TRUE(res.IsInvalid());
    res &= d;
    ASSERT_TRUE(res.IsInvalid());

    // With rvalues
    res = Status::OK() & Status::Invalid("foo");
    ASSERT_TRUE(res.IsInvalid());
    res = Status::Invalid("foo") & Status::OK();
    ASSERT_TRUE(res.IsInvalid());
    res = Status::Invalid("foo") & Status::IOError("bar");
    ASSERT_TRUE(res.IsInvalid());

    res = Status::OK();
    res &= Status::OK();
    ASSERT_OK(res);
    res &= Status::Invalid("foo");
    ASSERT_TRUE(res.IsInvalid());
    res &= Status::IOError("bar");
    ASSERT_TRUE(res.IsInvalid());
}

TEST(StatusTest, TestEquality) {
    ASSERT_EQ(Status(), Status::OK());
    ASSERT_EQ(Status::Invalid("error"), Status::Invalid("error"));

    ASSERT_NE(Status::Invalid("error"), Status::OK());
    ASSERT_NE(Status::Invalid("error"), Status::Invalid("other error"));
}

TEST(StatusTest, TestDetailEquality) {
    const auto status_with_detail =
        paimon::Status(StatusCode::IOError, "", std::make_shared<TestStatusDetail>());
    const auto status_with_detail2 =
        paimon::Status(StatusCode::IOError, "", std::make_shared<TestStatusDetail>());
    const auto status_without_detail = paimon::Status::IOError("");

    ASSERT_EQ(*status_with_detail.detail(), *status_with_detail2.detail());
    ASSERT_EQ(status_with_detail, status_with_detail2);
    ASSERT_NE(status_with_detail, status_without_detail);
    ASSERT_NE(status_without_detail, status_with_detail);
}

TEST(StatusTest, TestStatusDefine) {
    Status s;
    ASSERT_OK(s);

    auto func1 = []() {
        PAIMON_RETURN_NOT_OK(Status::OutOfMemory("out of memory"));
        return Status::OK();
    };
    ASSERT_NOK(func1());

    auto func2 = [](bool is_ok) {
        PAIMON_RETURN_IF(!is_ok, Status::Invalid("invalid args"));
        return Status::OK();
    };
    ASSERT_OK(func2(true));
    ASSERT_NOK(func2(false));

    bool set_value = false;
    auto func3 = [&]() { set_value = true; };
    auto func4 = [&]() {
        PAIMON_RETURN_NOT_OK_ELSE(Status::OutOfMemory("out of memory"), func3());
        return Status::OK();
    };
    ASSERT_NOK(func4());
    ASSERT_TRUE(set_value);
}

TEST(StatusTest, TestResultDefine) {
    std::unique_ptr<int> value;
    auto func_ok = []() -> Result<std::unique_ptr<int>> { return std::make_unique<int>(233); };
    auto func_not_ok = []() -> Result<std::unique_ptr<int>> {
        return Status::OutOfMemory("out of memory");
    };
    auto test_func = [&](const auto& func) {
        PAIMON_ASSIGN_OR_RAISE(value, func());
        return Status::OK();
    };
    ASSERT_NOK(test_func(func_not_ok));
    ASSERT_FALSE(value);

    ASSERT_OK(test_func(func_ok));
    ASSERT_TRUE(value);
    ASSERT_EQ(233, *value);
}

TEST(StatusTest, TestArrowStatusDefine) {
    auto arrow_status_func = [](bool is_ok) -> arrow::Status {
        if (!is_ok) {
            return arrow::Status::Invalid("invalid");
        }
        return arrow::Status::OK();
    };
    auto func = [&](bool is_ok) -> Status {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow_status_func(is_ok));
        return Status::OK();
    };
    ASSERT_OK(func(true));
    ASSERT_NOK(func(false));
}

TEST(StatusTest, TestResultSimple) {
    {
        Result<int32_t> result(233);
        ASSERT_OK(result);
        ASSERT_EQ(233, result.value());
        ASSERT_EQ(233, result.value_or(-1));
    }
    {
        Result<std::string> result("abcd");
        ASSERT_OK(result);
        ASSERT_EQ("abcd", result.value());
        ASSERT_EQ("abcd", result.value_or("aaaa"));
    }
    {
        Result<std::string> result = Status::Invalid("invalid args");
        ASSERT_NOK(result);
        ASSERT_TRUE(result.status().IsInvalid());
        ASSERT_NOK_WITH_MSG(result, "invalid args");
        ASSERT_EQ("aaaa", result.value_or("aaaa"));
    }
    {
        auto CreateResult = []() -> Result<std::unique_ptr<int32_t>> {
            return std::make_unique<int32_t>(233);
        };
        std::unique_ptr<int32_t> value = CreateResult().value_or(nullptr);
        ASSERT_EQ(233, *value);
    }
}

TEST(StatusTest, TestResultWithDerivedClass) {
    class B {
     public:
        B() = default;
        virtual ~B() = default;
        virtual int32_t GetValue() const {
            return value_b_;
        }

     private:
        int32_t value_b_ = -1;
    };
    class D : public B {
     public:
        explicit D(int32_t value) : B(), value_d_(value) {}
        int32_t GetValue() const override {
            return value_d_;
        }
        bool operator==(const D& other) const {
            if (this == &other) {
                return true;
            }
            return value_b_ == other.value_b_ && value_d_ == other.value_d_;
        }

     private:
        int32_t value_d_ = 1;
    };
    class A {
     public:
        int32_t GetValue() const {
            return value_a_;
        }

     private:
        int32_t value_a_ = 2;
    };

    {
        Result<std::shared_ptr<B>> r0 = std::make_shared<B>();
        r0 = Result<std::shared_ptr<D>>(Status::Invalid("invalid D"));
        ASSERT_TRUE(r0.status().ToString().find("invalid D") != std::string::npos);
    }
    {
        Result<std::shared_ptr<B>> r0 = Status::Invalid("invalid B");
        r0 = Result<std::shared_ptr<D>>(std::make_shared<D>(20));
        ASSERT_EQ(r0.value()->GetValue(), 20);
    }

    Result<std::unique_ptr<B>> b0 = std::make_unique<D>(232);
    ASSERT_EQ(232, b0.value()->GetValue());

    Result<std::unique_ptr<B>> b1 = std::make_unique<D>(233);
    ASSERT_EQ(233, b1.value()->GetValue());

    Result<std::shared_ptr<B>> b2 = std::make_shared<D>(234);
    ASSERT_EQ(234, b2.value()->GetValue());

    Result<std::unique_ptr<B>> b3 = std::make_unique<B>();
    ASSERT_EQ(-1, b3.value()->GetValue());

    Result<std::unique_ptr<D>> b4 = std::make_unique<D>(245);
    ASSERT_EQ(245, b4.value()->GetValue());

    b0 = std::move(b1);
    ASSERT_EQ(233, b0.value()->GetValue());

    b3 = std::move(b4);
    ASSERT_EQ(245, b3.value()->GetValue());

    Result<std::unique_ptr<B>> b5 = std::move(b3);
    ASSERT_EQ(245, b5.value()->GetValue());

    Result<std::unique_ptr<D>> b6 = std::make_unique<D>(246);
    Result<std::unique_ptr<B>> b7 = std::move(b6);
    ASSERT_EQ(246, b7.value()->GetValue());

    // failed, because A can not convert to B
    // Result<std::unique_ptr<B>> b5 = std::make_unique<A>();

    // support PAIMON_UNIQUE_PTR
    auto pool = GetDefaultPool();
    Result<PAIMON_UNIQUE_PTR<B>> b8 = pool->AllocateUnique<D>(249);
    ASSERT_EQ(249, b8.value()->GetValue());
    Result<PAIMON_UNIQUE_PTR<D>> b9 = pool->AllocateUnique<D>(250);
    ASSERT_EQ(250, b9.value()->GetValue());

    b8 = std::move(b9);
    ASSERT_EQ(250, b8.value()->GetValue());

    auto func = [&]() -> Result<PAIMON_UNIQUE_PTR<B>> { return pool->AllocateUnique<D>(251); };
    Result<PAIMON_UNIQUE_PTR<B>> b10 = func();
    ASSERT_EQ(251, b10.value()->GetValue());
}

TEST(StatusTest, TestResultConstruct) {
    {
        // status ok
        Result<std::shared_ptr<int32_t>> r0 = std::make_shared<int32_t>(10);
        Result<std::shared_ptr<int32_t>> r1(std::move(r0));
    }
    {
        // status invalid
        Result<std::shared_ptr<int32_t>> r0 = Status::Invalid("invalid");
        Result<std::shared_ptr<int32_t>> r1(std::move(r0));
    }
    {
        // status ok
        Result<std::shared_ptr<int32_t>> r0 = std::make_shared<int32_t>(10);
        Result<std::shared_ptr<int32_t>> r1(r0);
    }
    {
        // status invalid
        Result<std::shared_ptr<int32_t>> r0 = Status::Invalid("invalid");
        Result<std::shared_ptr<int32_t>> r1(r0);
    }
}

TEST(StatusTest, TestResultMoveAndAssign) {
    {
        Result<std::shared_ptr<int32_t>> r0 = std::make_shared<int32_t>(10);
        Result<std::shared_ptr<int32_t>> r1 = std::make_shared<int32_t>(20);
        r0 = r1;
        ASSERT_EQ(*r0.value(), 20);
    }
    {
        Result<std::shared_ptr<int32_t>> r0 = std::make_shared<int32_t>(10);
        r0 = Result<std::shared_ptr<int32_t>>(std::make_shared<int32_t>(20));
        ASSERT_EQ(*r0.value(), 20);
    }
    {
        Result<std::shared_ptr<int32_t>> r0 = Status::Invalid("invalid 10");
        Result<std::shared_ptr<int32_t>> r1 = Status::Invalid("invalid 20");
        r0 = r1;
        ASSERT_TRUE(r0.status().ToString().find("invalid 20") != std::string::npos);
    }
    {
        Result<std::shared_ptr<int32_t>> r0 = Status::Invalid("invalid 10");
        r0 = Result<std::shared_ptr<int32_t>>(Status::Invalid("invalid 20"));
        ASSERT_TRUE(r0.status().ToString().find("invalid 20") != std::string::npos);
    }
    {
        Result<std::shared_ptr<int32_t>> r0 = std::make_shared<int32_t>(10);
        Result<std::shared_ptr<int32_t>> r1 = Status::Invalid("invalid 20");
        r0 = r1;
        ASSERT_TRUE(r0.status().ToString().find("invalid 20") != std::string::npos);
    }
    {
        Result<std::shared_ptr<int32_t>> r0 = std::make_shared<int32_t>(10);
        r0 = Result<std::shared_ptr<int32_t>>(Status::Invalid("invalid 20"));
        ASSERT_TRUE(r0.status().ToString().find("invalid 20") != std::string::npos);
    }
    {
        Result<std::shared_ptr<int32_t>> r0 = Status::Invalid("invalid 10");
        Result<std::shared_ptr<int32_t>> r1 = std::make_shared<int32_t>(20);
        r0 = r1;
        ASSERT_EQ(*r0.value(), 20);
    }
    {
        Result<std::shared_ptr<int32_t>> r0 = Status::Invalid("invalid 10");
        r0 = Result<std::shared_ptr<int32_t>>(std::make_shared<int32_t>(20));
        ASSERT_EQ(*r0.value(), 20);
    }
}

}  // namespace paimon::test
