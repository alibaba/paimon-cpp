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

#include "paimon/common/utils/string_utils.h"

#include <limits>
#include <memory>

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class StringUtilsTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

 private:
    template <typename T>
    void CheckBoundary(const std::string& max_value_str, const std::string& min_value_str);
    template <typename T>
    void CheckOverFlowAndUnderFlow(const std::string& over_flow, const std::string& under_flow);
};

template <typename T>
void StringUtilsTest::CheckBoundary(const std::string& max_value_str,
                                    const std::string& min_value_str) {
    ASSERT_EQ(std::numeric_limits<T>::min(), StringUtils::StringToValue<T>(min_value_str).value());
    ASSERT_EQ(std::numeric_limits<T>::max(), StringUtils::StringToValue<T>(max_value_str).value());
}

template <>
void StringUtilsTest::CheckBoundary<double>(const std::string& max_value_str,
                                            const std::string& min_value_str) {
    ASSERT_NEAR(-std::numeric_limits<double>::max(),
                StringUtils::StringToValue<double>(min_value_str).value(), 0.00001e+308);
    ASSERT_NEAR(std::numeric_limits<double>::max(),
                StringUtils::StringToValue<double>(max_value_str).value(), 0.00001e+308);
}

template <>
void StringUtilsTest::CheckBoundary<float>(const std::string& max_value_str,
                                           const std::string& min_value_str) {
    ASSERT_NEAR(-std::numeric_limits<float>::max(),
                StringUtils::StringToValue<float>(min_value_str).value(), 0.00001e+38);
    ASSERT_NEAR(std::numeric_limits<float>::max(),
                StringUtils::StringToValue<float>(max_value_str).value(), 0.00001e+38);
}

template <typename T>
void StringUtilsTest::CheckOverFlowAndUnderFlow(const std::string& over_flow,
                                                const std::string& under_flow) {
    ASSERT_EQ(StringUtils::StringToValue<T>(over_flow), std::nullopt);
    ASSERT_EQ(StringUtils::StringToValue<T>(under_flow), std::nullopt);
}

TEST_F(StringUtilsTest, TestReplaceAll) {
    {
        std::string origin = "how is is you";
        std::string expect = "how are are you";
        std::string result = StringUtils::Replace(origin, "is", "are");
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "aabac";
        std::string expect = "aaaabaac";
        std::string result = StringUtils::Replace(origin, "a", "aa");
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "aaaabaac";
        std::string expect = "aabac";
        std::string result = StringUtils::Replace(origin, "aa", "a");
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "aaaabaac";
        std::string expect = "aaaabaac";
        std::string result = StringUtils::Replace(origin, "abc", "a");
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "aaaaaaaa";
        std::string expect = "bbbb";
        std::string result = StringUtils::Replace(origin, "aa", "b");
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "aaaaaaaaa";
        std::string expect = "bbbba";
        std::string result = StringUtils::Replace(origin, "aa", "b");
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "/home/admin/ops";
        std::string expect = R"(\/home\/admin\/ops)";
        std::string result = StringUtils::Replace(origin, "/", "\\/");
        ASSERT_EQ(expect, result);
    }
}

TEST_F(StringUtilsTest, TestReplaceWithMaxCount) {
    {
        std::string origin = "how is is you";
        std::string expect = "how are is you";
        std::string result = StringUtils::Replace(origin, "is", "are", 1);
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "aabac";
        std::string expect = "aaaabac";
        std::string result = StringUtils::Replace(origin, "a", "aa", 2);
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "aaaabaac";
        std::string expect = "aaaabaac";
        std::string result = StringUtils::Replace(origin, "aa", "a", 0);
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "aaaaaaaa";
        std::string expect = "bbbb";
        std::string result = StringUtils::Replace(origin, "aa", "b", 100);
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "aaaaaaaaa";
        std::string expect = "bbbaaa";
        std::string result = StringUtils::Replace(origin, "aa", "b", 3);
        ASSERT_EQ(expect, result);
    }
    {
        std::string origin = "/home/admin/ops";
        std::string expect = "\\/home\\/admin/ops";
        std::string result = StringUtils::Replace(origin, "/", "\\/", 2);
        ASSERT_EQ(expect, result);
    }
}

TEST_F(StringUtilsTest, TestIsNullOrWhitespaceOnly) {
    {
        std::string str = "";
        auto ret = StringUtils::IsNullOrWhitespaceOnly(str);
        ASSERT_TRUE(ret);
    }
    {
        std::string str = "a a a a";
        auto ret = StringUtils::IsNullOrWhitespaceOnly(str);
        ASSERT_FALSE(ret);
    }
    {
        std::string str = "     ";
        auto ret = StringUtils::IsNullOrWhitespaceOnly(str);
        ASSERT_TRUE(ret);
    }
    {
        std::string str = "\n";
        auto ret = StringUtils::IsNullOrWhitespaceOnly(str);
        ASSERT_TRUE(ret);
    }
    {
        std::string str = "\t";
        auto ret = StringUtils::IsNullOrWhitespaceOnly(str);
        ASSERT_TRUE(ret);
    }
}

TEST_F(StringUtilsTest, TestToLowerCase) {
    {
        std::string str = "HDGF";
        ASSERT_EQ("hdgf", StringUtils::ToLowerCase(str));
    }
    {
        std::string str = "ab CD ffg +8";
        ASSERT_EQ("ab cd ffg +8", StringUtils::ToLowerCase(str));
    }
    {
        std::string str = "";
        ASSERT_EQ("", StringUtils::ToLowerCase(str));
    }
}

TEST_F(StringUtilsTest, TestToUpperCase) {
    {
        std::string str = "hdgf";
        ASSERT_EQ("HDGF", StringUtils::ToUpperCase(str));
    }
    {
        std::string str = "AB cd ffg +8";
        ASSERT_EQ("AB CD FFG +8", StringUtils::ToUpperCase(str));
    }
    {
        std::string str = "";
        ASSERT_EQ("", StringUtils::ToUpperCase(str));
    }
}

TEST_F(StringUtilsTest, TestStartsWith) {
    {
        std::string str = "abcde";
        ASSERT_TRUE(StringUtils::StartsWith(str, "ab"));
    }
    {
        std::string str = "abcde";
        ASSERT_FALSE(StringUtils::StartsWith(str, "ba"));
    }
    {
        std::string str = "abcde";
        ASSERT_TRUE(StringUtils::StartsWith(str, "bc", /*start_pos=*/1));
    }
    {
        std::string str = "abcde";
        ASSERT_FALSE(StringUtils::StartsWith(str, "bc", /*start_pos=*/3));
    }
    {
        std::string str = "";
        ASSERT_FALSE(StringUtils::StartsWith(str, "bc"));
    }
    {
        std::string str = "";
        ASSERT_TRUE(StringUtils::StartsWith(str, ""));
    }
}
TEST_F(StringUtilsTest, TestEndsWith) {
    {
        std::string str = "abcde";
        ASSERT_TRUE(StringUtils::EndsWith(str, "de"));
    }
    {
        std::string str = "abcde";
        ASSERT_FALSE(StringUtils::EndsWith(str, "ba"));
    }
    {
        std::string str = "";
        ASSERT_FALSE(StringUtils::EndsWith(str, "bc"));
    }
    {
        std::string str = "";
        ASSERT_TRUE(StringUtils::EndsWith(str, ""));
    }
}

TEST_F(StringUtilsTest, TestSplit) {
    {
        std::vector<std::string> expect = {"aabbcc"};
        std::vector<std::string> result = StringUtils::Split("aabbcc", "");
        ASSERT_EQ(expect, result);
    }
    {
        std::vector<std::string> expect = {"aa", "bb", "cc"};
        std::vector<std::string> result = StringUtils::Split("aa,bb,cc", ",");
        ASSERT_EQ(expect, result);
    }
    {
        std::vector<std::string> expect = {"aa", "bb", "cc"};
        std::vector<std::string> result =
            StringUtils::Split("aa,bb,,cc", ",", /*ignore_empty=*/true);
        ASSERT_EQ(expect, result);
    }
    {
        std::vector<std::string> expect = {"aa", "bb", "", "cc"};
        std::vector<std::string> result =
            StringUtils::Split("aa,bb,,cc", ",", /*ignore_empty=*/false);
        ASSERT_EQ(expect, result);
    }
    {
        std::vector<std::vector<std::string>> expect = {
            {"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}};
        std::vector<std::vector<std::string>> result = StringUtils::Split(
            "key1=value1/key2=value2/key3=value3", std::string("/"), std::string("="));
        ASSERT_EQ(expect, result);
    }
    {
        std::vector<std::vector<std::string>> expect = {{"key1"}, {"key2"}, {"key3", "value3"}};
        std::vector<std::vector<std::string>> result =
            StringUtils::Split("key1/key2=/key3=value3", std::string("/"), std::string("="));
        ASSERT_EQ(expect, result);
    }
    {
        std::vector<std::vector<std::string>> expect = {
            {"key1"}, {"key2", "   "}, {"key3", "value3"}};
        std::vector<std::vector<std::string>> result =
            StringUtils::Split("key1/key2=   /key3=value3", std::string("/"), std::string("="));
        ASSERT_EQ(expect, result);
    }
    {
        std::vector<std::vector<std::string>> expect = {{"key1", "value1"}, {"key3", "value3"}};
        std::vector<std::vector<std::string>> result =
            StringUtils::Split("key1=value1//key3=value3", std::string("/"), std::string("="));
        ASSERT_EQ(expect, result);
    }
}

TEST_F(StringUtilsTest, TestStringToValueSimple) {
    ASSERT_EQ(static_cast<int32_t>(233), StringUtils::StringToValue<int32_t>("233").value());
    ASSERT_EQ(static_cast<int8_t>(10), StringUtils::StringToValue<int8_t>("10").value());
    ASSERT_EQ(std::nullopt, StringUtils::StringToValue<int8_t>("1024"));
    ASSERT_EQ(static_cast<int64_t>(34785895352),
              StringUtils::StringToValue<int64_t>("34785895352").value());
    ASSERT_EQ(std::nullopt, StringUtils::StringToValue<int32_t>("abc"));
    ASSERT_EQ(std::nullopt, StringUtils::StringToValue<int32_t>(""));

    ASSERT_EQ(true, StringUtils::StringToValue<bool>("1").value());
    ASSERT_EQ(true, StringUtils::StringToValue<bool>("true").value());
    ASSERT_EQ(true, StringUtils::StringToValue<bool>("TRUE").value());
    ASSERT_EQ(false, StringUtils::StringToValue<bool>("0").value());
    ASSERT_EQ(false, StringUtils::StringToValue<bool>("false").value());
    ASSERT_EQ(false, StringUtils::StringToValue<bool>("FALSE").value());
    ASSERT_EQ(std::nullopt, StringUtils::StringToValue<bool>("123"));
}

TEST_F(StringUtilsTest, TestStringToValueWithBoundaryValue) {
    {
        // normal case
        CheckBoundary<int8_t>("127", "-128");
        CheckBoundary<int16_t>("32767", "-32768");
        CheckBoundary<int32_t>("2147483647", "-2147483648");
        CheckBoundary<uint32_t>("4294967295", "0");
        CheckBoundary<int64_t>("9223372036854775807", "-9223372036854775808");
        CheckBoundary<uint64_t>("18446744073709551615", "0");
        CheckBoundary<float>("3.4028235e+38", "-3.4028235e+38");
        CheckBoundary<double>("1.7976931348623157e+308", "-1.7976931348623157e+308");
    }
    {
        // overflow or underflow
        CheckOverFlowAndUnderFlow<int8_t>("128", "-129");
        CheckOverFlowAndUnderFlow<int16_t>("32768", "-32769");
        CheckOverFlowAndUnderFlow<int32_t>("2147483648", "-2147483649");
        CheckOverFlowAndUnderFlow<uint32_t>("4294967296", "-1");
        CheckOverFlowAndUnderFlow<int64_t>("9223372036854775808", "-9223372036854775809");
        CheckOverFlowAndUnderFlow<uint64_t>("18446744073709551616", "-1");

        CheckOverFlowAndUnderFlow<float>("3.4028235e+39", "-3.4028235e+39");
        CheckOverFlowAndUnderFlow<double>("1.7976931348623157e+309", "-1.7976931348623157e+309");
    }
}

TEST_F(StringUtilsTest, TestStringToDate) {
    {
        ASSERT_OK_AND_ASSIGN(auto date, StringUtils::StringToDate("2147483647"));
        ASSERT_EQ(date, 2147483647);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto date, StringUtils::StringToDate("-2147483648"));
        ASSERT_EQ(date, -2147483648);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto date, StringUtils::StringToDate("1970-01-01"));
        ASSERT_EQ(date, 0);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto date, StringUtils::StringToDate("0000-01-01"));
        ASSERT_EQ(date, -719528);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto date, StringUtils::StringToDate("9999-12-31"));
        ASSERT_EQ(date, 2932896);
    }
    // invalid str
    ASSERT_NOK(StringUtils::StringToDate("9223372036854775807"));
    ASSERT_NOK(StringUtils::StringToDate("11970-01-02"));
    ASSERT_NOK(StringUtils::StringToDate("-1970-01-02"));
    ASSERT_NOK(StringUtils::StringToDate(""));
    ASSERT_NOK(StringUtils::StringToDate("1970-XX-02"));
}

TEST_F(StringUtilsTest, TestVectorToString) {
    class A {
     public:
        explicit A(int32_t value) : value_(value) {}
        std::string ToString() const {
            return std::to_string(value_);
        }

     private:
        int32_t value_;
    };

    {
        std::vector<A> vec = {A(10), A(20), A(30)};
        ASSERT_EQ(StringUtils::VectorToString(vec), "[10, 20, 30]");
    }
    {
        std::vector<std::optional<A>> vec = {A(10), A(20), A(30), std::nullopt};
        ASSERT_EQ(StringUtils::VectorToString(vec), "[10, 20, 30, null]");
    }
    {
        std::vector<std::shared_ptr<A>> vec = {std::make_shared<A>(10), std::make_shared<A>(20),
                                               std::make_shared<A>(30)};
        ASSERT_EQ(StringUtils::VectorToString(vec), "[10, 20, 30]");
    }
}
}  // namespace paimon::test
